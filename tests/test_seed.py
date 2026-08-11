import json
import subprocess
import threading

from typer.testing import CliRunner

from dt import cli, sshio
from dt.config import HeadConfig, LaptopConfig, Node


def _cfg(tmp_path, nodes=None):
    return HeadConfig(
        center="test",
        nodes=nodes or [Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )


def _local_seed_sources(tmp_path):
    uv_cache = tmp_path / ".cache" / "uv"
    uv_python = tmp_path / ".local" / "share" / "uv" / "python"
    uv_cache.mkdir(parents=True)
    uv_python.mkdir(parents=True)
    (uv_cache / "wheel").write_text("wheel\n")
    (uv_python / "cpython").write_text("python\n")


def test_seed_cache_lock_serializes_same_node(tmp_path):
    from dt.dispatch import _seed_cache_lock

    cfg = _cfg(tmp_path)
    node = cfg.nodes[0]
    first_entered = threading.Event()
    second_attempted = threading.Event()
    second_entered = threading.Event()
    release_first = threading.Event()

    def first():
        with _seed_cache_lock(cfg, node):
            first_entered.set()
            assert release_first.wait(1)

    def second():
        assert first_entered.wait(1)
        second_attempted.set()
        with _seed_cache_lock(cfg, node):
            second_entered.set()

    first_thread = threading.Thread(target=first)
    second_thread = threading.Thread(target=second)
    first_thread.start()
    second_thread.start()
    assert second_attempted.wait(1)
    assert not second_entered.is_set()
    release_first.set()
    first_thread.join(1)
    second_thread.join(1)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert second_entered.is_set()


def test_seed_json_runs_nodes_concurrently_and_reports_exact_bytes(
    tmp_path, monkeypatch
):
    nodes = [Node(name="n1"), Node(name="n2")]
    cfg = _cfg(tmp_path, nodes)
    _local_seed_sources(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    both_started = threading.Event()
    started = set()
    lock = threading.Lock()
    rsync_calls = []

    def fake_run_on(name, local, command, timeout):
        with lock:
            started.add(name)
            if len(started) == 2:
                both_started.set()
        if not both_started.wait(1):
            raise AssertionError("seed nodes were processed sequentially")
        return subprocess.CompletedProcess([], 0, "", "")

    def fake_rsync(src, dst, **kwargs):
        rsync_calls.append((src, dst, kwargs))
        size = 10 if src.endswith("/.cache/uv/") else 20
        return subprocess.CompletedProcess(
            [],
            0,
            f"Total transferred file size: {size} bytes\n",
            "",
        )

    monkeypatch.setattr(cli, "run_on", fake_run_on)
    monkeypatch.setattr(cli, "rsync", fake_rsync)

    result = CliRunner().invoke(cli.app, ["seed", "n2", "n1", "--json"])

    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    assert [row["node"] for row in rows] == ["n2", "n1"]
    assert [row["status"] for row in rows] == ["seeded", "seeded"]
    assert [row["transferred_bytes"] for row in rows] == [30, 30]
    assert all(
        row["source_bytes"]
        == sum(component["source_bytes"] for component in row["components"])
        for row in rows
    )
    assert all(
        [component["transferred_bytes"] for component in row["components"]] == [10, 20]
        for row in rows
    )
    assert all(
        all(component["source_bytes"] > 0 for component in row["components"])
        for row in rows
    )
    assert len(rsync_calls) == 4
    assert all(call[2]["stats"] is True for call in rsync_calls)
    assert all(call[2]["retries"] == 1 for call in rsync_calls)
    assert all(callable(call[2]["on_retry"]) for call in rsync_calls)


def test_seed_rejects_negative_retries_before_config(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid retries must fail before config access")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["seed", "n1", "--retries", "-1", "--json"],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "error": "invalid_argument",
        "message": "seed --retries must be non-negative",
        "reasons": {},
        "exit_code": 1,
    }


def test_seed_reports_component_retry_without_polluting_json(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _local_seed_sources(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    calls = 0

    def transient_rsync(src, dst, **kwargs):
        nonlocal calls
        calls += 1
        assert kwargs["retries"] == 1
        if calls == 1:
            kwargs["on_retry"](
                sshio.RsyncRetryEvent(
                    failed_attempt=1,
                    next_attempt=2,
                    max_attempts=2,
                    delay_s=5,
                    returncode=255,
                    message="ssh: transient seed link",
                )
            )
        size = 10 if src.endswith("/.cache/uv/") else 20
        return subprocess.CompletedProcess(
            [],
            0,
            f"Total transferred file size: {size} bytes\n",
            "",
        )

    monkeypatch.setattr(cli, "rsync", transient_rsync)

    result = CliRunner().invoke(
        cli.app,
        ["seed", "n1", "--retries", "1", "--json"],
    )

    assert result.exit_code == 0, result.output
    row = json.loads(result.stdout)[0]
    assert row["retry_events"] == [
        {
            "phase": "uv-cache",
            "failed_attempt": 1,
            "next_attempt": 2,
            "max_attempts": 2,
            "delay_s": 5,
            "returncode": 255,
            "message": "ssh: transient seed link",
            "kind": "transport",
        }
    ]
    assert result.stdout.count("\n") == 1
    assert "n1 · uv-cache attempt 1/2 failed" in result.stderr
    assert "retry 2/2 in 5s" in result.stderr


def test_seed_plan_json_is_read_only_and_reports_local_source_size(
    tmp_path, monkeypatch
):
    nodes = [Node(name="n1"), Node(name="n2")]
    cfg = _cfg(tmp_path, nodes)
    _local_seed_sources(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("seed --plan must not access a remote node")
        ),
    )
    monkeypatch.setattr(
        cli,
        "rsync",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("seed --plan must not transfer data")
        ),
    )

    result = CliRunner().invoke(cli.app, ["seed", "n2", "n1", "--plan", "--json"])

    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    assert [row["node"] for row in rows] == ["n2", "n1"]
    assert [row["status"] for row in rows] == ["planned", "planned"]
    assert all(row["source_bytes"] > 0 for row in rows)
    assert all(
        row["source_bytes"]
        == sum(component["source_bytes"] for component in row["components"])
        for row in rows
    )
    assert all(
        [component["status"] for component in row["components"]]
        == ["planned", "planned"]
        for row in rows
    )
    assert all("transferred_bytes" not in row for row in rows)


def test_seed_json_classifies_prepare_link_failure_without_rsync(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _local_seed_sources(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: No route to host"
        ),
    )
    monkeypatch.setattr(
        cli,
        "rsync",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("prepare failure must stop data transfer")
        ),
    )

    result = CliRunner().invoke(cli.app, ["seed", "n1", "--json"])

    assert result.exit_code == cli.EXIT_UNREACHABLE
    row = json.loads(result.stdout)[0]
    assert row == {
        "node": "n1",
        "status": "error",
        "hf": False,
        "source_bytes": row["source_bytes"],
        "transferred_bytes": 0,
        "components": [],
        "error_kind": "unreachable",
        "message": "cache preparation failed: ssh: No route to host",
        "exit_code": cli.EXIT_UNREACHABLE,
    }
    assert row["source_bytes"] > 0


def test_seed_stops_after_component_link_failure_without_false_partial(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    _local_seed_sources(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    calls = []

    def fail_link(*args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess([], 255, "", "ssh: connection lost")

    monkeypatch.setattr(cli, "rsync", fail_link)

    result = CliRunner().invoke(cli.app, ["seed", "n1", "--json"])

    assert result.exit_code == cli.EXIT_UNREACHABLE
    row = json.loads(result.stdout)[0]
    assert row["error_kind"] == "unreachable"
    assert row["transferred_bytes"] == 0
    assert "partial" not in row
    assert [component["name"] for component in row["components"]] == ["uv-cache"]
    assert row["components"][0]["source_bytes"] > 0
    assert len(calls) == 1


def test_seed_reports_partial_when_later_component_has_data_error(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    _local_seed_sources(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    calls = 0

    def fail_second(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return subprocess.CompletedProcess(
                [],
                0,
                "Total transferred file size: 10 bytes\n",
                "",
            )
        return subprocess.CompletedProcess([], 23, "", "permission denied")

    monkeypatch.setattr(cli, "rsync", fail_second)

    result = CliRunner().invoke(cli.app, ["seed", "n1", "--json"])

    assert result.exit_code == 1
    row = json.loads(result.stdout)[0]
    assert row["error_kind"] == "seed_failed"
    assert row["partial"] is True
    assert row["transferred_bytes"] == 10
    assert [component["status"] for component in row["components"]] == [
        "seeded",
        "error",
    ]
    assert all(component["source_bytes"] > 0 for component in row["components"])


def test_seed_human_output_reports_idempotent_noop(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _local_seed_sources(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    monkeypatch.setattr(
        cli,
        "rsync",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            "Total transferred file size: 0 bytes\n",
            "",
        ),
    )

    result = CliRunner().invoke(cli.app, ["seed", "n1"])

    assert result.exit_code == 0, result.output
    assert "local source" in result.output
    assert "missing/changed" in result.output
    assert "n1 seeded" in result.output
    assert "no changed bytes" in result.output


def test_seed_plan_human_output_is_explicitly_read_only(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _local_seed_sources(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("seed --plan must not access a remote node")
        ),
    )
    monkeypatch.setattr(
        cli,
        "rsync",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("seed --plan must not transfer data")
        ),
    )

    result = CliRunner().invoke(cli.app, ["seed", "n1", "--plan"])

    assert result.exit_code == 0, result.output
    assert "n1 would seed" in result.output
    assert "local source" in result.output
    assert "preview only" in result.output
    assert "no remote access or writes" in result.output


def test_seed_json_rejects_unknown_nodes_before_remote_access(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "run_on",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unknown node must fail before remote access")
        ),
    )

    result = CliRunner().invoke(cli.app, ["seed", "missing", "--json"])

    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "error": "unknown_node",
        "message": "unknown node(s) ['missing']; configured: ['n1']",
        "exit_code": 1,
    }


def test_seed_json_hf_and_plan_are_forwarded_from_laptop(monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    seen = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "_laptop_center", lambda cfg_, center: "test")
    monkeypatch.setattr(
        cli,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "{}", ""),
    )

    def capture(head, argv, tty=False, **kwargs):
        seen.append((head, argv, tty, kwargs))
        return 0, "[]\n"

    monkeypatch.setattr(cli, "forward_capture_stdout", capture)
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("laptop seed must retain control to reconnect")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "seed",
            "n2",
            "n1",
            "--hf",
            "--plan",
            "--retries",
            "0",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == []
    assert seen == [
        (
            "head",
            [
                "seed",
                "n2",
                "n1",
                "--hf",
                "--plan",
                "--retries",
                "0",
                "--json",
            ],
            False,
            {"emit_stdout": False},
        )
    ]


def test_laptop_seed_reconnects_without_leaking_partial_json(monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    payload = [
        {
            "node": "n1",
            "status": "seeded",
            "transferred_bytes": 0,
        }
    ]
    captures = iter(
        [
            (255, '[{"node":"n1","status":"see'),
            (0, json.dumps(payload) + "\n"),
        ]
    )
    probes = iter(
        [
            subprocess.CompletedProcess([], 255, "", ""),
            subprocess.CompletedProcess([], 0, "{}", ""),
        ]
    )
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def remote(*args, **kwargs):
        if not calls:
            return subprocess.CompletedProcess([], 0, "{}", "")
        return next(probes)

    def capture(head, argv, tty=False, **kwargs):
        calls.append((head, argv, tty, kwargs))
        return next(captures)

    monkeypatch.setattr(cli, "remote_dt", remote)
    monkeypatch.setattr(cli, "forward_capture_stdout", capture)
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("laptop seed must not use one-shot forwarding")
        ),
    )
    sleeps = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    result = CliRunner().invoke(
        cli.app,
        ["seed", "n1", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == payload
    assert len(calls) == 2
    assert sleeps == [2.0, 4.0]
    assert '[{"node":"n1","status":"see' not in result.stdout
    normalized = " ".join(result.output.split())
    assert normalized.count("seed link to head unavailable") == 1
    assert normalized.count("head reachable again; seed resumed") == 1


def test_laptop_seed_initially_unreachable_fails_before_mutation(monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: No route to host"
        ),
    )
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unreachable preflight must not start seed")
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["seed", "n1", "--json"],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    payload = json.loads(result.stdout)
    assert payload["error"] == "unreachable"
    assert payload["exit_code"] == cli.EXIT_UNREACHABLE
    assert "head unavailable before seed" in payload["message"]
    assert "No route to host" in payload["message"]


def test_laptop_seed_ctrl_c_keeps_cache_and_prints_exact_resume(monkeypatch):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "{}", ""),
    )
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "seed",
            "n2",
            "n1",
            "--hf",
            "--plan",
            "--retries",
            "0",
        ],
    )

    assert result.exit_code == 130, result.output
    normalized = " ".join(result.output.split())
    assert "seed stopped locally" in normalized
    assert "remote caches and partial data were not deleted" in normalized
    assert "dt seed n2 n1 --hf --plan --retries 0" in normalized


def test_laptop_seed_ctrl_c_json_is_one_complete_resume_payload(
    monkeypatch,
):
    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "{}", ""),
    )
    monkeypatch.setattr(
        cli,
        "forward_capture_stdout",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    result = CliRunner().invoke(
        cli.app,
        ["seed", "n1", "--hf", "-c", "test", "--json"],
    )

    assert result.exit_code == 130, result.output
    payload = json.loads(result.stdout)
    assert payload["error"] == "seed_interrupted"
    assert payload["exit_code"] == 130
    assert "remote caches and partial data were not deleted" in payload["message"]
    assert "dt seed n1 --hf -c test --json" in payload["message"]
    assert result.stdout.count("\n") == 1

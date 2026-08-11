import json
import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path

import dt.probe as probe_mod
from dt.config import HeadConfig, Node
from dt.probe import (
    GPU_ERROR,
    Gpu,
    NodeStatus,
    PROBE_CMD,
    SEP,
    SYS_SEP,
    parse_probe_error,
    parse_probe_output,
    parse_system_output,
)
from dt.sshio import RemoteError

SAMPLE = f"""0, GPU-aaa, 3, 81920, 0
1, GPU-bbb, 76000, 81920, 98
2, GPU-ccc, 800, 81920, 0
{SEP}
GPU-bbb, 12345
GPU-bbb, 12346
"""


def test_free_iff_no_procs_and_low_mem():
    gpus = parse_probe_output(SAMPLE, mem_threshold_mib=500)
    by_idx = {g.index: g for g in gpus}
    assert by_idx[0].free  # idle
    assert not by_idx[1].free  # busy: procs + memory
    assert by_idx[1].procs == 2
    assert not by_idx[2].free  # zombie ctx: mem>threshold, no procs


def test_unparsable_compute_app_row_counts_as_occupancy():
    """Evidence we cannot read must never make a card look free.

    APP_Q normalises every row to `uuid,pid,user`, so reaching this needs a
    truncated line. The direction matters more than the probability: `free` is
    what dt hands out, and dropping the row would turn an occupied GPU into an
    idle-looking one.
    """
    truncated = f"0, GPU-aaa, 3, 81920, 0\n{SEP}\nGPU-aaa\n{SYS_SEP}\n"
    gpu = parse_probe_output(truncated, mem_threshold_mib=500)[0]

    assert gpu.procs == 1
    assert not gpu.free
    assert gpu.users == ["?"]


def test_truncated_row_for_an_unknown_card_invents_no_occupancy():
    """Conservative must not mean credulous: only known uuids count."""
    truncated = f"0, GPU-aaa, 3, 81920, 0\n{SEP}\nGPU-zzz\n{SYS_SEP}\n"
    gpu = parse_probe_output(truncated, mem_threshold_mib=500)[0]

    assert gpu.procs == 0
    assert gpu.free


def test_threshold_boundary():
    gpus = parse_probe_output(SAMPLE, mem_threshold_mib=1000)
    by_idx = {g.index: g for g in gpus}
    assert by_idx[2].free  # 800 MiB < 1000 threshold


def test_empty_apps_section():
    text = f"0, GPU-x, 0, 81920, 0\n{SEP}\n"
    gpus = parse_probe_output(text, 500)
    assert len(gpus) == 1 and gpus[0].free


def test_duplicate_compute_app_identity_counts_once():
    text = f"0, GPU-x, 4096, 81920, 50\n{SEP}\nGPU-x, 123, alice\nGPU-x, 123, alice\n"

    gpu = parse_probe_output(text, 500)[0]

    assert gpu.procs == 1
    assert gpu.users == ["alice"]
    assert not gpu.free


def test_probe_batches_owner_lookup_and_deduplicates_apps(tmp_path):
    fake_commands = r"""
    nvidia-smi() {
        case "$*" in
          *--query-gpu=*) echo "0, GPU-test, 4096, 81920, 50, 42" ;;
          *--query-compute-apps=*)
            i=0
            while [ "$i" -lt 60 ]; do
              echo "GPU-test, $((1000 + i % 30))"
              i=$((i + 1))
            done
            ;;
          *) return 1 ;;
        esac
    }
    ps() {
        printf "call\n" >> "$DT_TEST_PS_CALLS"
        for arg do pids=$arg; done
        old_ifs=$IFS
        IFS=,
        for pid in $pids; do printf '%s batchuser\n' "$pid"; done
        IFS=$old_ifs
    }
    """
    calls = tmp_path / "ps-calls"

    proc = subprocess.run(
        ["bash", "-c", f"{fake_commands}\n{PROBE_CMD}"],
        env={
            **os.environ,
            "DT_TEST_PS_CALLS": str(calls),
        },
        capture_output=True,
        text=True,
        check=True,
    )
    gpu = parse_probe_output(proc.stdout, 500)[0]

    assert calls.read_text().splitlines() == ["call"]
    assert gpu.procs == 30
    assert gpu.users == ["batchuser"]


def test_probe_overlaps_independent_nvidia_smi_queries(tmp_path):
    fake_bin = tmp_path / "bin"
    barrier = tmp_path / "barrier"
    fake_bin.mkdir()
    barrier.mkdir()
    nvidia_smi = fake_bin / "nvidia-smi"
    nvidia_smi.write_text(
        r"""#!/bin/sh
case "$*" in
  *--query-gpu=*)
    own="$DT_TEST_BARRIER/gpu"
    peer="$DT_TEST_BARRIER/apps"
    output="0, GPU-test, 4096, 81920, 50, 42"
    ;;
  *--query-compute-apps=*)
    own="$DT_TEST_BARRIER/apps"
    peer="$DT_TEST_BARRIER/gpu"
    output="GPU-test, 123"
    ;;
  *) exit 9 ;;
esac
: > "$own"
attempt=0
while [ ! -e "$peer" ] && [ "$attempt" -lt 100 ]; do
  sleep 0.01
  attempt=$((attempt + 1))
done
[ -e "$peer" ] || exit 81
printf '%s\n' "$output"
""",
        encoding="utf-8",
    )
    nvidia_smi.chmod(0o755)

    proc = subprocess.run(
        ["bash", "-c", PROBE_CMD],
        env={
            **os.environ,
            "DT_TEST_BARRIER": str(barrier),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "TMPDIR": str(tmp_path),
        },
        capture_output=True,
        text=True,
        check=True,
        timeout=5,
    )

    assert parse_probe_error(proc.stdout) is None, proc.stdout
    gpu = parse_probe_output(proc.stdout, 500)[0]
    assert gpu.uuid == "GPU-test"
    assert gpu.procs == 1
    assert not gpu.free


def test_dt_lease_marks_pre_cuda_job_busy():
    text = f"0, GPU-x, 0, 81920, 0, 1\n1, GPU-y, 0, 81920, 0, 0\n{SEP}\n"
    gpus = parse_probe_output(text, 500)

    assert not gpus[0].free
    assert gpus[0].leased
    assert gpus[0].users == ["dt-lease"]
    assert gpus[1].free
    assert not gpus[1].leased


def test_dt_lease_exposes_exact_job_owner_from_lock_file():
    text = f"0, GPU-x, 0, 81920, 0, 42, 1, 20260724-1220_train-policy_abcd\n{SEP}\n"

    gpu = parse_probe_output(text, 500)[0]

    assert gpu.leased
    assert gpu.lease_owner == "20260724-1220_train-policy_abcd"
    assert gpu.users == ["dt-lease"]


def test_concurrent_probe_readers_do_not_create_false_gpu_leases(tmp_path):
    import fcntl

    fake_nvidia_smi = r"""
    nvidia-smi() {
        case "$*" in
          *--query-gpu=*) echo "0, GPU-test, 0, 24576, 0, 30" ;;
          *--query-compute-apps=*) return 0 ;;
          *) return 1 ;;
        esac
    }
    """
    lease = tmp_path / "dt/gpu-leases/gpu-0.lock"
    lease.parent.mkdir(parents=True)
    lease.write_text("stale-finished-owner\n")
    env = {
        **os.environ,
        "HOME": str(tmp_path),
    }

    def live_probe():
        proc = subprocess.run(
            ["bash", "-c", f"{fake_nvidia_smi}\n{PROBE_CMD}"],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
        return parse_probe_output(proc.stdout, mem_threshold_mib=500)[0]

    with lease.open("r+") as observer:
        fcntl.flock(observer, fcntl.LOCK_SH | fcntl.LOCK_NB)
        concurrent_reader = live_probe()
        fcntl.flock(observer, fcntl.LOCK_UN)

        fcntl.flock(observer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        real_wrapper = live_probe()
        fcntl.flock(observer, fcntl.LOCK_UN)

    assert concurrent_reader.free
    assert not concurrent_reader.leased
    assert not real_wrapper.free
    assert real_wrapper.leased
    assert real_wrapper.lease_owner == "stale-finished-owner"


def test_parse_live_gpu_temperature_with_lease():
    text = f"0, GPU-x, 20480, 24576, 96, 69, 1\n1, GPU-y, 0, 24576, 0, N/A, 0\n{SEP}\n"

    gpus = parse_probe_output(text, 500)

    assert gpus[0].temperature == 69
    assert gpus[0].leased
    assert gpus[1].temperature is None
    assert not gpus[1].leased


def test_parse_system_resources():
    text = (
        f"0, GPU-x, 0, 24576, 0, 0\n{SEP}\n"
        f"{SYS_SEP}\n"
        "16,1.25,65536000,49152000,1048576000,524288000,0.42\n"
    )
    system = parse_system_output(text)

    assert system is not None
    assert system.cpu_cores == 16
    assert system.cpu_load1 == 1.25
    assert system.mem_used_mib == 16000
    assert system.mem_total_mib == 64000
    assert system.disk_free_gib == 500.0
    assert system.disk_total_gib == 1000.0
    assert system.io_pressure == 0.42


def test_system_resources_are_optional_for_old_probe_output():
    assert parse_system_output(SAMPLE) is None


def test_probe_command_preserves_nvidia_smi_failure():
    command = (
        "nvidia-smi() { echo 'driver unavailable sentinel' >&2; return 9; };\n"
        f"{PROBE_CMD}"
    )

    proc = subprocess.run(
        ["bash", "-c", command],
        env=os.environ,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert "---DT-GPU-ERROR---" in proc.stdout
    assert "driver unavailable sentinel" in proc.stdout


def test_probe_command_preserves_compute_app_failure_and_fails_closed(monkeypatch):
    command = (
        r"""
    nvidia-smi() {
        case "$*" in
          *--query-gpu=*) echo "0, GPU-test, 0, 24576, 0, 30" ;;
          *--query-compute-apps=*)
            echo "compute process sentinel" >&2
            return 9
            ;;
          *) return 1 ;;
        esac
    }
    """
        + PROBE_CMD
    )

    proc = subprocess.run(
        ["bash", "-c", command],
        env=os.environ,
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    monkeypatch.setattr(
        probe_mod,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 0, proc.stdout, proc.stderr
        ),
    )

    assert parse_probe_error(proc.stdout) == (
        "GPU process query failed: compute process sentinel"
    )
    status = probe_mod.probe_node(Node(name="n1"), mem_threshold_mib=500)
    assert status.gpus == []
    assert status.error == "GPU process query failed: compute process sentinel"


def test_bounded_probe_cleans_up_workers_and_temporary_directory(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    pids = tmp_path / "nvidia-pids"
    nvidia_smi = fake_bin / "nvidia-smi"
    nvidia_smi.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$$" >> "$DT_TEST_PIDS"\n'
        "trap 'exit 0' TERM INT HUP\n"
        "while :; do sleep 1; done\n",
        encoding="utf-8",
    )
    nvidia_smi.chmod(0o755)

    proc = subprocess.run(
        ["bash", "-c", probe_mod.bounded_probe_command(0.2)],
        env={
            **os.environ,
            "DT_TEST_PIDS": str(pids),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "TMPDIR": str(tmp_path),
        },
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert proc.returncode == probe_mod.PROBE_TIMEOUT_EXIT
    assert not list(tmp_path.glob("dt-probe.*"))
    worker_pids = [int(pid) for pid in pids.read_text().splitlines()]
    deadline = time.monotonic() + 1
    while any(Path(f"/proc/{pid}").exists() for pid in worker_pids):
        assert time.monotonic() < deadline, "timed-out nvidia-smi worker leaked"
        time.sleep(0.01)


def test_bounded_probe_parent_retries_cleanup_after_child_cleanup_race(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    cleanup_calls = tmp_path / "rmdir-calls"
    nvidia_smi = fake_bin / "nvidia-smi"
    nvidia_smi.write_text(
        "#!/bin/sh\n"
        'case "$*" in\n'
        "  *query-compute-apps*) exit 0;;\n"
        "  *) printf '%s\\n' '0, GPU-test, 0, 1000, 0, 30';;\n"
        "esac\n",
        encoding="utf-8",
    )
    nvidia_smi.chmod(0o755)
    rmdir = fake_bin / "rmdir"
    rmdir.write_text(
        "#!/bin/sh\n"
        'count=$(cat "$DT_TEST_RMDIR_CALLS" 2>/dev/null || printf 0)\n'
        "count=$((count + 1))\n"
        'printf \'%s\\n\' "$count" > "$DT_TEST_RMDIR_CALLS"\n'
        '[ "$count" -eq 1 ] && exit 1\n'
        'exec /usr/bin/rmdir "$@"\n',
        encoding="utf-8",
    )
    rmdir.chmod(0o755)

    proc = subprocess.run(
        ["bash", "-c", probe_mod.bounded_probe_command(2)],
        env={
            **os.environ,
            "DT_TEST_RMDIR_CALLS": str(cleanup_calls),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "TMPDIR": str(tmp_path),
        },
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert proc.returncode == 0, proc.stderr
    assert int(cleanup_calls.read_text()) >= 2
    assert not list(tmp_path.glob("dt-probe.*"))


def test_probe_node_surfaces_query_failure_instead_of_zero_gpus(monkeypatch):
    output = (
        f"{GPU_ERROR}\ndriver unavailable sentinel\n"
        f"{SEP}\n"
        f"{SYS_SEP}\n"
        "16,1.0,65536000,49152000,1048576000,524288000,0.0\n"
    )
    monkeypatch.setattr(
        probe_mod,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, output, ""),
    )

    status = probe_mod.probe_node(Node(name="n1"), mem_threshold_mib=500)

    assert status.gpus == []
    assert status.error == "GPU query failed: driver unavailable sentinel"
    assert status.unreachable is False


def test_probe_node_surfaces_malformed_gpu_inventory_without_admitting_card(
    monkeypatch,
):
    output = (
        "0, GPU-bad, N/A, 24576, 0, 42, 0,\n"
        f"{SEP}\n"
        f"{SYS_SEP}\n"
        "16,1.0,65536000,49152000,1048576000,524288000,0.0\n"
    )
    monkeypatch.setattr(
        probe_mod,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, output, ""),
    )

    status = probe_mod.probe_node(Node(name="n1"), mem_threshold_mib=500)

    assert status.gpus == []
    assert status.error is None
    assert status.gpu_inventory_error == (
        "GPU inventory incomplete: 1 malformed row not schedulable"
    )
    assert status.unreachable is False


def test_probe_node_types_ssh_255_as_unreachable(monkeypatch):
    monkeypatch.setattr(
        probe_mod,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 255, "", "ssh: connect to host n1: No route to host"
        ),
    )

    status = probe_mod.probe_node(Node(name="n1"), mem_threshold_mib=500)

    assert status.gpus == []
    assert status.unreachable is True
    assert status.error == "ssh: connect to host n1: No route to host"


def test_probe_node_types_remote_probe_timeout_as_reachable_error(monkeypatch):
    captured = {}

    def timed_out(
        node_name,
        is_local,
        command,
        timeout,
        check=False,
        retry_stale_mux=False,
    ):
        captured.update(command=command, timeout=timeout)
        assert retry_stale_mux is True
        return subprocess.CompletedProcess([], 124, "", "")

    monkeypatch.setattr(probe_mod, "run_on", timed_out)

    status = probe_mod.probe_node(
        Node(name="n1"),
        mem_threshold_mib=500,
        timeout=7,
    )

    assert status.gpus == []
    assert status.unreachable is False
    assert status.error == "GPU probe timed out after 7s"
    assert captured["command"].startswith("umask 077; dt_outer_tmp=")
    assert "timeout --signal=TERM --kill-after=2s 7s" in captured["command"]
    assert captured["timeout"] > 7


def test_probe_node_uses_node_specific_timeout_by_default(monkeypatch):
    captured = {}

    def completed(
        node_name,
        is_local,
        command,
        timeout,
        check=False,
        retry_stale_mux=False,
    ):
        captured.update(command=command, timeout=timeout)
        assert retry_stale_mux is True
        return subprocess.CompletedProcess(
            [],
            0,
            f"0, GPU-x, 0, 24576, 0, 30, 0,\n{SEP}\n{SYS_SEP}\n",
            "",
        )

    monkeypatch.setattr(probe_mod, "run_on", completed)

    status = probe_mod.probe_node(
        Node(name="slow", probe_timeout_s=23.5),
        mem_threshold_mib=500,
    )

    assert status.error is None
    assert "23.5s" in captured["command"]
    assert captured["timeout"] == 28.5


def test_probe_node_types_outer_transport_timeout_as_unreachable(monkeypatch):
    def timed_out(*args, **kwargs):
        raise RemoteError("n1", "timed out after 12s")

    monkeypatch.setattr(probe_mod, "run_on", timed_out)

    status = probe_mod.probe_node(Node(name="n1"), mem_threshold_mib=500)

    assert status.gpus == []
    assert status.unreachable is True
    assert status.error == "[n1] timed out after 12s"


def test_probe_cache_atomic_writes_do_not_collide_across_callers(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(
        probe_mod,
        "probe_node",
        lambda node, threshold: NodeStatus(node=node.name),
    )
    original_write_text = Path.write_text
    both_shared_temp_writes_finished = threading.Barrier(2, timeout=1)

    def synchronized_write_text(path, data, *args, **kwargs):
        result = original_write_text(path, data, *args, **kwargs)
        if path.name == "probe.tmp":
            both_shared_temp_writes_finished.wait()
        return result

    monkeypatch.setattr(Path, "write_text", synchronized_write_text)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(probe_mod.probe_center, cfg, False) for _ in range(2)]
        results = [future.result() for future in futures]

    assert [[status.node for status in result] for result in results] == [
        ["n1"],
        ["n1"],
    ]
    assert not list(cfg.cache_dir().glob("*.tmp"))


def test_concurrent_fresh_probes_share_one_inflight_refresh(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    first_probe_started = threading.Event()
    second_lock_attempted = threading.Event()
    release_probe = threading.Event()
    calls = 0

    def probe(node, threshold):
        nonlocal calls
        calls += 1
        first_probe_started.set()
        assert release_probe.wait(timeout=1)
        return NodeStatus(node=node.name)

    original_lock = probe_mod._probe_refresh_lock
    lock_attempts = 0
    attempts_guard = threading.Lock()

    def observed_lock(path):
        nonlocal lock_attempts
        with attempts_guard:
            lock_attempts += 1
            if lock_attempts == 2:
                second_lock_attempted.set()
        return original_lock(path)

    monkeypatch.setattr(probe_mod, "probe_node", probe)
    monkeypatch.setattr(probe_mod, "_probe_refresh_lock", observed_lock)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(probe_mod.probe_center, cfg, False)
        assert first_probe_started.wait(timeout=1)
        second = pool.submit(probe_mod.probe_center, cfg, False)
        assert second_lock_attempted.wait(timeout=1)
        release_probe.set()
        results = [first.result(), second.result()]

    assert calls == 1
    assert [[status.node for status in result] for result in results] == [
        ["n1"],
        ["n1"],
    ]


def test_sequential_fresh_probes_do_not_reuse_completed_refresh(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    calls = 0

    def probe(node, threshold):
        nonlocal calls
        calls += 1
        return NodeStatus(node=node.name)

    monkeypatch.setattr(probe_mod, "probe_node", probe)

    probe_mod.probe_center(cfg, use_cache=False)
    probe_mod.probe_center(cfg, use_cache=False)

    assert calls == 2


def test_probe_results_survive_optional_cache_write_failure(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(
        probe_mod,
        "probe_node",
        lambda node, threshold: NodeStatus(node=node.name),
    )
    monkeypatch.setattr(
        probe_mod.tempfile,
        "mkstemp",
        lambda **kwargs: (_ for _ in ()).throw(OSError("read-only cache directory")),
    )

    statuses = probe_mod.probe_center(cfg, use_cache=False)

    assert [status.node for status in statuses] == ["n1"]


def test_oversized_probe_cache_is_ignored_and_replaced_by_live_data(
    tmp_path, monkeypatch
):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    cache = cfg.cache_dir() / "probe.json"
    cache.write_bytes(b"[]" + b"x" * 128)
    monkeypatch.setattr(probe_mod, "PROBE_CACHE_MAX_BYTES", 120)
    calls = []
    monkeypatch.setattr(
        probe_mod,
        "probe_node",
        lambda node, threshold: calls.append(node.name) or NodeStatus(node=node.name),
    )

    statuses = probe_mod.probe_center(cfg, use_cache=True)

    assert calls == ["n1"]
    assert [status.node for status in statuses] == ["n1"]
    assert cache.stat().st_size <= 120


def test_probe_cache_with_wrong_node_set_is_ignored(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    cache = cfg.cache_dir() / "probe.json"
    cache.write_text(json.dumps([asdict(NodeStatus(node="removed-node"))]))
    calls = []
    monkeypatch.setattr(
        probe_mod,
        "probe_node",
        lambda node, threshold: calls.append(node.name) or NodeStatus(node=node.name),
    )

    statuses = probe_mod.probe_center(cfg, use_cache=True)

    assert calls == ["n1"]
    assert [status.node for status in statuses] == ["n1"]


def test_probe_cache_with_invalid_gpu_values_is_ignored(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    cache = cfg.cache_dir() / "probe.json"
    unsafe = NodeStatus(
        node="n1",
        gpus=[Gpu(index=0, uuid="GPU-0", mem_used=-1, mem_total=0, util=-1)],
    )
    cache.write_text(json.dumps([asdict(unsafe)]))
    calls = []
    monkeypatch.setattr(
        probe_mod,
        "probe_node",
        lambda node, threshold: calls.append(node.name) or NodeStatus(node=node.name),
    )

    statuses = probe_mod.probe_center(cfg, use_cache=True)

    assert calls == ["n1"]
    assert statuses[0].gpus == []


def test_probe_cache_and_refresh_lock_do_not_follow_symlinks(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    cache_dir = cfg.cache_dir()
    outside_cache = tmp_path / "outside-cache"
    outside_lock = tmp_path / "outside-lock"
    outside_cache.write_text("must survive\n")
    outside_lock.write_text("must survive\n")
    (cache_dir / "probe.json").symlink_to(outside_cache)
    (cache_dir / "probe.lock").symlink_to(outside_lock)
    monkeypatch.setattr(
        probe_mod,
        "probe_node",
        lambda node, threshold: NodeStatus(node=node.name),
    )

    statuses = probe_mod.probe_center(cfg, use_cache=True)

    assert [status.node for status in statuses] == ["n1"]
    assert outside_cache.read_text() == "must survive\n"
    assert outside_lock.read_text() == "must survive\n"
    assert not (cache_dir / "probe.json").is_symlink()
    assert (cache_dir / "probe.lock").is_symlink()


def test_probe_cache_preserves_gpu_inventory_error(tmp_path, monkeypatch):
    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    expected = "GPU inventory incomplete: 1 malformed row not schedulable"
    monkeypatch.setattr(
        probe_mod,
        "probe_node",
        lambda node, threshold: NodeStatus(
            node=node.name,
            gpu_inventory_error=expected,
        ),
    )

    fresh = probe_mod.probe_center(cfg, use_cache=False)
    monkeypatch.setattr(
        probe_mod,
        "probe_node",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("fresh cache must avoid a reprobe")
        ),
    )
    cached = probe_mod.probe_center(cfg, use_cache=True)

    assert fresh[0].gpu_inventory_error == expected
    assert cached[0].gpu_inventory_error == expected

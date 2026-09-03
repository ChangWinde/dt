"""v0.8 operator-visibility round: probe owners, snapshot stats, auto center,
info parsing helpers."""

import pytest

from dt.cli.commands import ps as ps_cmd

from rich.text import Text

from dt.config import Project
from dt.dispatch import transferred_gib
from dt.probe import SEP, parse_probe_output
from dt.remote import best_center
from dt.render import busy_owners

SAMPLE_WHO = f"""0, GPU-aaa, 3, 81920, 0
1, GPU-bbb, 76000, 81920, 98
2, GPU-ccc, 800, 81920, 0
{SEP}
GPU-bbb, 12345, alice
GPU-bbb, 12346, bob
"""


def _max_terminal_width(output: str) -> int:
    """Return the widest rendered line, ignoring ANSI styling bytes."""
    return max(
        (Text.from_ansi(line).cell_len for line in output.splitlines()),
        default=0,
    )


# -- root onboarding -----------------------------------------------------------


def test_repository_sha_is_bounded_to_the_package_repo(monkeypatch, tmp_path):
    """dt --version must not read a commit from a repo that merely contains HOME."""
    import subprocess as subprocess_mod

    from dt import version

    monkeypatch.setattr(version, "SOURCE_COMMIT", None)

    calls = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        return subprocess_mod.CompletedProcess(argv, 0, "deadbee\n", "")

    monkeypatch.setattr(version.subprocess, "run", fake_run)

    fake_pkg_file = tmp_path / "checkout" / "src" / "dt" / "version.py"
    fake_pkg_file.parent.mkdir(parents=True)
    monkeypatch.setattr(version, "__file__", str(fake_pkg_file))

    # No .git and no pyproject near the package: never walk to $HOME's repo.
    assert version.repository_sha() is None
    assert calls == []

    # The dt checkout itself (src layout, pyproject, .git) is honored.
    (tmp_path / "checkout" / "pyproject.toml").write_text("[project]" + chr(10))
    (tmp_path / "checkout" / ".git").mkdir()
    assert version.repository_sha() == "deadbee"
    assert len(calls) == 1


def test_version_prefers_installed_source_commit(monkeypatch):
    from typer.testing import CliRunner

    from dt import __version__
    from dt import cli
    from dt import version

    monkeypatch.setattr(version, "SOURCE_COMMIT", "a" * 40)
    monkeypatch.setattr(version, "repository_sha", lambda: "wrong")
    monkeypatch.setattr(version, "install_digest", lambda: "b" * 12)
    monkeypatch.setattr(version, "payload_digest", lambda: "c" * 12)

    result = CliRunner().invoke(cli.app, ["--version"])

    assert result.exit_code == 0, result.output
    assert result.output == (
        f"dt {__version__} (git aaaaaaaaaaaa, "
        "install bbbbbbbbbbbb, payload cccccccccccc)\n"
    )


def test_repository_sha_ignores_ancestor_git_when_installed(monkeypatch, tmp_path):
    from dt import version

    installed = tmp_path / "site-packages" / "dt" / "version.py"
    installed.parent.mkdir(parents=True)
    (tmp_path / ".git").mkdir()  # unrelated ancestor repository

    monkeypatch.setattr(version, "__file__", str(installed))
    assert version.repository_sha() is None


def test_repository_sha_survives_missing_git_binary(monkeypatch, tmp_path):
    from dt import version

    checkout = tmp_path / "checkout"
    (checkout / "src" / "dt").mkdir(parents=True)
    (checkout / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (checkout / ".git").mkdir()
    module_file = checkout / "src" / "dt" / "version.py"
    module_file.write_text("", encoding="utf-8")

    def no_git(*_args, **_kwargs):
        raise FileNotFoundError("git")

    monkeypatch.setattr(version, "__file__", str(module_file))
    monkeypatch.setattr(version.subprocess, "run", no_git)
    assert version.repository_sha() is None


def test_root_help_has_a_compact_end_to_end_quick_start():
    import re

    from typer.testing import CliRunner

    from dt import cli

    result = CliRunner().invoke(cli.app, ["--help"], terminal_width=80)

    assert result.exit_code == 0, result.output
    output = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", result.output)
    normalized = " ".join(output.split())
    assert "Quick start" in normalized
    for command in (
        "dt free",
        "dt run -n exp -f -- python train.py",
        "dt ps",
        "dt logs exp -f",
        "dt pull exp --lite",
    ):
        assert command in normalized
    assert "seed Seed caches for slow-network nodes." in normalized
    assert "Everyday" in normalized
    assert "Experiments" in normalized
    assert "Operations" in normalized
    assert not re.search(r"\btask\s+Safe fast path", normalized)
    assert "nodes whose own internet is too slow" not in normalized
    assert _max_terminal_width(result.output) <= 80
    command_lines = [
        line.strip()
        for line in output.splitlines()
        if re.match(r"\d+\s+dt ", line.strip())
    ]
    assert len(command_lines) == 5

    seed_help = CliRunner().invoke(
        cli.app,
        ["seed", "--help"],
        terminal_width=80,
    )
    assert seed_help.exit_code == 0, seed_help.output
    assert "Idempotent" in seed_help.output
    assert "managed Python runtimes" in seed_help.output

    compatibility_help = CliRunner().invoke(cli.app, ["task", "--help"])
    assert compatibility_help.exit_code == 0, compatibility_help.output


def test_dense_submission_help_groups_everyday_and_advanced_options():
    from typer.testing import CliRunner

    from dt import cli

    result = CliRunner().invoke(cli.app, ["run", "--help"], terminal_width=80)

    assert result.exit_code == 0, result.output
    for heading in (
        "Everyday",
        "Scheduling & safety",
        "Reproducibility",
        "Follow & output",
    ):
        assert heading in result.output
    assert result.output.index("Everyday") < result.output.index("Scheduling & safety")
    assert _max_terminal_width(result.output) <= 80


# -- probe owners --------------------------------------------------------------


def test_probe_collects_owners():
    gpus = parse_probe_output(SAMPLE_WHO, mem_threshold_mib=500)
    by_idx = {g.index: g for g in gpus}
    assert by_idx[1].users == ["alice", "bob"]
    assert by_idx[0].users == []


def test_probe_owner_column_optional():
    # old two-column app rows (no owner) still parse
    text = f"0, GPU-x, 900, 81920, 50\n{SEP}\nGPU-x, 111\n"
    gpus = parse_probe_output(text, 500)
    assert gpus[0].procs == 1 and gpus[0].users == ["?"]


def test_busy_owners_rendering():
    gpus = [
        {"free": False, "procs": 2, "users": ["alice"]},
        {"free": False, "procs": 1, "users": ["alice"]},
        {"free": False, "procs": 1, "users": ["bob"]},
        {"free": True, "procs": 0, "users": []},
        {"free": False, "procs": 0, "users": []},  # zombie mem: no owner
        {
            "free": False,
            "procs": 0,
            "leased": True,
            "lease_owner": "20260724-1220_train-policy_abcd",
            "users": ["dt-lease"],
        },
    ]
    assert busy_owners(gpus) == "alice\u00d72 bob\u00d71 dt:train-policy\u00d71"


def test_busy_owners_escapes_remote_rich_markup():
    gpus = [{"free": False, "procs": 1, "users": ["[link=https://bad]alice[/link]"]}]

    assert busy_owners(gpus) == r"\[link=https://bad]alice\[/link]×1"


def test_free_table_stays_compact_without_owner_column():
    from dt.render import free_table

    table = free_table([_node("psibot", "psibot-ds", 1, total=1)])
    assert [column.header for column in table.columns] == [
        "node",
        "GPU free",
        "load",
        "VRAM free",
        "CPU",
        "RAM G",
        "disk",
        "IO",
    ]


def test_resource_tables_treat_configured_node_labels_as_literal_text():
    from rich.console import Console

    from dt.render import doctor_table, free_table

    node = "[b]x[/b]"
    free_console = Console(width=120, record=True, color_system=None)
    free_console.print(
        free_table(
            [
                {
                    "center": "c",
                    "node": node,
                    "gpus": [],
                    "system": {},
                }
            ]
        )
    )
    doctor_console = Console(width=120, record=True, color_system=None)
    doctor_console.print(
        doctor_table(
            [
                {
                    "center": "c",
                    "node": node,
                    "checks": {"ssh": "ok"},
                }
            ]
        )
    )

    assert node in free_console.export_text()
    assert node in doctor_console.export_text()


def test_free_table_headers_and_values_share_the_same_column_start():
    from rich.console import Console

    from dt.render import free_table

    row = {
        "center": "psibot",
        "node": "psibot-hm",
        "gpus": [
            {
                "index": 0,
                "free": False,
                "procs": 1,
                "leased": False,
                "users": ["alice"],
                "mem_used": 15 * 1024,
                "mem_total": 32 * 1024,
                "util": 86,
                "temperature": 81,
            }
        ],
        "system": {
            "cpu_cores": 32,
            "cpu_load1": 1.4,
            "mem_used_mib": 13 * 1024,
            "mem_total_mib": 63 * 1024,
            "disk_free_gib": 109,
            "disk_total_gib": 1024,
            "io_pressure": 0.0,
        },
    }
    console = Console(width=80, record=True, color_system=None)

    console.print(free_table([row]))
    header, values = console.export_text().splitlines()

    header_cursor = values_cursor = 0
    for label, value in (
        ("node", "psibot-hm"),
        ("GPU free", "0/1"),
        ("load", "86%/81°"),
        ("VRAM free", "17/32G"),
        ("CPU", "1.4/32"),
        ("RAM G", "13/63"),
        ("disk", "109G"),
        ("IO", "0.0%"),
    ):
        header_start = header.index(label, header_cursor)
        values_start = values.index(value, values_cursor)
        assert header_start == values_start
        header_cursor = header_start + len(label)
        values_cursor = values_start + len(value)


def test_free_table_gpu_availability_is_self_explanatory_at_80_columns():
    from rich.console import Console

    from dt.render import free_table

    row = {
        "center": "psibot",
        "node": "psibot-hm",
        "gpus": [
            {
                "index": 0,
                "free": False,
                "procs": 1,
                "leased": False,
                "users": ["alice"],
                "mem_used": 4096,
                "mem_total": 32768,
                "util": 87,
                "temperature": 69,
            },
            {
                "index": 1,
                "free": True,
                "procs": 0,
                "leased": False,
                "users": [],
                "mem_used": 0,
                "mem_total": 32768,
                "util": 0,
                "temperature": 35,
            },
        ],
        "system": {
            "cpu_cores": 32,
            "cpu_load1": 1.2,
            "mem_used_mib": 8192,
            "mem_total_mib": 65536,
            "disk_free_gib": 512,
            "disk_total_gib": 1024,
            "io_pressure": 0.0,
        },
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(free_table([row], who=True))
    rendered = console.export_text()

    assert "GPU free" in rendered
    assert "VRAM free" in rendered
    assert "60/64G" in rendered
    assert "1.2/32" in rendered
    assert "1/2 [1]" in rendered
    assert "87%/69°" in rendered
    assert "alice×1" in rendered
    assert len(rendered.splitlines()) == 2


def test_free_table_preserves_resource_values_at_60_columns():
    from rich.console import Console

    from dt.render import free_table

    row = {
        "center": "psibot",
        "node": "psibot-hm",
        "gpus": [
            {
                "index": 0,
                "free": False,
                "procs": 1,
                "leased": False,
                "users": ["alice"],
                "mem_used": 15 * 1024,
                "mem_total": 32 * 1024,
                "util": 86,
                "temperature": 81,
            }
        ],
        "system": {
            "cpu_cores": 32,
            "cpu_load1": 1.4,
            "mem_used_mib": 13 * 1024,
            "mem_total_mib": 63 * 1024,
            "disk_free_gib": 109,
            "disk_total_gib": 1024,
            "io_pressure": 0.0,
        },
    }
    console = Console(width=60, record=True, color_system=None)

    console.print(free_table([row]))
    rendered = console.export_text()

    for value in (
        "psibot-hm",
        "0/1",
        "86%/81°",
        "17/32G",
        "1.4/32",
        "13/63",
        "109G",
        "0.0%",
    ):
        assert value in rendered
    assert "…" not in rendered
    assert max(map(len, rendered.splitlines())) <= 60


def test_free_table_surfaces_incomplete_gpu_inventory_at_80_columns():
    from rich.console import Console

    from dt.render import free_table

    row = {
        "center": "psibot",
        "node": "psibot-ds",
        "gpus": [],
        "gpu_inventory_error": (
            "GPU inventory incomplete: 1 malformed row not schedulable"
        ),
        "system": {
            "cpu_cores": 32,
            "cpu_load1": 1.2,
            "mem_used_mib": 8192,
            "mem_total_mib": 65536,
            "disk_free_gib": 512,
            "disk_total_gib": 1024,
            "io_pressure": 0.0,
        },
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(free_table([row]))
    rendered = console.export_text()

    assert "0/0" in rendered
    assert "GPU inventory!" in rendered
    assert max(map(len, rendered.splitlines())) <= 80


def test_free_table_distinguishes_probe_timeout_from_offline_node():
    from rich.console import Console

    from dt.render import free_table

    rows = [
        {
            "center": "c",
            "node": "slow-probe",
            "gpus": [],
            "system": None,
            "error": "GPU probe timed out after 10s",
            "unreachable": False,
        },
        {
            "center": "c",
            "node": "offline-node",
            "gpus": [],
            "system": None,
            "error": "ssh: Connection timed out",
            "unreachable": True,
        },
    ]
    console = Console(width=100, record=True, color_system=None)

    console.print(free_table(rows))
    rendered = console.export_text()

    slow_row = next(line for line in rendered.splitlines() if "slow-probe" in line)
    offline_row = next(line for line in rendered.splitlines() if "offline-node" in line)
    assert "error" in slow_row
    assert "offline" not in slow_row
    assert "offline" in offline_row


def test_free_table_labels_reserved_pre_cuda_gpu_as_initializing():
    from rich.console import Console

    from dt.render import free_table

    row = {
        "center": "psibot",
        "node": "psibot-ds",
        "gpus": [
            {
                "index": 0,
                "free": False,
                "procs": 0,
                "leased": True,
                "lease_owner": "20260725-0940_dp-util_abcd",
                "users": ["dt-lease"],
                "mem_used": 15,
                "mem_total": 24564,
                "util": 0,
                "temperature": 42,
            }
        ],
        "system": {},
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(free_table([row], who=True))
    rendered = console.export_text()

    assert "init/42°" in rendered
    assert "dt:dp-util×1" in rendered


def test_free_table_labels_reserved_gpu_context_as_pulse_workload():
    from rich.console import Console

    from dt.render import free_table

    row = {
        "center": "psibot",
        "node": "psibot-ds",
        "gpus": [
            {
                "index": 0,
                "free": False,
                "procs": 0,
                "leased": True,
                "lease_owner": "20260727-0310_uo20-multidemo_abcd",
                "users": ["dt-lease"],
                "mem_used": 1536,
                "mem_total": 24564,
                "util": 0,
                "temperature": 49,
            }
        ],
        "system": {},
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(free_table([row], who=True))
    rendered = console.export_text()

    assert "pulse/49°" in rendered
    assert "dt:uo20-mul" in rendered


def test_free_table_warns_when_disk_headroom_is_low_at_80_columns():
    from rich.console import Console

    from dt.render import free_table

    row = {
        "center": "psibot",
        "node": "psibot-hm",
        "gpus": [
            {
                "index": 0,
                "free": True,
                "procs": 0,
                "leased": False,
                "users": [],
                "mem_used": 461,
                "mem_total": 32607,
                "util": 0,
                "temperature": 35,
            }
        ],
        "system": {
            "cpu_cores": 32,
            "cpu_load1": 0.9,
            "mem_used_mib": 9135,
            "mem_total_mib": 64013,
            "disk_free_gib": 85.0,
            "disk_total_gib": 1800.0,
            "io_pressure": 0.0,
        },
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(free_table([row], who=True))
    rendered = console.export_text()

    assert "85G!" in rendered
    assert "disk 4.7%" in rendered
    assert "1/1 [0]" in rendered
    assert max(map(len, rendered.splitlines())) <= 80


def test_disk_headroom_warning_covers_percentage_and_absolute_floor():
    from dt.render import _disk_low_headroom

    percentage_low, percentage = _disk_low_headroom(
        {"disk_free_gib": 85.0, "disk_total_gib": 1800.0}
    )
    absolute_low, absolute = _disk_low_headroom(
        {"disk_free_gib": 15.0, "disk_total_gib": 100.0}
    )
    healthy, healthy_fraction = _disk_low_headroom(
        {"disk_free_gib": 50.0, "disk_total_gib": 100.0}
    )

    assert percentage_low is True
    assert percentage == 85.0 / 1800.0
    assert absolute_low is True
    assert absolute == 0.15
    assert healthy is False
    assert healthy_fraction == 0.5


def test_agent_status_card_stays_readable_at_80_columns():
    from rich.console import Console

    from dt.cli.commands.agent import _agent_status_table

    status = {
        "alive": True,
        "pid": 3641709,
        "queued": 12,
        "running": 3,
        "registry_entries": 561,
        "registry_damage": 0,
        "handoff_state": "covered",
        "handoff_reason": "queued work covers the current runway",
        "queue_head": "20260725-0440_dp-libero-screen_abcd",
        "poll_s": 15,
        "active_poll_s": 2.0,
        "completion_wake": True,
        "max_my_jobs": None,
        "reserve_free_per_node": 0,
        "webhook": False,
        "log_bytes": 455630,
        "log_max_bytes": 10 * 1024 * 1024,
        "log_backups": 2,
    }
    compact_console = Console(width=80, record=True, color_system=None)
    compact_console.print(_agent_status_table(status))
    compact = compact_console.export_text()

    assert max(map(len, compact.splitlines())) <= 80
    assert "queued 12  ·  running 3  ·  history 561" in compact
    assert "covered  ·  queued work covers the current runway" in compact
    assert "dp-libero-screen · ref abcd" in compact
    assert "scheduler" not in compact
    assert "policy" not in compact
    assert "log" not in compact
    assert "20260725-0440_dp-libero-screen_abcd" not in compact

    verbose_console = Console(width=80, record=True, color_system=None)
    verbose_console.print(_agent_status_table(status, verbose=True))
    verbose = verbose_console.export_text()

    assert max(map(len, verbose.splitlines())) <= 80
    assert "15s idle  ·  2s queued  ·  completion wake" in verbose
    assert "445.0 KiB / 10.0 MiB  ·  2 backups" in verbose
    assert "20260725-0440_dp-libero-screen_abcd" in verbose


def test_stopped_agent_status_shows_the_recovery_command():
    from rich.console import Console

    from dt.cli.commands.agent import _agent_status_table

    status = {
        "alive": False,
        "pid": None,
        "queued": 2,
        "running": 0,
        "registry_entries": 2,
        "registry_damage": 0,
        "handoff_state": "agent_stopped",
        "handoff_reason": "queue agent is not running",
        "queue_head": None,
        "poll_s": 15,
        "active_poll_s": 2.0,
        "completion_wake": True,
        "max_my_jobs": None,
        "reserve_free_per_node": 0,
        "webhook": False,
        "log_bytes": 0,
        "log_max_bytes": 10 * 1024 * 1024,
        "log_backups": 0,
    }
    console = Console(width=80, record=True, color_system=None)

    console.print(_agent_status_table(status))

    assert "next  dt agent start" in console.export_text()


def test_verbose_agent_status_keeps_complete_queue_id_at_60_columns():
    from rich.console import Console

    from dt.cli.commands.agent import _agent_status_table

    job_id = "20260731-1311_uo114-libero_object_dp-v1_2d0f4c7f75c473c4"
    status = {
        "alive": True,
        "pid": 123,
        "queued": 1,
        "running": 1,
        "registry_entries": 2,
        "registry_damage": 0,
        "handoff_state": "covered",
        "handoff_reason": "queue has work",
        "queue_head": job_id,
        "poll_s": 15,
        "active_poll_s": 2.0,
        "completion_wake": True,
        "max_my_jobs": None,
        "reserve_free_per_node": 0,
        "webhook": False,
        "log_bytes": 0,
        "log_max_bytes": 10 * 1024 * 1024,
        "log_backups": 0,
    }
    console = Console(width=60, record=True, color_system=None)

    console.print(_agent_status_table(status, verbose=True))
    rendered = console.export_text()

    assert job_id in "".join(rendered.split())
    assert "…" not in rendered


def test_run_help_names_the_required_command_boundary():
    from typer.testing import CliRunner

    from dt import cli

    result = CliRunner().invoke(cli.app, ["run", "--help"])

    assert result.exit_code == 0, result.output
    assert "-- COMMAND [ARGS]..." in result.output


def test_free_table_who_keeps_resources_readable_with_offline_node():
    from rich.console import Console

    from dt.render import free_table

    system = {
        "cpu_cores": 32,
        "cpu_load1": 1.2,
        "mem_used_mib": 8192,
        "mem_total_mib": 65536,
        "disk_free_gib": 512,
        "disk_total_gib": 1024,
        "io_pressure": 0.0,
    }
    rows = [
        {
            "center": "psibot",
            "node": "psibot-hm",
            "gpus": [
                {
                    "index": 0,
                    "free": False,
                    "procs": 1,
                    "leased": False,
                    "users": ["psibot"],
                    "mem_used": 4096,
                    "mem_total": 32768,
                }
            ],
            "system": system,
        },
        {
            "center": "psibot",
            "node": "psibot-ds",
            "error": ("ssh: connect to host 172.16.6.78 port 22: No route to host"),
        },
        {
            "center": "psibot",
            "node": "psibot-ys",
            "gpus": [
                {
                    "index": 0,
                    "free": False,
                    "procs": 1,
                    "leased": False,
                    "users": ["frankie"],
                    "mem_used": 17435,
                    "mem_total": 24576,
                }
            ],
            "system": system,
        },
    ]
    console = Console(width=80, record=True, color_system=None)
    console.print(free_table(rows, who=True))
    rendered = console.export_text()

    assert "psibot-hm" in rendered
    assert "psibot-ds" in rendered
    assert "psibot-ys" in rendered
    assert "VRAM" in rendered
    assert "CPU" in rendered
    assert "RAM" in rendered
    assert "disk" in rendered
    assert "IO" in rendered
    assert "in use" in rendered
    assert "offline" in rendered
    assert "no route" in rendered
    assert "psibot×1" in rendered
    assert "frankie×1" in rendered


def test_free_human_explains_idle_gpu_and_keeps_public_json_unchanged(
    tmp_path, monkeypatch
):
    import json

    from typer.testing import CliRunner

    from dt import agent, cli
    from dt.config import HeadConfig, Node

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    rows = [_node("c", "n1", 1, total=1)]
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "probe_center", lambda *args, **kwargs: [])
    monkeypatch.setattr(cli, "status_as_dict", lambda *args, **kwargs: rows)
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)

    human = CliRunner().invoke(cli.app, ["free", "--who"])
    machine = CliRunner().invoke(cli.app, ["free", "--json"])
    explained = CliRunner().invoke(
        cli.app,
        ["free", "--json", "--explain"],
    )

    assert human.exit_code == 0, human.output
    normalized = " ".join(human.output.split())
    assert "1/1 GPU free" in normalized
    assert "0 running" in normalized
    assert "0 queued" in normalized
    assert "idle: no dt work queued" in normalized
    assert "dt task n1" in normalized
    assert machine.exit_code == 0, machine.output
    assert json.loads(machine.stdout) == rows
    assert explained.exit_code == 0, explained.output
    payload = json.loads(explained.stdout)
    assert payload["schema_version"] == "dt_free_explain_v1"
    assert payload["summary"] == {
        "centers": 1,
        "reachable_nodes": 1,
        "unavailable_nodes": 0,
        "gpus_total": 1,
        "gpus_free": 1,
        "running": 0,
        "queued": 0,
    }
    assert payload["resources"] == rows
    center = payload["centers"][0]
    assert center["state"] == "idle_no_dt_work"
    assert center["capacity"]["free_by_node"] == {"n1": 1}
    scheduler = center["scheduler"]
    assert scheduler["running"] == scheduler["queued"] == 0
    assert scheduler["runnable_queued"] == scheduler["blocked_queued"] == 0
    assert scheduler["model"] == {
        "schema_version": "dt_scheduler_state_v1",
        "state": "idle",
        "idle_reason": "queue is empty",
        "agent": {"alive": True, "heartbeat_stale": False},
        "running": 0,
        "queue_depth": 0,
        "runnable_queued": 0,
        "blocked_queued": 0,
        "waiting_queued": 0,
        "next_job_id": None,
        "next_condition": None,
        "registry_damage": 0,
        "capacity": {
            "schema_version": "dt_schedulable_capacity_v1",
            "nodes": [
                {
                    "node": "n1",
                    "drained": False,
                    "available": True,
                    "physical_free_gpus": 1,
                    "schedulable_free_gpus": 1,
                }
            ],
        },
        "queue": [],
    }


def test_free_explain_reports_stopped_agent_and_blocked_queue():

    from dt.cli.commands.free import _free_explain_payload

    row = _node("c", "n1", 1, total=1)
    row["_scheduler"] = {
        "center": "c",
        "running": 0,
        "running_nodes": [],
        "queued": 1,
        "queue_head_job_id": "queued-id",
        "queue_head_reason": "waiting: batch FIFO",
        "agent_alive": False,
    }

    stopped = _free_explain_payload([row])["centers"][0]

    assert stopped["state"] == "queue_agent_stopped"
    assert stopped["actions"] == [
        {
            "kind": "start_agent",
            "argv": ["dt", "agent", "start"],
        }
    ]

    row["_scheduler"]["agent_alive"] = True
    row["_scheduler"]["queue_head_reason"] = "blocked: n1: path-missing: /data/libero"

    blocked = _free_explain_payload([row])["centers"][0]

    assert blocked["state"] == "queue_head_blocked"
    assert blocked["message"] == "blocked: n1: path-missing: /data/libero"
    assert blocked["actions"] == [
        {
            "kind": "inspect_queue_head",
            "job_id": "queued-id",
            "argv": ["dt", "info", "queued-id"],
        }
    ]


def test_free_explain_distinguishes_incomplete_inventory_from_no_gpu_node():

    from dt.cli.commands.free import _free_explain_payload

    row = {
        "center": "c",
        "node": "n1",
        "gpus": [],
        "system": None,
        "error": None,
        "gpu_inventory_error": (
            "GPU inventory incomplete: 1 malformed row not schedulable"
        ),
        "unreachable": False,
        "_scheduler": {
            "center": "c",
            "running": 0,
            "running_nodes": [],
            "queued": 0,
            "queue_head_job_id": None,
            "queue_head_reason": None,
            "queue_head_pin_node": None,
            "queue_head_gpus_requested": None,
            "reserve_free_per_node": 0,
            "agent_alive": True,
        },
    }

    center = _free_explain_payload([row])["centers"][0]

    assert center["state"] == "gpu_inventory_incomplete"
    assert center["message"] == (
        "GPU inventory incomplete: n1: 1 malformed row not schedulable"
    )
    assert center["capacity"]["gpu_inventory_errors"] == {
        "n1": "GPU inventory incomplete: 1 malformed row not schedulable",
    }


def test_free_explain_reports_queue_runway_and_free_capacity():

    from dt.cli.commands.free import _free_explain_payload

    rows = [
        _node("c", "busy", 0, total=1),
        _node("c", "free", 1, total=1),
    ]
    context = {
        "center": "c",
        "running": 1,
        "running_nodes": ["busy"],
        "queued": 0,
        "queue_head_job_id": None,
        "queue_head_reason": None,
        "agent_alive": True,
    }
    for row in rows:
        row["_scheduler"] = context

    payload = _free_explain_payload(rows)
    center = payload["centers"][0]

    assert payload["summary"]["running"] == 1
    assert payload["summary"]["queued"] == 0
    assert center["state"] == "queue_runway_empty_with_free_capacity"
    assert center["actions"] == [
        {
            "kind": "submit_now",
            "node": "free",
            "argv": ["dt", "task", "free", "COMMAND", "-n", "NAME"],
        },
        {
            "kind": "queue_successor",
            "node": "busy",
            "argv": ["dt", "task", "busy", "COMMAND", "-n", "NAME"],
        },
    ]
    assert all("_scheduler" not in row for row in payload["resources"])


def test_free_explain_marks_old_head_scheduler_unknown():

    from dt.cli.commands.free import _free_explain_payload

    row = _node("old", "n1", 1, total=1)

    payload = _free_explain_payload([row])

    assert payload["summary"]["running"] is None
    assert payload["summary"]["queued"] is None
    assert payload["resources"] == [row]
    assert payload["centers"][0]["state"] == "scheduler_unavailable"
    assert payload["centers"][0]["actions"] == []


def test_free_human_idle_suggestion_avoids_known_low_disk_node():
    from rich.console import Console

    from dt.cli.commands.free import _free_view

    rows = [
        _node("c", "low-disk", 1, total=1),
        _node("c", "healthy", 1, total=1),
    ]
    rows[0]["system"] = {
        "disk_free_gib": 71.0,
        "disk_total_gib": 1832.0,
    }
    rows[1]["system"] = {
        "disk_free_gib": 1281.0,
        "disk_total_gib": 1832.0,
    }
    context = {
        "center": "c",
        "running": 0,
        "running_nodes": [],
        "queued": 0,
        "queue_head_job_id": None,
        "queue_head_reason": None,
        "agent_alive": True,
    }
    for row in rows:
        row["_scheduler"] = context
    console = Console(width=80, record=True, color_system=None)

    console.print(_free_view(rows, who=False))
    normalized = " ".join(console.export_text().split())

    assert "idle: no dt work queued" in normalized
    assert "submit: dt task healthy" in normalized
    assert "submit: dt task low-disk" not in normalized


def test_free_human_warns_when_active_queue_has_no_successor(tmp_path, monkeypatch):
    import json

    from typer.testing import CliRunner

    from dt import agent, cli
    from dt.config import HeadConfig, Node
    from dt.jobs import JobEntry, save

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    save(
        cfg,
        JobEntry(
            job_id="running-id",
            name="running",
            center="c",
            project="p",
            node="n1",
            node_local=False,
            job_dir="dt/jobs/running-id",
            session="dt_running",
            cmd="python train.py",
            gpus=[0],
            status="running",
        ),
    )
    rows = [_node("c", "n1", 0, total=1)]
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "probe_center", lambda *args, **kwargs: [])
    monkeypatch.setattr(cli, "status_as_dict", lambda *args, **kwargs: rows)
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)

    human = CliRunner().invoke(cli.app, ["free"])
    machine = CliRunner().invoke(cli.app, ["free", "--json"])

    assert human.exit_code == 0, human.output
    normalized = " ".join(human.output.split())
    assert "0/1 GPU free · 1 running · 0 queued" in normalized
    assert "queue ends after 1 running job" in normalized
    assert "dt task n1" in normalized
    assert machine.exit_code == 0, machine.output
    assert json.loads(machine.stdout) == rows


def test_free_human_warns_when_queue_empty_with_unused_capacity():
    from rich.console import Console

    from dt.cli.commands.free import _free_view

    rows = [
        _node("c", "busy", 0, total=1),
        _node("c", "free", 1, total=1),
    ]
    context = {
        "center": "c",
        "running": 1,
        "running_nodes": ["busy"],
        "queued": 0,
        "queue_head_job_id": None,
        "queue_head_reason": None,
        "agent_alive": True,
    }
    for row in rows:
        row["_scheduler"] = context
    console = Console(width=80, record=True, color_system=None)

    console.print(_free_view(rows, who=False))
    rendered = console.export_text()
    normalized = " ".join(rendered.split())

    assert max(map(len, rendered.splitlines())) <= 80
    assert "queue empty; additional GPU capacity is available now" in normalized
    assert "dt task free" in normalized
    assert "keep busy: dt task busy" in normalized


def test_free_human_queue_runway_old_head_context_uses_safe_node_placeholder():
    from rich.console import Console

    from dt.cli.commands.free import _free_view

    row = _node("c", "busy", 0, total=1)
    row["_scheduler"] = {
        "center": "c",
        "running": 2,
        "queued": 0,
        "queue_head_job_id": None,
        "queue_head_reason": None,
        "agent_alive": True,
    }
    console = Console(width=80, record=True, color_system=None)

    console.print(_free_view([row], who=False))
    normalized = " ".join(console.export_text().split())

    assert "queue ends after 2 running jobs" in normalized
    assert "dt task NODE" in normalized


def test_ps_watch_view_warns_when_running_center_has_no_successor():
    from rich.console import Console

    from dt.cli.commands.ps import _ps_view

    rows = [
        {
            "name": "train",
            "job_id": "running-id",
            "center": "c",
            "node": "n1",
            "gpus": [0],
            "status": "running",
            "exit_code": None,
            "created_at": 100.0,
            "duration_s": 30.0,
            "cmd": "python train.py",
            "resources": None,
        }
    ]
    console = Console(width=80, record=True, color_system=None)

    console.print(
        _ps_view(
            rows,
            {},
            all_=False,
            wide=False,
            poll=2.0,
            show_queue_runway=True,
            laptop=False,
        )
    )
    rendered = console.export_text()
    normalized = " ".join(rendered.split())

    assert max(map(len, rendered.splitlines())) <= 80
    assert "queue ends after 1 running job" in normalized
    assert "queue next: dt task n1" in normalized


def test_ps_watch_view_treats_center_errors_as_literal_text():
    from rich.console import Console

    from dt.cli.commands.ps import _ps_view

    console = Console(width=100, record=True, color_system=None)
    console.print(
        _ps_view(
            [],
            {"east": "bad [/yellow] /tmp/[broken]"},
            all_=False,
            wide=False,
            poll=2.0,
        )
    )

    assert "bad [/yellow] /tmp/[broken]" in console.export_text()


def test_ps_watch_view_laptop_runway_command_pins_the_center():
    from rich.console import Console

    from dt.cli.commands.ps import _ps_view

    rows = [
        {
            "name": "train",
            "job_id": "running-id",
            "center": "east",
            "node": "n1",
            "gpus": [0],
            "status": "running",
            "exit_code": None,
            "created_at": 100.0,
            "duration_s": 30.0,
            "cmd": "python train.py",
            "resources": None,
        }
    ]
    console = Console(width=80, record=True, color_system=None)

    console.print(
        _ps_view(
            rows,
            {},
            all_=False,
            wide=False,
            poll=2.0,
            show_queue_runway=True,
            laptop=True,
        )
    )
    normalized = " ".join(console.export_text().split())

    assert "queue next: dt task n1 'COMMAND' -n NAME -c east" in normalized


def test_ps_watch_view_status_filtered_mode_does_not_infer_queue_runway():
    from rich.console import Console

    from dt.cli.commands.ps import _ps_view

    rows = [
        {
            "name": "train",
            "job_id": "running-id",
            "center": "c",
            "node": "n1",
            "gpus": [0],
            "status": "running",
            "exit_code": None,
            "created_at": 100.0,
            "duration_s": 30.0,
            "cmd": "python train.py",
            "resources": None,
        }
    ]
    console = Console(width=80, record=True, color_system=None)

    console.print(
        _ps_view(
            rows,
            {},
            all_=False,
            wide=False,
            poll=2.0,
            show_queue_runway=False,
            laptop=False,
        )
    )
    normalized = " ".join(console.export_text().split())

    assert "queue ends" not in normalized
    assert "queue next:" not in normalized


def test_ps_watch_view_suppresses_runway_warning_when_successor_is_queued():
    from rich.console import Console

    from dt.cli.commands.ps import _ps_view

    rows = [
        {
            "name": "train",
            "job_id": "running-id",
            "center": "c",
            "node": "n1",
            "gpus": [0],
            "status": "running",
            "exit_code": None,
            "created_at": 100.0,
            "duration_s": 30.0,
            "cmd": "python train.py",
            "resources": None,
        },
        {
            "name": "next",
            "job_id": "queued-id",
            "center": "c",
            "node": "-",
            "pin_node": "n1",
            "gpus": [],
            "gpus_requested": 1,
            "status": "queued",
            "exit_code": None,
            "created_at": 101.0,
            "duration_s": None,
            "cmd": "python next.py",
            "resources": None,
        },
    ]
    console = Console(width=80, record=True, color_system=None)

    console.print(
        _ps_view(
            rows,
            {},
            all_=False,
            wide=False,
            poll=2.0,
            show_queue_runway=True,
            laptop=False,
        )
    )
    normalized = " ".join(console.export_text().split())

    assert "queue ends" not in normalized
    assert "queue next:" not in normalized


def test_ps_watch_view_multi_center_runway_avoids_guessing_one_command():
    from rich.console import Console

    from dt.cli.commands.ps import _ps_view

    rows = [
        {
            "name": f"train-{center}",
            "job_id": f"running-{center}",
            "center": center,
            "node": node,
            "gpus": [0],
            "status": "running",
            "exit_code": None,
            "created_at": 100.0,
            "duration_s": 30.0,
            "cmd": "python train.py",
            "resources": None,
        }
        for center, node in (("east", "e1"), ("west", "w1"))
    ]
    console = Console(width=80, record=True, color_system=None)

    console.print(
        _ps_view(
            rows,
            {},
            all_=False,
            wide=False,
            poll=2.0,
            show_queue_runway=True,
            laptop=True,
        )
    )
    rendered = console.export_text()
    normalized = " ".join(rendered.split())

    assert max(map(len, rendered.splitlines())) <= 80
    assert "2 centers have running jobs but no queued successor" in normalized
    assert "inspect: dt free" in normalized
    assert "dt task e1" not in normalized
    assert "dt task w1" not in normalized


def test_free_human_explains_queued_work_stalled_by_dead_agent(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from dt import agent, cli
    from dt.config import HeadConfig, Node
    from dt.jobs import JobEntry, save

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    save(
        cfg,
        JobEntry(
            job_id="queued-id",
            name="queued",
            center="c",
            project="p",
            node="-",
            node_local=False,
            job_dir="dt/jobs/queued-id",
            session="dt_queued",
            cmd="true",
            status="queued",
            reason="waiting: batch FIFO",
        ),
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "probe_center", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        cli,
        "status_as_dict",
        lambda *args, **kwargs: [_node("c", "n1", 1, total=1)],
    )
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: None)

    result = CliRunner().invoke(cli.app, ["free"])
    explained = CliRunner().invoke(cli.app, ["free", "--explain"])

    assert result.exit_code == 0, result.output
    normalized = " ".join(result.output.split())
    assert "0 running" in normalized
    assert "1 queued" in normalized
    assert "stalled: queue agent is stopped" in normalized
    assert "dt agent start" in normalized
    assert "waiting: batch FIFO" not in normalized
    assert explained.exit_code == 0, explained.output
    explained_normalized = " ".join(explained.output.split())
    assert "next job queued-id" in explained_normalized
    assert "reason waiting: batch FIFO" in explained_normalized


def test_laptop_free_human_requests_scheduler_context(monkeypatch):
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"c": "head"},
        default_center="c",
    )
    calls = []
    row = _node("c", "n1", 1, total=1)
    row["_scheduler"] = {
        "center": "c",
        "running": 0,
        "queued": 0,
        "queue_head_job_id": None,
        "queue_head_reason": None,
        "agent_alive": True,
    }
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "fan_json",
        lambda cfg_, argv: calls.append(argv) or ([row], {}),
    )

    result = CliRunner().invoke(cli.app, ["free"])

    assert result.exit_code == 0, result.output
    assert calls == [["free", "--scheduler-context"]]
    normalized = " ".join(result.output.split())
    assert "idle: no dt work queued" in normalized
    assert "dt task n1 'COMMAND' -n NAME -c c" in normalized


def test_laptop_free_explain_pins_actions_to_their_centers(monkeypatch):
    import json

    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"east": "east-head", "west": "west-head"},
        default_center="east",
    )
    east = _node("east", "east-gpu", 1, total=1)
    east["_scheduler"] = {
        "center": "east",
        "running": 0,
        "queued": 0,
        "agent_alive": True,
    }
    west = _node("west", "west-gpu", 1, total=1)
    west["_scheduler"] = {
        "center": "west",
        "running": 0,
        "queued": 1,
        "queue_head_job_id": "west-queued",
        "queue_head_reason": "waiting: capacity",
        "agent_alive": False,
    }
    calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "fan_json",
        lambda cfg_, argv: calls.append(argv) or ([east, west], {}),
    )

    result = CliRunner().invoke(
        cli.app,
        ["free", "--json", "--explain"],
    )

    assert result.exit_code == 0, result.output
    assert calls == [["free", "--scheduler-context"]]
    centers = {
        center["center"]: center for center in json.loads(result.stdout)["centers"]
    }
    assert centers["east"]["actions"] == [
        {
            "kind": "submit",
            "node": "east-gpu",
            "argv": [
                "dt",
                "task",
                "east-gpu",
                "COMMAND",
                "-n",
                "NAME",
                "-c",
                "east",
            ],
        }
    ]
    assert centers["west"]["actions"] == [
        {
            "kind": "start_agent",
            "argv": ["dt", "agent", "start", "-c", "west"],
        }
    ]


def test_free_scheduler_explains_untracked_dt_lease():
    from rich.console import Console

    from dt.cli.commands.free import _free_view

    row = _node("c", "n1", 0, total=1)
    row["gpus"][0].update(
        {
            "leased": True,
            "lease_owner": "finished-but-lock-held",
            "procs": 0,
        }
    )
    row["_scheduler"] = {
        "center": "c",
        "running": 0,
        "queued": 0,
        "queue_head_job_id": None,
        "queue_head_reason": None,
        "agent_alive": True,
    }
    console = Console(width=80, record=True, color_system=None)

    console.print(_free_view([row], who=True))
    rendered = console.export_text()
    normalized = " ".join(rendered.split())

    assert max(map(len, rendered.splitlines())) <= 80
    assert "registry idle, but 1 dt GPU lease remains" in normalized
    assert "dt info finished-but-lock-held" in normalized
    assert "occupied outside dt" not in normalized


def test_free_scheduler_default_hides_verbose_queue_head_diagnosis():
    from rich.console import Console

    from dt.cli.commands.free import _free_view

    rows = [
        _node("c", "psibot-hm", 0, total=1),
        _node("c", "psibot-ds", 0, total=1),
        {
            "center": "c",
            "node": "psibot-ys",
            "gpus": [],
            "system": None,
            "error": "timeout",
        },
    ]
    job_id = "20260731-1311_uo114-libero_spatial_dp-v1_544d1a0d6161d898"
    reason = (
        "waiting: no free capacity (psibot-hm: 0 free < 1 wanted; "
        "busy: gpu0 another-long-running-job 16.5/31.8GiB util93%)"
    )
    context = {
        "center": "c",
        "running": 3,
        "queued": 12,
        "queue_head_job_id": job_id,
        "queue_head_reason": reason,
        "queue_head_pin_node": "psibot-hm",
        "queue_head_gpus_requested": 1,
        "reserve_free_per_node": 0,
        "agent_alive": True,
    }
    for row in rows:
        row["_scheduler"] = context
    console = Console(width=80, record=True, color_system=None)

    console.print(_free_view(rows, who=False))
    rendered = console.export_text()
    normalized = " ".join(rendered.split())

    assert max(map(len, rendered.splitlines())) <= 80
    assert "0/2 GPU free · 3 running · 12 queued" in normalized
    assert "next needs 1 GPU on psibot-hm" in normalized
    assert job_id not in normalized
    assert reason not in normalized
    assert "· head" not in normalized


def test_free_scheduler_explains_pinned_queue_cannot_use_free_gpu_elsewhere():
    from rich.console import Console

    from dt.cli.commands.free import _free_view

    rows = [
        _node("c", "n1", 1, total=1),
        _node("c", "n2", 0, total=1),
    ]
    context = {
        "center": "c",
        "running": 1,
        "queued": 1,
        "queue_head_job_id": "queued-on-n2",
        "queue_head_reason": "waiting: batch FIFO",
        "queue_head_pin_node": "n2",
        "queue_head_gpus_requested": 1,
        "reserve_free_per_node": 0,
        "agent_alive": True,
    }
    for row in rows:
        row["_scheduler"] = context
    console = Console(width=80, record=True, color_system=None)

    console.print(_free_view(rows, who=False, explain=True))
    normalized = " ".join(console.export_text().split())

    assert "1/2 GPU free · 1 running · 1 queued" in normalized
    assert "next needs 1 GPU on n2" in normalized
    assert "1 free elsewhere is not eligible" in normalized
    assert "dispatching" not in normalized
    assert "next job queued-on-n2" in normalized
    assert "reason waiting: batch FIFO" in normalized


def test_free_scheduler_prioritizes_job_specific_block_over_free_capacity():
    from rich.console import Console

    from dt.cli.commands.free import _free_view

    row = _node("c", "n1", 1, total=1)
    row["_scheduler"] = {
        "center": "c",
        "running": 0,
        "queued": 1,
        "queue_head_job_id": "blocked",
        "queue_head_reason": "blocked: n1: path-missing: /data/libero",
        "queue_head_pin_node": "n1",
        "queue_head_gpus_requested": 1,
        "reserve_free_per_node": 0,
        "agent_alive": True,
    }
    console = Console(width=80, record=True, color_system=None)

    console.print(_free_view([row], who=False, explain=True))
    normalized = " ".join(console.export_text().split())

    assert "next is blocked by a job constraint" in normalized
    assert "path-missing: /data/libero" in normalized
    assert "dispatching" not in normalized


def test_laptop_free_human_falls_back_for_old_head_without_scheduler_option(
    monkeypatch,
):
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"c": "head"},
        default_center="c",
    )
    calls = []

    def fan(_cfg, argv):
        calls.append(argv)
        if "--scheduler-context" in argv:
            return [], {"c": "No such option: --scheduler-context"}
        return [_node("c", "n1", 1, total=1)], {}

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "fan_json", fan)

    result = CliRunner().invoke(cli.app, ["free"])

    assert result.exit_code == 0, result.output
    assert calls == [
        ["free", "--scheduler-context"],
        ["free"],
    ]
    assert "n1" in result.output
    assert "idle: no dt work queued" not in result.output


def test_free_watch_json_streams_until_interrupted(tmp_path, monkeypatch):
    import json

    from typer.testing import CliRunner

    from dt import cli
    from dt.config import HeadConfig, Node
    from dt.probe import NodeStatus

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.time, "monotonic", lambda: 10.0)
    monkeypatch.setattr(
        cli,
        "probe_center",
        lambda cfg_, use_cache=True, **_kwargs: [NodeStatus(node="n1")],
    )
    sleeps = []

    def stop_after_first_frame(seconds):
        sleeps.append(seconds)
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.time, "sleep", stop_after_first_frame)

    result = CliRunner().invoke(cli.app, ["free", "--watch", "--json"])

    assert result.exit_code == 130
    assert sleeps == [2]
    frames = [json.loads(line) for line in result.stdout.splitlines()]
    assert frames[:-1] == [
        [
            {
                "center": "c",
                "node": "n1",
                "gpus": [],
                "system": None,
                "error": None,
                "unreachable": False,
                "stale": False,
            }
        ]
    ]
    assert frames[-1] == {
        "schema_version": "dt_stream_event_v1",
        "event": "interrupted",
        "exit_code": 130,
    }


def test_free_watch_json_explain_streams_versioned_frames(tmp_path, monkeypatch):
    import json

    from typer.testing import CliRunner

    from dt import agent, cli
    from dt.config import HeadConfig, Node
    from dt.probe import NodeStatus

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.time, "monotonic", lambda: 10.0)
    monkeypatch.setattr(
        cli,
        "probe_center",
        lambda cfg_, use_cache=True, **_kwargs: [NodeStatus(node="n1")],
    )
    monkeypatch.setattr(agent, "alive_pid", lambda cfg_: 123)
    sleeps = []

    def stop_after_first_frame(seconds):
        sleeps.append(seconds)
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.time, "sleep", stop_after_first_frame)

    result = CliRunner().invoke(
        cli.app,
        ["free", "--watch", "--json", "--explain"],
    )

    assert result.exit_code == 130, result.output
    assert sleeps == [2]
    frames = [json.loads(line) for line in result.stdout.splitlines()]
    assert len(frames) == 2
    assert frames[0]["schema_version"] == "dt_free_explain_v1"
    assert frames[0]["summary"] == {
        "centers": 1,
        "reachable_nodes": 1,
        "unavailable_nodes": 0,
        "gpus_total": 0,
        "gpus_free": 0,
        "running": 0,
        "queued": 0,
    }
    assert frames[0]["centers"][0]["state"] == "no_gpu_inventory"
    assert frames[1] == {
        "schema_version": "dt_stream_event_v1",
        "event": "interrupted",
        "exit_code": 130,
    }


def test_laptop_free_watch_requests_fresh_frames_and_honors_poll(monkeypatch):
    import json

    import pytest
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    calls = []
    sleeps = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monotonic = iter([10.0, 10.2])
    monkeypatch.setattr(cli.time, "monotonic", lambda: next(monotonic))
    monkeypatch.setattr(
        cli,
        "fan_json",
        lambda cfg_, argv: (
            calls.append(argv)
            or (
                [
                    {
                        "center": "test",
                        "node": "n1",
                        "gpus": [],
                        "system": None,
                        "error": None,
                        "unreachable": False,
                    }
                ],
                {},
            )
        ),
    )

    def stop_after_first_frame(seconds):
        sleeps.append(seconds)
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.time, "sleep", stop_after_first_frame)

    result = CliRunner().invoke(
        cli.app,
        ["free", "--watch", "--json", "--poll", "0.25"],
    )

    assert result.exit_code == 130, result.output
    assert calls == [["free", "--fresh"]]
    assert sleeps == [pytest.approx(0.05)]
    frames = [json.loads(line) for line in result.stdout.splitlines()]
    assert frames[0][0]["node"] == "n1"
    assert frames[-1]["event"] == "interrupted"


def test_ps_watch_subtracts_collection_time_from_poll(tmp_path, monkeypatch):
    import json

    import pytest
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import HeadConfig

    cfg = HeadConfig(
        center="c",
        nodes=[],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monotonic = iter([10.0, 10.4])
    sleeps = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli.time, "monotonic", lambda: next(monotonic))
    monkeypatch.setattr(ps_cmd, "_gather_ps_rows", lambda *args, **kwargs: ([], {}))

    def stop_after_first_frame(seconds):
        sleeps.append(seconds)
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.time, "sleep", stop_after_first_frame)

    result = CliRunner().invoke(
        cli.app,
        ["ps", "--watch", "--json", "--poll", "0.5"],
    )

    assert result.exit_code == 0, result.output
    assert sleeps == [pytest.approx(0.1)]
    assert json.loads(result.stdout) == []


def test_laptop_doctor_overlaps_head_and_node_diagnostics(monkeypatch):
    import json
    import subprocess
    import threading

    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(centers={"test": "head"}, default_center="test")
    node_diagnostics_started = threading.Event()

    def version(*args, **kwargs):
        assert node_diagnostics_started.wait(timeout=1)
        return subprocess.CompletedProcess([], 0, "dt 1.2.3\n", "")

    def fan(*args, **kwargs):
        node_diagnostics_started.set()
        return [], {}

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "remote_dt", version)
    monkeypatch.setattr(cli, "fan_json", fan)

    result = CliRunner().invoke(cli.app, ["doctor", "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["nodes"][0]["checks"] == {
        "ssh": "ok",
        "dt": "1.2.3",
    }


def test_laptop_doctor_checks_head_versions_in_parallel(monkeypatch):
    import json
    import subprocess
    import threading

    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"east": "head-a", "west": "head-b"},
        default_center="east",
    )
    both_versions_started = threading.Barrier(2, timeout=1)

    def version(*args, **kwargs):
        both_versions_started.wait()
        return subprocess.CompletedProcess([], 0, "dt 1.2.3\n", "")

    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "remote_dt", version)
    monkeypatch.setattr(cli, "fan_json", lambda *args, **kwargs: ([], {}))

    result = CliRunner().invoke(cli.app, ["doctor", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert [row["center"] for row in payload["nodes"]] == ["east", "west"]
    assert all(row["checks"]["ssh"] == "ok" for row in payload["nodes"])


def test_laptop_free_all_heads_unreachable_returns_exit_5_rows(
    monkeypatch,
):
    import json
    import subprocess

    import dt.remote as remote_mod
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"east": "head-a", "west": "head-b"},
        default_center="east",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        remote_mod,
        "remote_dt",
        lambda head, argv, timeout: subprocess.CompletedProcess(
            argv,
            255,
            "",
            f"ssh: connect to {head}: No route to host",
        ),
    )

    result = CliRunner().invoke(cli.app, ["free", "--json"])

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    assert json.loads(result.stdout) == [
        {
            "center": "east",
            "node": "head-a",
            "gpus": [],
            "system": None,
            "error": "ssh: connect to head-a: No route to host",
            "unreachable": True,
        },
        {
            "center": "west",
            "node": "head-b",
            "gpus": [],
            "system": None,
            "error": "ssh: connect to head-b: No route to host",
            "unreachable": True,
        },
    ]


def test_laptop_free_partial_center_outage_keeps_results_and_exit_0(
    monkeypatch,
):
    import json
    import subprocess

    import dt.remote as remote_mod
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"healthy": "head-a", "offline": "head-b"},
        default_center="healthy",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def probe(head, argv, timeout):
        if head == "head-a":
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps(
                    [
                        {
                            "center": "healthy",
                            "node": "gpu-node",
                            "gpus": [],
                            "system": None,
                            "error": None,
                            "unreachable": False,
                        }
                    ]
                ),
                "",
            )
        return subprocess.CompletedProcess(
            argv,
            255,
            "",
            "ssh: connect to head-b: Connection timed out",
        )

    monkeypatch.setattr(remote_mod, "remote_dt", probe)

    result = CliRunner().invoke(cli.app, ["free", "--json"])

    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    assert rows[0]["node"] == "gpu-node"
    assert rows[1] == {
        "center": "offline",
        "node": "head-b",
        "gpus": [],
        "system": None,
        "error": "ssh: connect to head-b: Connection timed out",
        "unreachable": True,
    }


def test_laptop_free_all_protocol_failures_return_exit_1(monkeypatch):
    import json
    import subprocess

    import dt.remote as remote_mod
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        remote_mod,
        "remote_dt",
        lambda head, argv, timeout: subprocess.CompletedProcess(
            argv, 0, "not-json\n", ""
        ),
    )

    result = CliRunner().invoke(cli.app, ["free", "--json"])

    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout) == [
        {
            "center": "test",
            "node": "head",
            "gpus": [],
            "system": None,
            "error": "bad json from head (dt installed there?)",
            "unreachable": False,
        }
    ]


def test_head_free_all_nodes_unreachable_returns_exit_5(
    tmp_path,
    monkeypatch,
):
    import json

    from typer.testing import CliRunner

    from dt import cli
    from dt.config import HeadConfig, Node
    from dt.probe import NodeStatus

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1"), Node(name="n2")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "probe_center",
        lambda *args, **kwargs: [
            NodeStatus(
                node="n1",
                error="ssh: No route to host",
                unreachable=True,
            ),
            NodeStatus(
                node="n2",
                error="ssh: Connection timed out",
                unreachable=True,
            ),
        ],
    )

    result = CliRunner().invoke(cli.app, ["free", "--json"])

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    rows = json.loads(result.stdout)
    assert [row["node"] for row in rows] == ["n1", "n2"]
    assert all(row["unreachable"] is True for row in rows)


def test_head_free_fresh_bypasses_probe_cache(tmp_path, monkeypatch):
    import json

    from typer.testing import CliRunner

    from dt import cli
    from dt.config import HeadConfig, Node
    from dt.probe import NodeStatus

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    cache_flags = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "probe_center",
        lambda cfg_, use_cache=True, **_kwargs: (
            cache_flags.append(use_cache) or [NodeStatus(node="n1")]
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        ["free", "--fresh", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert cache_flags == [False]
    assert json.loads(result.stdout)[0]["node"] == "n1"


@pytest.mark.parametrize("poll", ["0", "nan", "inf", "-inf"])
def test_free_rejects_invalid_poll_before_loading_config(monkeypatch, poll):
    import json

    from typer.testing import CliRunner

    from dt import cli

    monkeypatch.setattr(
        cli,
        "_cfg",
        lambda: (_ for _ in ()).throw(AssertionError("must not load config")),
    )

    human = CliRunner().invoke(cli.app, ["free", "--watch", "--poll", poll])
    machine = CliRunner().invoke(
        cli.app,
        ["free", "--watch", "--poll", poll, "--json"],
    )

    assert human.exit_code == 1
    assert "--poll must be positive" in human.output
    assert machine.exit_code == 1
    assert json.loads(machine.stdout) == {
        "error": "invalid_argument",
        "message": "--poll must be positive",
        "reasons": {},
        "exit_code": 1,
    }


def test_doctor_table_keeps_node_names_readable_at_80_columns():
    from rich.console import Console

    from dt.render import doctor_table

    rows = [
        {
            "center": "psibot",
            "node": node,
            "checks": {
                "ssh": (
                    "ssh: connect to host 172.16.6.78 port 22: No route to host"
                    if node == "psibot-ds"
                    else "ok"
                ),
                "gpu": "-" if node == "psibot-ds" else "580.159.06",
                "uv": "-" if node == "psibot-ds" else "ok",
                "tmux": "-" if node == "psibot-ds" else "ok",
                "rsync": "-" if node == "psibot-ds" else "ok",
                "flock": "-" if node == "psibot-ds" else "ok",
                "net": "-" if node == "psibot-ds" else "slow(2MB/s)",
                "agent": "ok" if node == "psibot-hm" else "-",
                "dt": "-",
            },
        }
        for node in ("psibot-hm", "psibot-ds", "psibot-ys")
    ]
    console = Console(width=80, record=True, color_system=None)
    console.print(doctor_table(rows))
    rendered = console.export_text()

    assert "psibot-hm" in rendered
    assert "psibot-ds" in rendered
    assert "psibot-ys" in rendered
    assert "tools" in rendered
    assert "driver" in rendered
    assert "580.159.06" in rendered
    assert "all ok" in rendered
    assert "no route" in rendered


def test_doctor_probe_preserves_complete_ssh_error_for_json(monkeypatch):
    import subprocess

    from dt import doctor
    from dt.config import Node

    detail = "ssh: connect to host 172.16.6.78 port 22: No route to host"
    monkeypatch.setattr(
        doctor,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            255,
            "",
            detail,
        ),
    )

    row = doctor.check_node(Node(name="psibot-ds"))

    # The full failure vocabulary must survive into JSON (nothing truncated
    # away), while the remote endpoint identity is redacted (A31-3).
    assert (
        row["checks"]["ssh"] == "ssh: connect to host <addr> port 22: No route to host"
    )


def test_doctor_probe_reports_contract_runtime_dependencies(monkeypatch):
    import subprocess

    from dt import doctor
    from dt.config import Node

    assert "command -v python3" in doctor.CHECK_SNIPPET
    assert "DT_PYTHON3=ok" in doctor.CHECK_SNIPPET
    assert "command -v timeout" in doctor.CHECK_SNIPPET
    assert "DT_TIMEOUT=ok" in doctor.CHECK_SNIPPET

    monkeypatch.setattr(
        doctor,
        "run_on",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [],
            0,
            "\n".join(
                [
                    "DT_SSH=ok",
                    "DT_GPU=570.1",
                    "DT_UV=ok",
                    "DT_TMUX=ok",
                    "DT_RSYNC=ok",
                    "DT_FLOCK=ok",
                    "DT_PYTHON3=ok",
                    "DT_TIMEOUT=ok",
                    "DT_NET=mirror",
                ]
            ),
            "",
        ),
    )

    checks = doctor.check_node(Node(name="n1"))["checks"]

    assert checks["python3"] == "ok"
    assert checks["timeout"] == "ok"


def test_doctor_overlaps_network_and_runtime_checks(tmp_path):
    import os
    import subprocess

    from dt import doctor

    gpu_started = tmp_path / "gpu-started"
    net_started = tmp_path / "net-started"
    fake_commands = (
        "nvidia-smi() {\n"
        ': > "$DT_TEST_GPU_STARTED"\n'
        "i=0\n"
        'while [ ! -e "$DT_TEST_NET_STARTED" ]; do\n'
        "  i=$((i + 1))\n"
        '  [ "$i" -ge 100 ] && return 9\n'
        "  sleep 0.01\n"
        "done\n"
        'echo "570.1"\n'
        "}\n"
        "curl() {\n"
        ': > "$DT_TEST_NET_STARTED"\n'
        "i=0\n"
        'while [ ! -e "$DT_TEST_GPU_STARTED" ]; do\n'
        "  i=$((i + 1))\n"
        '  [ "$i" -ge 100 ] && return 9\n'
        "  sleep 0.01\n"
        "done\n"
        "return 1\n"
        "}\n"
    )

    proc = subprocess.run(
        ["bash", "-c", f"{fake_commands}\n{doctor.CHECK_SNIPPET}"],
        capture_output=True,
        text=True,
        timeout=3,
        env={
            **os.environ,
            "DT_TEST_GPU_STARTED": str(gpu_started),
            "DT_TEST_NET_STARTED": str(net_started),
        },
    )

    assert proc.returncode == 0, proc.stderr
    assert "DT_GPU=570.1" in proc.stdout
    assert "DT_NET=blocked" in proc.stdout


def test_doctor_json_keeps_health_failure_exit_code(tmp_path, monkeypatch):
    import json

    from typer.testing import CliRunner

    from dt import cli
    from dt.config import HeadConfig, Node

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    rows = [
        {
            "center": "c",
            "node": "n1",
            "checks": {"ssh": "ssh: No route to host"},
        }
    ]
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "doctor_center", lambda cfg_: rows)

    result = CliRunner().invoke(cli.app, ["doctor", "--json"])

    assert result.exit_code == cli.EXIT_UNREACHABLE
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == "dt_doctor_v2"
    assert payload["summary"]["exit_code"] == cli.EXIT_UNREACHABLE
    assert payload["nodes"] == rows
    assert payload["issues"][0]["kind"] == "unreachable"


def test_fan_json_can_preserve_valid_health_rows_on_nonzero(monkeypatch):
    import json
    import subprocess

    import dt.remote as remote_mod
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    rows = [
        {
            "center": "test",
            "node": "offline-node",
            "checks": {"ssh": "ssh: No route to host"},
        }
    ]
    monkeypatch.setattr(
        remote_mod,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 5, json.dumps(rows), ""
        ),
    )

    actual, errors = remote_mod.fan_json(
        cfg,
        ["doctor"],
        accept_nonzero_json=True,
    )

    assert actual == rows
    assert errors == {}


def test_fan_json_bounds_shareable_head_error(monkeypatch):
    import subprocess

    import dt.remote as remote_mod
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    detail = (
        "ssh: connect to host head port 22: No route to host; "
        + "transport-detail-" * 8
    )
    monkeypatch.setattr(
        remote_mod,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 255, "", detail),
    )

    rows, errors = remote_mod.fan_json(cfg, ["free"])

    assert rows == []
    assert errors == {"test": detail[:160]}


def test_laptop_doctor_preserves_remote_health_rows_and_exit_5(
    monkeypatch,
):
    import json
    import subprocess

    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    node_rows = [
        {
            "center": "test",
            "node": "offline-node",
            "checks": {"ssh": "ssh: No route to host"},
        }
    ]
    fan_calls = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "remote_dt",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "dt 1.2.3\n", ""),
    )

    def fan(cfg_, argv, timeout, **kwargs):
        fan_calls.append((cfg_, argv, timeout, kwargs))
        return node_rows, {}

    monkeypatch.setattr(cli, "fan_json", fan)

    result = CliRunner().invoke(cli.app, ["doctor", "--json"])

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    payload = json.loads(result.stdout)
    assert payload["nodes"][0]["checks"] == {"ssh": "ok", "dt": "1.2.3"}
    assert payload["nodes"][1:] == node_rows
    assert payload["issues"][0]["kind"] == "unreachable"
    assert len(fan_calls) == 1
    called_cfg, argv, timeout, kwargs = fan_calls[0]
    assert called_cfg is cfg
    assert argv == ["doctor", "--rows-json"]
    assert timeout == 120
    assert kwargs["accept_nonzero_json"] is True
    assert kwargs["unreachable_errors"] == set()


def test_doctor_treats_missing_gpu_as_health_failure(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import HeadConfig, Node

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    rows = [
        {
            "center": "c",
            "node": "n1",
            "checks": {
                "ssh": "ok",
                "gpu": "missing",
                "uv": "ok",
                "tmux": "ok",
                "rsync": "ok",
                "flock": "ok",
            },
        }
    ]
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "doctor_center", lambda cfg_: rows)

    result = CliRunner().invoke(cli.app, ["doctor", "--json"])

    assert result.exit_code == 1


def test_doctor_treats_missing_contract_runtime_as_health_failure(
    tmp_path,
    monkeypatch,
):
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import HeadConfig, Node

    cfg = HeadConfig(
        center="c",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    rows = [
        {
            "center": "c",
            "node": "n1",
            "checks": {
                "ssh": "ok",
                "gpu": "570.1",
                "uv": "ok",
                "tmux": "ok",
                "rsync": "ok",
                "flock": "ok",
                "python3": "missing",
                "timeout": "ok",
            },
        }
    ]
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "doctor_center", lambda cfg_: rows)

    result = CliRunner().invoke(cli.app, ["doctor", "--json"])

    assert result.exit_code == 1


def test_doctor_table_surfaces_missing_contract_runtime_at_80_columns():
    from rich.console import Console

    from dt.render import doctor_table

    row = {
        "center": "c",
        "node": "n1",
        "checks": {
            "ssh": "ok",
            "gpu": "570.1",
            "uv": "ok",
            "tmux": "ok",
            "rsync": "ok",
            "flock": "ok",
            "python3": "missing",
            "timeout": "ok",
            "net": "mirror",
        },
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(doctor_table([row]))
    rendered = console.export_text()

    assert "py:missing" in rendered
    assert "all ok" not in rendered
    assert max(map(len, rendered.splitlines())) <= 80


def test_doctor_human_suggests_seed_for_remote_slow_network(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import HeadConfig, Node

    cfg = HeadConfig(
        center="c",
        nodes=[
            Node(name="slow-node-with-a-descriptive-name"),
            Node(name="another-slow-node-with-a-long-name"),
            Node(name="fast-node"),
        ],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    healthy = {
        "ssh": "ok",
        "gpu": "570.1",
        "uv": "ok",
        "tmux": "ok",
        "rsync": "ok",
        "flock": "ok",
    }
    rows = [
        {
            "center": "c",
            "node": "slow-node-with-a-descriptive-name",
            "checks": {**healthy, "net": "slow(40KB/s)"},
        },
        {
            "center": "c",
            "node": "another-slow-node-with-a-long-name",
            "checks": {**healthy, "net": "blocked"},
        },
        {
            "center": "c",
            "node": "fast-node",
            "checks": {**healthy, "net": "ok(4MB/s)"},
        },
    ]
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(cli, "doctor_center", lambda cfg_: rows)

    result = CliRunner().invoke(cli.app, ["doctor"], env={"COLUMNS": "80"})

    # The network hints remain visible, but a pure head with no resident
    # scheduler is now a doctor failure instead of an invisible idle state.
    assert result.exit_code == 1, result.output
    assert "dt seed slow-node-with-a-descriptive-name" in result.output
    assert "another-slow-node-with-a-long-name --plan" in result.output
    assert "dt seed fast-node" not in result.output
    assert max(map(len, result.output.splitlines())) <= 80


def test_ps_table_defaults_to_one_compact_row_per_job_at_80_columns(monkeypatch):
    from datetime import datetime

    from rich.console import Console

    import dt.render as render

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 7, 24, 12, 0)
            return value if tz is None else value.astimezone(tz)

    monkeypatch.setattr(render, "datetime", FrozenDatetime)

    rows = [
        {
            "name": "dt-dp-microbatch64-stability10k",
            "job_id": "20260724-0510_dt-dp-microbatch64-stability10k_baea",
            "display_ref": "lity_baea",
            "center": "psibot",
            "node": "psibot-ds",
            "gpus": [0],
            "status": "finished",
            "exit_code": 0,
            "created_at": FrozenDatetime(2026, 7, 24, 5, 10).timestamp(),
            "cmd": "python -c 'VERY_LONG_COMMAND_SHOULD_NOT_BE_IN_COMPACT_VIEW'",
        },
        {
            "name": "short-canary",
            "job_id": "20260724-0546_short-canary_f787",
            "center": "psibot",
            "node": "psibot-ds",
            "gpus": [],
            "status": "running",
            "exit_code": None,
            "created_at": FrozenDatetime(2026, 7, 24, 5, 46).timestamp(),
            "cmd": "sleep 30",
        },
    ]
    console = Console(width=80, record=True, color_system=None)
    console.print(render.ps_table(rows))
    rendered = console.export_text()

    assert "dt-dp-microbatch64-stability10k" in rendered
    assert "short-canary" in rendered
    assert "psibot-ds" in rendered
    assert "GPU" in rendered
    assert "state" in rendered
    assert "ref" in rendered
    assert "lity_baea" in rendered
    assert "f787" in rendered
    assert "when" in rendered
    assert "05:10" in rendered
    assert "VERY_LONG_COMMAND" not in rendered
    assert "job id" not in rendered
    assert len([line for line in rendered.splitlines() if "dt-dp-" in line]) == 1
    assert len([line for line in rendered.splitlines() if "short-canary" in line]) == 1


def test_ps_table_preserves_complete_historical_date_at_80_columns(monkeypatch):
    from datetime import datetime

    from rich.console import Console

    import dt.render as render

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 8, 1, 12, 0)
            return value if tz is None else value.astimezone(tz)

    monkeypatch.setattr(render, "datetime", FrozenDatetime)
    row = {
        "name": "general-lossless-accel-gla06-local-a",
        "job_id": "20260731-1311_general-lossless-accel-gla06-local-a_abcd",
        "display_ref": "abcd",
        "center": "psibot",
        "node": "psibot-hm",
        "pin_node": "psibot-hm",
        "gpus": [],
        "gpus_requested": 1,
        "status": "queued",
        "queue_position": 10,
        "queue_depth": 10,
        "exit_code": None,
        "created_at": FrozenDatetime(2026, 7, 31, 13, 11).timestamp(),
        "cmd": "python train.py",
    }
    console = Console(width=80, record=True, color_system=None)

    console.print(render.ps_table([row]))
    rendered = console.export_text()

    assert max(map(len, rendered.splitlines())) <= 80
    assert "07-31" in rendered
    assert "07-3 " not in rendered


def test_ps_issues_empty_state_does_not_render_an_empty_table(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import HeadConfig

    cfg = HeadConfig(
        center="c",
        nodes=[],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        ps_cmd,
        "_gather_ps_rows",
        lambda *args, **kwargs: ([], {}),
    )

    result = CliRunner().invoke(cli.app, ["ps", "--issues"])

    assert result.exit_code == 0, result.output
    assert "No jobs need attention." in result.output
    assert "Recent issues" not in result.output
    assert "name" not in result.output
    assert "0 need attention" not in result.output


def test_ps_human_issues_compact_dependency_ids_without_mutating_machine_rows():

    predecessor_id = "20260730-0047_long-predecessor-name_1234567890abcdef"
    rows = ps_cmd._PsRows(
        [
            {
                "job_id": predecessor_id,
                "display_ref": "cdef",
                "reason": "setup failed",
            },
            {
                "job_id": "20260730-0100_dependent_fedcba0987654321",
                "display_ref": "4321",
                "reason": f"dependency {predecessor_id} did not succeed: failed",
            },
        ],
        total=2,
        applied_filters={"issues"},
    )

    human_rows = ps_cmd._humanize_ps_references(rows)

    assert predecessor_id in rows[1]["reason"]
    assert human_rows[1]["reason"] == "dependency cdef failed"
    assert human_rows.total == 2
    assert human_rows.applied_filters == frozenset({"issues"})


def test_ps_table_wide_retains_full_identity_and_command_at_80_columns():
    from rich.console import Console

    from dt.render import ps_table

    row = {
        "name": "exp",
        "job_id": (
            "20260724-0510_exp-with-a-descriptive-name_0123456789abcdef0123456789abcdef"
        ),
        "center": "psibot",
        "node": "psibot-ds",
        "gpus": [0],
        "status": "finished",
        "exit_code": 0,
        "created_at": 1784841026.0,
        "cmd": (
            "python train.py --configuration configs/long/research/baseline.yaml "
            "--learning-rate 3e-4 --sentinel COMPLETE"
        ),
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(ps_table([row], wide=True))
    rendered = console.export_text()

    compact = "".join(rendered.split())
    assert row["job_id"] in compact
    assert "--sentinelCOMPLETE" in compact
    assert "..." not in rendered
    assert "…" not in rendered


def test_ps_table_keeps_state_reference_target_and_time_at_60_columns(monkeypatch):
    from datetime import datetime

    from rich.console import Console

    import dt.render as render

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 8, 1, 12, 0)
            return value if tz is None else value.astimezone(tz)

    monkeypatch.setattr(render, "datetime", FrozenDatetime)
    row = {
        "name": "general-lossless-acceleration-experiment",
        "job_id": "20260731-1311_general-lossless-acceleration-experiment_abcd",
        "display_ref": "abcd",
        "center": "psibot",
        "node": "-",
        "pin_node": "psibot-hm",
        "gpus": [],
        "gpus_requested": 1,
        "status": "queued",
        "queue_position": 10,
        "queue_depth": 10,
        "created_at": FrozenDatetime(2026, 7, 31, 13, 11).timestamp(),
        "cmd": "python train.py",
    }
    console = Console(width=60, record=True, color_system=None)

    console.print(render.ps_table([row]))
    rendered = console.export_text()

    assert "abcd" in rendered
    assert "psibot-hm" in rendered
    assert "queued #10/10" in rendered
    assert "07-31" in rendered
    assert max(map(len, rendered.splitlines())) <= 60


def test_ps_table_treats_job_names_and_commands_as_literal_text():
    from rich.console import Console

    from dt.render import ps_table

    row = {
        "name": "[red]not-a-status[/red]",
        "job_id": "20260724-0510_exp_baea",
        "center": "psibot",
        "node": "psibot-ds",
        "gpus": [0],
        "status": "running",
        "created_at": 1784841026.0,
        "cmd": "python -c '[link=file:///tmp/fake]text[/link]'",
    }
    console = Console(width=180, record=True, color_system=None)

    console.print(ps_table([row], wide=True))
    rendered = console.export_text()

    assert row["name"] in rendered
    assert row["cmd"] in rendered


def test_ps_table_keeps_center_scoped_ref_usable_at_80_columns():
    from rich.console import Console

    from dt.render import ps_table

    row = {
        "name": "long-experiment-name-that-may-ellipsize",
        "job_id": "20260728-1200_long-experiment_abcd",
        "display_ref": "research-west:abcd",
        "center": "research-west",
        "node": "gpu-node-12",
        "gpus": [3],
        "status": "failed",
        "exit_code": None,
        "created_at": 100.0,
        "cmd": "python train.py",
        "reason": "failed-before-start",
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(ps_table([row], show_issue=True))
    rendered = console.export_text()

    assert "research-west:abcd" in rendered
    assert "gpu-node-12" in rendered
    assert "failed" in rendered
    assert max(map(len, rendered.splitlines())) <= 80


def test_ps_compact_prioritizes_node_gpu_and_issue_at_80_columns():
    from rich.console import Console

    from dt.render import ps_table

    rows = [
        {
            "name": "dt-dp-microbatch64-stability10k",
            "job_id": "done",
            "center": "psibot",
            "node": "psibot-ds",
            "gpus": [0],
            "status": "finished",
            "exit_code": 0,
            "created_at": 1784841026.0,
            "cmd": "python train.py",
        },
        {
            "name": "dt-dp-internal-gpu-metrics-proof",
            "job_id": "live",
            "center": "psibot",
            "node": "psibot-ds",
            "gpus": [0],
            "status": "running",
            "exit_code": None,
            "created_at": 1784844986.0,
            "cmd": "python train.py",
            "node_unreachable": True,
            "max_hours_exceeded": True,
        },
        {
            "name": "pinned-offline-reason-live-proof",
            "job_id": "queued",
            "center": "psibot",
            "node": "-",
            "pin_node": "psibot-hm",
            "gpus": [],
            "gpus_requested": 2,
            "status": "queued",
            "exit_code": None,
            "created_at": 1784850310.0,
            "cmd": "python train.py",
            "reason": "waiting: psibot-hm unreachable: ssh timeout",
        },
    ]
    console = Console(width=80, record=True, color_system=None)
    console.print(ps_table(rows))
    rendered = console.export_text()

    assert "psibot-ds" in rendered
    assert "psibot-hm" in rendered
    assert "GPU" in rendered
    assert "running? offline >max" in rendered
    assert "queued offline" in rendered


def test_ps_table_marks_unreachable_overdue_registry_state():
    from rich.console import Console

    from dt.render import ps_table

    row = {
        "name": "offline-train",
        "job_id": "j",
        "center": "psibot",
        "node": "psibot-ds",
        "gpus": [0],
        "status": "running",
        "exit_code": None,
        "created_at": 100.0,
        "cmd": "python train.py",
        "node_unreachable": True,
        "max_hours_exceeded": True,
    }
    console = Console(width=100, record=True, color_system=None)
    console.print(ps_table([row]))
    rendered = console.export_text()

    assert "running? offline >max" in rendered


def test_ps_issue_view_explains_legacy_lost_records_without_reason():
    from rich.console import Console

    from dt.render import ps_table

    base = {
        "center": "psibot",
        "node": "psibot-ys",
        "gpus": [0],
        "status": "lost",
        "exit_code": None,
        "created_at": 100.0,
        "cmd": "python train.py",
    }
    rows = [
        {
            **base,
            "name": "legacy-no-reason",
            "job_id": "legacy-no-reason",
            "reason": None,
        },
        {
            **base,
            "name": "legacy-backfilled",
            "job_id": "legacy-backfilled",
            "reason": (
                "wrapper pid 4321 is not running and "
                "dt/jobs/legacy/exit_code is missing"
            ),
        },
    ]
    console = Console(width=120, record=True, color_system=None)
    console.print(ps_table(rows, show_issue=True))
    rendered = console.export_text()

    assert rendered.count("exit marker missing") == 2


def test_ps_issue_view_points_nonzero_finished_jobs_to_logs():
    from rich.console import Console

    from dt.render import ps_table

    row = {
        "name": "train-failed",
        "job_id": "20260724-1220_train-failed_9cce",
        "center": "psibot",
        "node": "psibot-hm",
        "gpus": [],
        "status": "finished",
        "exit_code": 7,
        "reason": None,
        "created_at": 100.0,
        "cmd": "python train.py",
    }
    console = Console(width=100, record=True, color_system=None)
    console.print(ps_table([row], show_issue=True))
    rendered = console.export_text()

    assert "finished/7" in rendered
    assert "dt logs" in rendered


def test_ps_table_marks_unverified_dispatch_cancellation():
    from rich.console import Console

    from dt.jobs import CANCEL_UNVERIFIED_PREFIX
    from dt.render import ps_table

    row = {
        "name": "cancel-warning",
        "job_id": "cancel-warning",
        "center": "psibot",
        "node": "psibot-hm",
        "gpus": [0],
        "status": "running",
        "exit_code": None,
        "created_at": 100.0,
        "cmd": "python train.py",
        "reason": f"{CANCEL_UNVERIFIED_PREFIX}connection closed",
    }
    console = Console(width=100, record=True, color_system=None)
    console.print(ps_table([row]))
    rendered = console.export_text()

    assert "running cancel!" in rendered


def test_ps_table_marks_queued_reason_and_uses_pinned_target():
    from rich.console import Console

    from dt.render import ps_table

    rows = [
        {
            "name": "offline-task",
            "job_id": "q1",
            "center": "psibot",
            "node": "-",
            "pin_node": "psibot-ds",
            "gpus": [],
            "gpus_requested": 1,
            "status": "queued",
            "exit_code": None,
            "created_at": 100.0,
            "cmd": "python train.py",
            "reason": ("waiting: psibot-ds unreachable: ssh: No route to host"),
        },
        {
            "name": "blocked-task",
            "job_id": "q2",
            "center": "psibot",
            "node": "-",
            "pin_node": "psibot-ds",
            "gpus": [],
            "gpus_requested": 1,
            "status": "queued",
            "exit_code": None,
            "created_at": 101.0,
            "cmd": "python train.py",
            "reason": "blocked: psibot-ds: path-missing: /data/libero",
        },
    ]
    console = Console(width=120, record=True, color_system=None)
    console.print(ps_table(rows))
    rendered = console.export_text()

    assert "psibot-ds" in rendered
    assert "queued offline" in rendered
    assert "queued blocked" in rendered


def test_ps_selection_defaults_to_active_and_recent_is_bounded():

    from dt.cli.commands.ps import _select_ps_rows

    rows = [
        {
            "job_id": f"done-{index}",
            "status": "finished",
            "created_at": float(index + 2),
        }
        for index in range(35)
    ]
    old_running = {
        "job_id": "old-running",
        "status": "running",
        "created_at": 1.0,
    }
    active = _select_ps_rows([old_running, *rows], all_=False, recent=False)
    recent = _select_ps_rows([old_running, *rows], all_=False, recent=True)

    assert active == [old_running]
    assert len(recent) == 11
    assert old_running in recent
    assert rows[-1] in recent
    assert rows[-10] in recent
    assert rows[-11] not in recent


def test_ps_table_has_explicit_empty_state():
    from rich.console import Console

    from dt.render import ps_table

    console = Console(width=80, record=True, color_system=None)
    console.print(ps_table([]))

    assert "no matching jobs" in console.export_text()


def test_ps_watch_table_trades_created_column_for_live_progress():
    from rich.console import Console

    from dt.render import ps_table

    row = {
        "name": "live-train",
        "job_id": "j",
        "center": "psibot",
        "node": "psibot-ds",
        "gpus": [0],
        "status": "running",
        "exit_code": None,
        "created_at": 100.0,
        "cmd": "python train.py",
        "progress": {
            "step": 5,
            "total_steps": 10,
            "percent": 26.6667,
            "eta": "5s",
        },
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(ps_table([row], show_progress=True))
    rendered = console.export_text()

    assert "progress" in rendered
    assert "5/10" in rendered
    assert "27%" in rendered
    assert "created" not in rendered
    assert len(rendered.splitlines()) == 2


def test_ps_watch_table_shows_live_gpu_util_memory_and_temperature():
    from rich.console import Console

    from dt.render import ps_table

    row = {
        "name": "live-train",
        "job_id": "j",
        "center": "psibot",
        "node": "psibot-ds",
        "gpus": [0],
        "status": "running",
        "exit_code": None,
        "created_at": 100.0,
        "cmd": "python train.py",
        "progress": {"step": 5, "percent": 50.0},
        "resources": {
            "gpus": [
                {
                    "index": 0,
                    "util": 96,
                    "mem_used": 20480,
                    "mem_total": 24576,
                    "temperature": 69,
                }
            ],
            "system": None,
        },
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(ps_table([row], show_progress=True))
    rendered = console.export_text()

    assert "live" in rendered
    assert "0:96%/20G/69°" in rendered
    assert "5 50%" in rendered
    assert len(rendered.splitlines()) == 2


def test_ps_watch_labels_reserved_pre_cuda_gpu_as_initializing():
    from rich.console import Console

    from dt.render import ps_table

    row = {
        "name": "initializing-train",
        "job_id": "j",
        "center": "psibot",
        "node": "psibot-ds",
        "gpus": [0],
        "status": "running",
        "exit_code": None,
        "created_at": 100.0,
        "cmd": "python train.py",
        "resources": {
            "gpus": [
                {
                    "index": 0,
                    "util": 0,
                    "mem_used": 15,
                    "mem_total": 24564,
                    "temperature": 42,
                    "leased": True,
                    "procs": 0,
                }
            ],
            "system": None,
        },
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(ps_table([row], show_progress=True))
    rendered = console.export_text()

    assert "0:init/0.0G/42°" in rendered


def test_ps_watch_labels_reserved_gpu_context_as_pulse_workload():
    from rich.console import Console

    from dt.render import ps_table

    row = {
        "name": "simulator-pulse",
        "job_id": "j",
        "center": "psibot",
        "node": "psibot-ds",
        "gpus": [0],
        "status": "running",
        "exit_code": None,
        "created_at": 100.0,
        "cmd": "python collect.py",
        "resources": {
            "gpus": [
                {
                    "index": 0,
                    "util": 0,
                    "mem_used": 1536,
                    "mem_total": 24564,
                    "temperature": 49,
                    "leased": True,
                    "procs": 0,
                }
            ],
            "system": None,
        },
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(ps_table([row], show_progress=True))
    rendered = console.export_text()

    assert "0:pulse/1.5G/49°" in rendered


def test_ps_watch_labels_known_target_before_first_step():
    from rich.console import Console

    from dt.render import ps_table

    row = {
        "name": "cold-compile",
        "job_id": "j",
        "center": "psibot",
        "node": "psibot-ds",
        "gpus": [0],
        "status": "running",
        "exit_code": None,
        "created_at": 100.0,
        "cmd": "python train.py",
        "progress": {"total_steps": 15000},
        "resources": {
            "gpus": [
                {
                    "index": 0,
                    "util": 0,
                    "mem_used": 18971,
                    "mem_total": 24564,
                    "temperature": 52,
                    "leased": True,
                    "procs": 1,
                }
            ],
            "system": None,
        },
    }
    console = Console(width=80, record=True, color_system=None)
    console.print(ps_table([row], show_progress=True))
    rendered = console.export_text()

    assert "0:0%/19G/52°" in rendered
    assert "pre-step" in rendered
    assert "/15,000" in rendered
    assert len(rendered.splitlines()) == 2


def test_ps_watch_table_preserves_cpu_host_resources_with_long_status_row():
    from rich.console import Console

    from dt.render import ps_table

    rows = [
        {
            "name": "offline-training",
            "job_id": "offline",
            "center": "psibot",
            "node": "psibot-ds",
            "gpus": [0],
            "status": "running",
            "exit_code": None,
            "created_at": 99.0,
            "cmd": "python train.py",
            "node_unreachable": True,
            "max_hours_exceeded": True,
            "resources": {"error": "ssh: No route to host"},
            "status_probe_error": "ssh: No route to host",
        },
        {
            "name": "cpu-preprocess",
            "job_id": "j",
            "center": "psibot",
            "node": "psibot-hm",
            "gpus": [],
            "gpus_requested": 0,
            "status": "running",
            "exit_code": None,
            "created_at": 100.0,
            "cmd": "python preprocess.py",
            "progress": {"step": 5, "percent": 50.0},
            "resources": {
                "gpus": [],
                "system": {
                    "cpu_load1": 1.5,
                    "mem_used_mib": 8192,
                    "io_pressure": 0.28,
                },
            },
        },
    ]
    console = Console(width=80, record=True, color_system=None)
    console.print(ps_table(rows, show_progress=True))
    rendered = console.export_text()

    assert "live" in rendered
    assert "C1.5/R8.0G/I0.3%" in rendered
    assert "running? offline >max" in rendered
    assert "5 50%" in rendered
    assert len(rendered.splitlines()) == 3


def test_ps_watch_preserves_offline_status_and_compacts_root_cause():
    from rich.console import Console

    from dt.render import ps_table

    rows = [
        {
            "name": "dt-dp-internal-gpu-metrics-proof",
            "job_id": "live",
            "center": "psibot",
            "node": "psibot-ds",
            "gpus": [0],
            "status": "running",
            "exit_code": None,
            "created_at": 1784844986.0,
            "cmd": "python train.py",
            "node_unreachable": True,
            "max_hours_exceeded": True,
            "resources": {
                "error": ("ssh: connect to host 172.16.6.78 port 22: No route to host")
            },
            "status_probe_error": (
                "ssh: connect to host 172.16.6.78 port 22: No route to host"
            ),
        },
        {
            "name": "old-dequeued-canary",
            "job_id": "killed",
            "center": "psibot",
            "node": "-",
            "pin_node": "psibot-ds",
            "gpus": [],
            "status": "killed",
            "exit_code": None,
            "created_at": 1784850310.0,
            "cmd": "true",
            "reason": "waiting: psibot-ds unreachable: No route to host",
        },
    ]
    console = Console(width=80, record=True, color_system=None)
    console.print(ps_table(rows, show_progress=True))
    rendered = console.export_text()

    assert "psibot-ds" in rendered
    assert "running? offline >max" in rendered
    assert "no route" in rendered
    assert "offline: n" not in rendered
    assert "ssh: connect to ho" not in rendered
    assert "waiting: psibot-ds" not in rendered


# -- snapshot size warning -------------------------------------------------------


def test_snapshot_excludes_are_root_anchored():
    """Artifact dirs must only be excluded at the project root: a package
    subdir like omnistack/data/ has to survive the snapshot."""
    from dt.dispatch import SNAPSHOT_EXCLUDES

    for name in ("data", "checkpoints", "outputs", "wandb"):
        assert f"/{name}/" in SNAPSHOT_EXCLUDES
        assert f"{name}/" not in SNAPSHOT_EXCLUDES  # unanchored form is the bug
    for junk in (
        ".venv/",
        "__pycache__/",
        ".git/",
        "*.pyc",
        ".coverage",
        ".coverage.*",
        "coverage.xml",
        "htmlcov/",
        ".mypy_cache/",
        ".ruff_cache/",
        ".hypothesis/",
    ):
        assert junk in SNAPSHOT_EXCLUDES


def test_ssh_base_has_multiplexing(tmp_path, monkeypatch):
    from dt.sshio import SSHWorkload, ssh_pool_config

    monkeypatch.setenv("DT_SSH_STATE_DIR", str(tmp_path / "ssh"))
    monkeypatch.setenv("DT_SSH_CONFIG", str(tmp_path / "absent"))
    joined = ssh_pool_config(SSHWorkload.CONTROL).read_text()
    assert "ControlMaster auto" in joined
    assert "ControlPersist 300" in joined
    assert "/control/%C" in joined


def test_pinned_submit_probes_only_the_pin(tmp_path, monkeypatch):
    """A --node submit must not fan out to the whole center."""
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node, QueueCfg
    from dt.probe import Gpu, NodeStatus

    cfg = HeadConfig(
        center="t",
        nodes=[Node(name="n1"), Node(name="n2"), Node(name="n3")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )
    probed: list[str] = []
    logs: list[str] = []

    def fake_probe_node(node, mem, timeout=10):
        probed.append(node.name)
        return NodeStatus(
            node=node.name,
            gpus=[
                Gpu(
                    index=0,
                    uuid="busy",
                    mem_used=1024,
                    mem_total=24 * 1024,
                    util=1,
                    procs=1,
                    free=False,
                )
            ],
        )

    def fake_probe_center(cfg_, use_cache=True):
        raise AssertionError("pinned submit must not probe the whole center")

    monkeypatch.setattr(dispatch, "probe_node", fake_probe_node)
    monkeypatch.setattr(dispatch, "probe_center", fake_probe_center)
    monkeypatch.setattr(
        dispatch,
        "resolve_project",
        lambda cfg_, req, cwd: ("p", Project(path=tmp_path)),
    )
    monkeypatch.setattr(dispatch, "git_info", lambda d: (None, False, None))
    # no free gpus -> goes to queue; _stage would rsync, stub it out
    monkeypatch.setattr(
        dispatch,
        "_stage",
        lambda _cfg, _project, _job_id, _spec, meta, *args, **kwargs: (
            meta.update(snapshot_sha256="a" * 64) or tmp_path
        ),
    )

    spec = dispatch.RunSpec(name="j", gpus=1, cmd=["true"], node="n2")
    entry = dispatch.submit(cfg, spec, tmp_path, logs.append)
    assert probed == ["n2"]
    assert entry.status == "queued"
    assert entry.reason == (
        "waiting: no free capacity "
        "(n2: 0 free < 1 wanted; busy: gpu0 ? 1.0/24.0GiB util1%)"
    )
    assert logs[-1] == (
        "no free capacity "
        "(n2: 0 free < 1 wanted; busy: gpu0 ? 1.0/24.0GiB util1%); "
        "queueing (agent retries automatically)"
    )


def test_pinned_no_capacity_explains_busy_gpu(tmp_path, monkeypatch):
    import pytest

    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node, QueueCfg
    from dt.probe import Gpu, NodeStatus

    cfg = HeadConfig(
        center="t",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )
    status = NodeStatus(
        node="n1",
        gpus=[
            Gpu(
                index=0,
                uuid="u",
                mem_used=3873,
                mem_total=32607,
                util=25,
                procs=1,
                free=False,
                users=["psibot"],
            )
        ],
    )
    monkeypatch.setattr(dispatch, "probe_node", lambda *args, **kwargs: status)
    monkeypatch.setattr(
        dispatch,
        "resolve_project",
        lambda cfg_, req, cwd: ("p", Project(path=tmp_path)),
    )
    monkeypatch.setattr(dispatch, "git_info", lambda directory: (None, False, None))

    with pytest.raises(dispatch.NoCapacity) as raised:
        dispatch.submit(
            cfg,
            dispatch.RunSpec(
                name="busy-detail",
                gpus=1,
                cmd=["true"],
                node="n1",
            ),
            tmp_path,
            lambda message: None,
            no_queue=True,
        )

    assert raised.value.reasons == {
        "n1": ("0 free < 1 wanted; busy: gpu0 psibot 3.8/31.8GiB util25%")
    }


def test_capacity_reason_explains_lease_and_unowned_vram():
    from dt.dispatch import capacity_reason
    from dt.probe import Gpu, NodeStatus

    status = NodeStatus(
        node="n1",
        gpus=[
            Gpu(
                index=0,
                uuid="free",
                mem_used=4,
                mem_total=24576,
                util=0,
                free=True,
            ),
            Gpu(
                index=1,
                uuid="lease",
                mem_used=12,
                mem_total=24576,
                util=0,
                leased=True,
                lease_owner="20260724-1220_train-policy_abcd",
                free=False,
            ),
            Gpu(
                index=2,
                uuid="memory",
                mem_used=900,
                mem_total=24576,
                util=0,
                free=False,
            ),
        ],
    )

    assert capacity_reason(status, 2) == (
        "1 free < 2 wanted; busy: "
        "gpu1 20260724-1220_train-policy_abcd 0.0/24.0GiB init, "
        "gpu2 VRAM-in-use 0.9/24.0GiB util0%"
    )


def test_capacity_reason_surfaces_incomplete_gpu_inventory():
    from dt.dispatch import capacity_reason
    from dt.probe import NodeStatus

    status = NodeStatus(
        node="n1",
        gpu_inventory_error=(
            "GPU inventory incomplete: 1 malformed row not schedulable"
        ),
    )

    assert capacity_reason(status, 1) == (
        "0 free < 1 wanted; inventory: 1 malformed row not schedulable"
    )


def test_capacity_reason_labels_retained_context_as_pulse_workload():
    from dt.dispatch import capacity_reason
    from dt.probe import Gpu, NodeStatus

    status = NodeStatus(
        node="n1",
        gpus=[
            Gpu(
                index=0,
                uuid="pulse",
                mem_used=1536,
                mem_total=24576,
                util=0,
                leased=True,
                lease_owner="20260727-0310_uo20_abcd",
                free=False,
            )
        ],
    )

    assert capacity_reason(status, 1) == (
        "0 free < 1 wanted; busy: gpu0 20260727-0310_uo20_abcd 1.5/24.0GiB pulse"
    )


def test_pinned_unreachable_no_queue_stops_after_probe(tmp_path, monkeypatch):
    import pytest

    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node, QueueCfg
    from dt.probe import NodeStatus

    cfg = HeadConfig(
        center="t",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )
    monkeypatch.setattr(
        dispatch,
        "probe_node",
        lambda *args, **kwargs: NodeStatus(
            node="n1",
            error="ssh: No route to host",
            unreachable=True,
        ),
    )
    monkeypatch.setattr(
        dispatch,
        "resolve_project",
        lambda cfg_, req, cwd: ("p", Project(path=tmp_path)),
    )
    monkeypatch.setattr(dispatch, "git_info", lambda directory: (None, False, None))
    monkeypatch.setattr(
        dispatch,
        "_try_nodes",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("known-unreachable pin must not reach snapshot/launch")
        ),
    )

    with pytest.raises(dispatch.NoReachableNode) as raised:
        dispatch.submit(
            cfg,
            dispatch.RunSpec(
                name="offline",
                gpus=0,
                cmd=["true"],
                node="n1",
            ),
            tmp_path,
            lambda message: None,
            no_queue=True,
        )

    assert raised.value.reasons == {"n1": "ssh: No route to host"}


def test_pinned_unreachable_queue_persists_visible_wait_reason(tmp_path, monkeypatch):
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node, QueueCfg
    from dt.probe import NodeStatus

    cfg = HeadConfig(
        center="t",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )
    monkeypatch.setattr(
        dispatch,
        "probe_node",
        lambda *args, **kwargs: NodeStatus(
            node="n1",
            error="ssh: No route to host",
            unreachable=True,
        ),
    )
    monkeypatch.setattr(
        dispatch,
        "resolve_project",
        lambda cfg_, req, cwd: ("p", Project(path=tmp_path)),
    )
    monkeypatch.setattr(dispatch, "git_info", lambda directory: (None, False, None))
    monkeypatch.setattr(
        dispatch,
        "capture_snapshot",
        lambda *args, **kwargs: dispatch.StoredSnapshot("a" * 64, tmp_path),
    )
    monkeypatch.setattr(
        dispatch,
        "_stage",
        lambda _cfg, _project, _job_id, _spec, meta, *args, **kwargs: (
            meta.update(snapshot_sha256="a" * 64) or tmp_path
        ),
    )

    entry = dispatch.submit(
        cfg,
        dispatch.RunSpec(
            name="offline",
            gpus=0,
            cmd=["true"],
            node="n1",
        ),
        tmp_path,
        lambda message: None,
    )

    assert entry.status == "queued"
    assert entry.reason == "waiting: n1 unreachable: ssh: No route to host"


def test_unpinned_all_unreachable_no_queue_uses_exit_5_class(tmp_path, monkeypatch):
    import pytest

    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node, QueueCfg
    from dt.probe import NodeStatus

    cfg = HeadConfig(
        center="t",
        nodes=[Node(name="n1"), Node(name="n2")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )
    monkeypatch.setattr(
        dispatch,
        "probe_center",
        lambda *args, **kwargs: [
            NodeStatus(node="n1", error="ssh timeout", unreachable=True),
            NodeStatus(node="n2", error="connection refused", unreachable=True),
        ],
    )
    monkeypatch.setattr(
        dispatch,
        "resolve_project",
        lambda cfg_, req, cwd: ("p", Project(path=tmp_path)),
    )
    monkeypatch.setattr(dispatch, "git_info", lambda directory: (None, False, None))

    with pytest.raises(dispatch.NoReachableNode) as raised:
        dispatch.submit(
            cfg,
            dispatch.RunSpec(name="offline", gpus=1, cmd=["true"]),
            tmp_path,
            lambda message: None,
            no_queue=True,
        )

    assert set(raised.value.reasons) == {"n1", "n2"}


def test_pinned_submit_records_job_specific_block_before_agent_tick(
    tmp_path, monkeypatch
):
    """A permanent pin/path mismatch must not initially look capacity-bound."""
    import dt.dispatch as dispatch
    from dt.config import HeadConfig, Node, QueueCfg
    from dt.probe import Gpu, NodeStatus

    cfg = HeadConfig(
        center="t",
        nodes=[Node(name="n1")],
        projects={},
        default_project=None,
        root=tmp_path / "dt",
        envs="~/dt/envs",
        queue=QueueCfg(),
    )
    status = NodeStatus(
        node="n1",
        gpus=[
            Gpu(
                index=0,
                uuid="u",
                mem_used=0,
                mem_total=24576,
                util=0,
                free=True,
            )
        ],
    )
    monkeypatch.setattr(dispatch, "probe_node", lambda *args, **kwargs: status)
    monkeypatch.setattr(
        dispatch,
        "resolve_project",
        lambda cfg_, req, cwd: ("p", Project(path=tmp_path)),
    )
    monkeypatch.setattr(dispatch, "git_info", lambda directory: (None, False, None))
    monkeypatch.setattr(
        dispatch,
        "_stage",
        lambda _cfg, _project, _job_id, _spec, meta, *args, **kwargs: (
            meta.update(snapshot_sha256="a" * 64) or tmp_path
        ),
    )

    def block_queued_job(cfg_, entry, _log):
        entry.reason = "blocked: n1: path-missing: /data/libero"
        dispatch.save(cfg_, entry)
        return "blocked", "path-missing: /data/libero"

    monkeypatch.setattr(dispatch, "dispatch_queued", block_queued_job)

    entry = dispatch.submit(
        cfg,
        dispatch.RunSpec(
            name="blocked",
            gpus=1,
            cmd=["true"],
            node="n1",
            require_path="/data/libero",
        ),
        tmp_path,
        lambda message: None,
    )

    assert entry.status == "queued"
    assert entry.reason == "blocked: n1: path-missing: /data/libero"


def test_pin_is_busy_classifier():
    from dt.dispatch import RunSpec, pin_is_busy
    from dt.probe import Gpu, NodeStatus

    busy = NodeStatus(
        node="n1",
        gpus=[
            Gpu(
                index=0,
                uuid="u",
                mem_used=70000,
                mem_total=81920,
                util=99,
                procs=1,
                free=False,
            )
        ],
    )
    free = NodeStatus(
        node="n1",
        gpus=[
            Gpu(
                index=0,
                uuid="u",
                mem_used=3,
                mem_total=81920,
                util=0,
                procs=0,
                free=True,
            )
        ],
    )
    err = NodeStatus(node="n1", error="ssh timeout")
    spec = RunSpec(name="j", gpus=1, cmd=["true"], node="n1")
    assert pin_is_busy([busy], spec)
    assert not pin_is_busy([free], spec)
    assert not pin_is_busy([err], spec)  # unknown: launcher decides
    assert not pin_is_busy([busy], RunSpec(name="j", gpus=0, cmd=["true"], node="n1"))
    assert not pin_is_busy([busy], RunSpec(name="j", gpus=1, cmd=["true"]))  # unpinned


def test_rsync_has_stall_guards(monkeypatch):
    import shlex
    import subprocess
    from pathlib import Path

    import dt.sshio as sshio

    seen = {}

    def fake_run(cmd, timeout, cancel_event):
        assert cancel_event is None
        seen["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(sshio, "_run_rsync_attempt", fake_run)
    sshio.rsync("a/", "b/")
    assert "--timeout=60" in seen["cmd"]  # io-stall abort for NAT'd links
    remote_shell = shlex.split(seen["cmd"][seen["cmd"].index("-e") + 1])
    config = Path(remote_shell[remote_shell.index("-F") + 1]).read_text()
    assert "ServerAliveInterval 15" in config  # ssh keepalives in -e
    assert "/artifact/%C" in config


def test_transferred_gib_parses_stats():
    stdout = (
        "Number of files: 120\n"
        "Total file size: 5,368,709,120 bytes\n"
        "Total transferred file size: 3,221,225,472 bytes\n"
    )
    assert abs(transferred_gib(stdout) - 3.0) < 0.01
    assert transferred_gib("no stats here") is None
    assert transferred_gib("") is None


# -- auto center -----------------------------------------------------------------


def _node(
    center: str,
    node: str,
    free: int,
    total: int = 8,
    disk_free_gib: float | None = None,
) -> dict:
    row = {
        "center": center,
        "node": node,
        "gpus": [{"index": i, "free": i < free} for i in range(total)],
    }
    if disk_free_gib is not None:
        row["system"] = {"disk_free_gib": disk_free_gib}
    return row


def test_best_center_prefers_single_node_headroom():
    rows = [
        _node("a", "a1", 2),
        _node("a", "a2", 2),  # total 4, best node 2
        _node("b", "b1", 3),  # total 3, best node 3
    ]
    assert best_center(rows, 3) == "b"
    assert best_center(rows, 2) == "b"  # 3 >= 2, biggest headroom wins
    assert best_center(rows, 4) is None  # nobody has 4 on one node


def test_best_center_ignores_error_rows_and_cpu_jobs():
    rows = [
        {"center": "a", "node": "a1", "error": "unreachable"},
        _node("b", "b1", 0),
    ]
    assert best_center(rows, 1) is None
    assert best_center(rows, 0) == "b"  # cpu job: any reachable center


def test_best_center_honors_known_disk_contract_and_keeps_unknown_fallback():
    rows = [
        _node("low", "low-1", 4, disk_free_gib=40),
        _node("fit", "fit-1", 2, disk_free_gib=120),
        _node("unknown", "unknown-1", 3),
    ]

    assert best_center(rows, 2, require_disk_gib=80) == "unknown"
    assert best_center(rows[:2], 2, require_disk_gib=80) == "fit"
    assert best_center(rows[:1], 1, require_disk_gib=80) is None


def test_run_auto_all_heads_unreachable_is_not_no_capacity(monkeypatch):
    import json
    import subprocess

    import dt.remote as remote_mod
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"east": "head-a", "west": "head-b"},
        default_center="east",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        remote_mod,
        "remote_dt",
        lambda head, argv, timeout: subprocess.CompletedProcess(
            argv,
            255,
            "",
            f"ssh: connect to {head}: No route to host",
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "-c",
            "auto",
            "-g",
            "1",
            "-n",
            "auto-outage",
            "--json",
            "--",
            "true",
        ],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    assert json.loads(result.stdout) == {
        "error": "unreachable",
        "message": "cannot select a center: every capacity probe failed",
        "reasons": {
            "east": "ssh: connect to head-a: No route to host",
            "west": "ssh: connect to head-b: No route to host",
        },
        "exit_code": cli.EXIT_UNREACHABLE,
    }


def test_run_auto_reachable_centers_without_capacity_has_json_contract(
    monkeypatch,
):
    import json
    import subprocess

    import dt.remote as remote_mod
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"test": "head"},
        default_center="test",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        remote_mod,
        "remote_dt",
        lambda head, argv, timeout: subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                [
                    {
                        "center": "test",
                        "node": "gpu-node",
                        "gpus": [{"index": 0, "free": False}],
                        "error": None,
                    }
                ]
            ),
            "",
        ),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "-c",
            "auto",
            "-g",
            "1",
            "-n",
            "auto-full",
            "--json",
            "--",
            "true",
        ],
    )

    assert result.exit_code == cli.EXIT_NO_GPU, result.output
    assert json.loads(result.stdout) == {
        "error": "no_capacity",
        "message": "no reachable center has 1 free card(s) on one node",
        "reasons": {},
        "exit_code": cli.EXIT_NO_GPU,
    }


def test_run_auto_partial_outage_without_capacity_is_unknown(
    monkeypatch,
):
    import json
    import subprocess

    import dt.remote as remote_mod
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"full": "head-a", "offline": "head-b"},
        default_center="full",
    )
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)

    def probe(head, argv, timeout):
        if head == "head-a":
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps(
                    [
                        {
                            "center": "full",
                            "node": "gpu-node",
                            "gpus": [{"index": 0, "free": False}],
                            "error": None,
                        }
                    ]
                ),
                "",
            )
        return subprocess.CompletedProcess(
            argv,
            255,
            "",
            "ssh: connect to head-b: Connection timed out",
        )

    monkeypatch.setattr(remote_mod, "remote_dt", probe)

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "-c",
            "auto",
            "-g",
            "1",
            "-n",
            "auto-partial",
            "--json",
            "--",
            "true",
        ],
    )

    assert result.exit_code == cli.EXIT_UNREACHABLE, result.output
    payload = json.loads(result.stdout)
    assert payload["error"] == "unreachable"
    assert payload["message"] == ("cannot select a center: some capacity probes failed")
    assert payload["reasons"] == {
        "offline": "ssh: connect to head-b: Connection timed out"
    }


def test_laptop_clean_defaults_to_only_the_configured_center(monkeypatch):
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"east": "east-head", "west": "west-head"},
        default_center="east",
    )
    forwarded = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda head, argv, tty: forwarded.append((head, argv, tty)) or 0,
    )

    result = CliRunner().invoke(
        cli.app,
        ["clean", "--before", "2026-01-01", "-y"],
    )

    assert result.exit_code == 0, result.output
    assert [head for head, _argv, _tty in forwarded] == ["east-head"]
    assert "cleaning east" in result.output


def test_laptop_clean_requires_explicit_all_centers_escalation(monkeypatch):
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"east": "east-head", "west": "west-head"},
        default_center="east",
    )
    forwarded = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda head, argv, tty: forwarded.append((head, argv, tty)) or 0,
    )

    result = CliRunner().invoke(
        cli.app,
        ["clean", "--before", "2026-01-01", "--all-centers", "-y"],
    )

    assert result.exit_code == 0, result.output
    assert [head for head, _argv, _tty in forwarded] == [
        "east-head",
        "west-head",
    ]
    assert all("-y" in argv for _head, argv, _tty in forwarded)


def test_laptop_clean_can_target_one_nondefault_center(monkeypatch):
    from typer.testing import CliRunner

    from dt import cli
    from dt.config import LaptopConfig

    cfg = LaptopConfig(
        centers={"east": "east-head", "west": "west-head"},
        default_center="east",
    )
    forwarded = []
    monkeypatch.setattr(cli, "_cfg", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "forward_call",
        lambda head, argv, tty: forwarded.append((head, argv, tty)) or 0,
    )

    result = CliRunner().invoke(
        cli.app,
        ["clean", "--before", "2026-01-01", "-c", "west", "-y"],
    )

    assert result.exit_code == 0, result.output
    assert [head for head, _argv, _tty in forwarded] == ["west-head"]


# -- info helpers -----------------------------------------------------------------


def test_parse_marked_segments():
    from dt.cli import INFO_MARK
    from dt.cli.commands.info import _parse_marked

    text = f"1752900000\n{INFO_MARK}\n\n{INFO_MARK}\n1.5G\n{INFO_MARK}\nyes\n"
    started, finished, outputs, patch = _parse_marked(text, 4)
    assert started == "1752900000" and finished == ""
    assert outputs == "1.5G" and patch == "yes"


def test_fmt_duration():
    from dt.cli import _fmt_duration

    assert _fmt_duration(42) == "42s"
    assert _fmt_duration(125) == "2m05s"
    assert _fmt_duration(3700) == "1h01m"
    assert _fmt_duration(-20) == "-20s"


# -- entrypoint exit handling -------------------------------------------------


def test_main_wrapper_translates_a_clean_typer_exit(monkeypatch, capsys):
    """CliRunner bypasses main(), so the real wrapper must be driven directly:
    typer 0.27.2 rebased typer.Exit off the vendored click exceptions module
    (so it now propagates out of standalone_mode=False instead of being
    swallowed into a return code), and only this path proves the CLI still
    exits 0 instead of crashing with an AttributeError inside its own
    exception handler."""
    import sys

    from dt import cli

    monkeypatch.setattr(sys, "argv", ["dt", "--version"])
    code = 0
    try:
        cli.main()
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
    assert code == 0
    assert capsys.readouterr().out.startswith("dt ")


def test_main_wrapper_translates_a_failing_typer_exit(monkeypatch, capsys):
    import json
    import sys

    import pytest

    from dt import cli

    monkeypatch.setattr(
        sys,
        "argv",
        ["dt", "run", "--retry-on", "infra", "--json", "--", "true"],
    )
    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert "--retry" in payload["message"]

"""v0.8 operator-visibility round: probe owners, snapshot stats, auto center,
info parsing helpers."""

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
    ]
    assert busy_owners(gpus) == "alice\u00d72 bob\u00d71"


# -- snapshot size warning -------------------------------------------------------

def test_rsync_has_stall_guards(monkeypatch):
    import subprocess

    import dt.sshio as sshio

    seen = {}

    def fake_run(cmd, capture_output, text, timeout):
        seen["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(sshio.subprocess, "run", fake_run)
    sshio.rsync("a/", "b/")
    assert "--timeout=60" in seen["cmd"]  # io-stall abort for NAT'd links
    joined = " ".join(seen["cmd"])
    assert "ServerAliveInterval=15" in joined  # ssh keepalives in -e


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

def _node(center: str, node: str, free: int, total: int = 8) -> dict:
    return {
        "center": center, "node": node,
        "gpus": [{"index": i, "free": i < free} for i in range(total)],
    }


def test_best_center_prefers_single_node_headroom():
    rows = [
        _node("a", "a1", 2), _node("a", "a2", 2),   # total 4, best node 2
        _node("b", "b1", 3),                        # total 3, best node 3
    ]
    assert best_center(rows, 3) == "b"
    assert best_center(rows, 2) == "b"   # 3 >= 2, biggest headroom wins
    assert best_center(rows, 4) is None  # nobody has 4 on one node


def test_best_center_ignores_error_rows_and_cpu_jobs():
    rows = [
        {"center": "a", "node": "a1", "error": "unreachable"},
        _node("b", "b1", 0),
    ]
    assert best_center(rows, 1) is None
    assert best_center(rows, 0) == "b"  # cpu job: any reachable center


# -- info helpers -----------------------------------------------------------------

def test_parse_marked_segments():
    from dt.cli import INFO_MARK, _parse_marked

    text = f"1752900000\n{INFO_MARK}\n\n{INFO_MARK}\n1.5G\n{INFO_MARK}\nyes\n"
    started, finished, outputs, patch = _parse_marked(text, 4)
    assert started == "1752900000" and finished == ""
    assert outputs == "1.5G" and patch == "yes"


def test_fmt_duration():
    from dt.cli import _fmt_duration

    assert _fmt_duration(42) == "42s"
    assert _fmt_duration(125) == "2m05s"
    assert _fmt_duration(3700) == "1h01m"

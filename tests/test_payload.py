"""Regression guards for the node-side payload and job support files -
every entry here is a lesson from the first real-project run (OmniStack)."""

from pathlib import Path

from dt.dispatch import _support_files, spec_from_entry
from dt.jobs import JobEntry

PAYLOAD = Path(__file__).parent.parent / "src" / "dt" / "payload"
LAUNCHER = (PAYLOAD / "launcher.sh").read_text()
WRAPPER = (PAYLOAD / "wrapper.sh").read_text()


def test_launcher_uses_dedicated_tmux_server():
    # user tmux servers can be systemd-managed (kill-server on stop):
    # jobs must live on dt's own socket
    assert "tmux -L dt new-session" in LAUNCHER
    assert "tmux -L dt kill-session" in LAUNCHER


def test_launcher_forces_managed_python():
    # system interpreters lack Python.h; sdist builds fail without it
    assert "UV_PYTHON_PREFERENCE=only-managed" in LAUNCHER


def test_launcher_setup_hook_contract():
    # hook runs under the env lock, once per env per content, and the sync
    # must be --inexact or it prunes what the hook installed
    assert "setup.sh" in LAUNCHER
    assert "--inexact" in LAUNCHER
    assert ".dt-setup-" in LAUNCHER


def test_wrapper_reaps_group_escapees():
    # setpgrp callers (omnistack-train) leave the pane group; membership
    # test is cwd-inside-job-dir
    assert 'readlink "$p/cwd"' in WRAPPER
    assert '[ "$pid" = "$$" ] && continue' in WRAPPER


def test_support_files_ship_setup_hook():
    files = _support_files(["echo", "hi"], {"job_id": "x"}, setup="uv pip install ./libs/Foo")
    assert files["setup.sh"] == "uv pip install ./libs/Foo\n"
    files_no = _support_files(["echo", "hi"], {"job_id": "x"})
    assert "setup.sh" not in files_no


def test_rerun_replays_setup_hook():
    e = JobEntry(
        job_id="j", name="j", center="c", project="p", node="n", node_local=False,
        job_dir="dt/jobs/j", session="dt_j", cmd="echo hi",
        setup="uv pip install --no-deps ./libs/CleanDiffuser",
    )
    assert spec_from_entry(e).setup == "uv pip install --no-deps ./libs/CleanDiffuser"

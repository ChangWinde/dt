"""dt must never consume the stdin of the script that invoked it.

Field report: ``ssh head 'bash -s' < submit.sh`` submitted 6 of 53 jobs because
the first ``dt run`` spawned ssh with the parent's stdin, and ssh drained the
rest of the script to forward it to the node. Every non-interactive child dt
starts gets ``/dev/null`` instead; only an explicit stdin payload or an
interactive (``-t``) forward passes anything through.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

SCRIPT_REST = "echo REST-OF-SCRIPT\n" * 3


def _child_sees(source: str) -> str:
    """Run ``source`` in a fresh interpreter whose stdin holds the script rest."""
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        input=SCRIPT_REST,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


def test_bounded_process_gives_the_child_dev_null_not_the_callers_stdin():
    out = _child_sees(
        """
        import dt.sshio as sshio
        proc = sshio._run_bounded_process(["cat"], timeout=10)
        print(repr(proc.stdout))
        """
    )
    assert out.strip() == "''"


def test_bounded_process_still_delivers_an_explicit_stdin_payload():
    out = _child_sees(
        """
        import dt.sshio as sshio
        proc = sshio._run_bounded_process(["cat"], timeout=10, stdin_bytes=b"payload")
        print(repr(proc.stdout))
        """
    )
    assert out.strip() == "'payload'"


def test_non_interactive_forward_call_does_not_drain_the_callers_stdin():
    out = _child_sees(
        """
        import dt.remote as remote
        # a stand-in "ssh" that copies its stdin to stdout and ignores its arguments
        remote.ssh_base = lambda: ["sh", "-c", "cat", "ssh"]
        rc = remote.forward_call("head", ["ps"], tty=False)
        print("rc", rc)
        """
    )
    assert out.strip() == "rc 0"  # nothing but our own line: cat printed nothing


def test_interactive_forward_call_keeps_a_real_terminal_attached():
    out = _child_sees(
        """
        import sys
        import dt.remote as remote
        remote.ssh_base = lambda: ["sh", "-c", "cat", "ssh"]
        sys.stdin.isatty = lambda: True  # pretend the pipe is a terminal
        remote.forward_call("head", ["attach"], tty=True)
        """
    )
    assert "REST-OF-SCRIPT" in out


def test_tty_forward_from_a_pipe_does_not_drain_the_script_either():
    """`dt kill` in a piped script: the head prompt could never be answered."""
    out = _child_sees(
        """
        import dt.remote as remote
        remote.ssh_base = lambda: ["sh", "-c", "cat", "ssh"]
        rc = remote.forward_call("head", ["kill", "job"], tty=True)
        print("rc", rc)
        """
    )
    assert out.strip() == "rc 0"

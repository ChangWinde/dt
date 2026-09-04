"""The shell libraries dt ships inside its probes load and define what they claim."""

from __future__ import annotations

import shutil
import subprocess

import pytest

from dt import lifecycle, shell

LIBRARIES = {
    "process_identity.sh": {
        "dt_pid_ticks",
        "dt_pid_group",
        "dt_pid_state",
        "dt_pid_has_live_task",
        "dt_pid_zombie",
        "dt_pid_cwd_owned",
        "dt_process_owned",
    },
    "runtime_scope.sh": {
        "dt_scope_marker",
        "dt_containment_marker",
        "dt_requested_gpus",
        "dt_gpu_containment_unproven",
        "dt_scope_census",
    },
    "liveness.sh": {"dt_job_live_state"},
    "termination_probe_functions.sh": {"group_open", "sig_scan", "survivors"},
}


def _defined_functions(script: str) -> set[str]:
    proc = subprocess.run(
        ["bash", "-c", script + "\ndeclare -F"],
        capture_output=True,
        text=True,
        check=True,
    )
    return {line.split()[-1] for line in proc.stdout.splitlines()}


@pytest.mark.parametrize(("name", "functions"), list(LIBRARIES.items()))
def test_library_parses_and_defines_exactly_its_functions(name, functions):
    text = shell.load(name)

    assert text.endswith("\n")
    subprocess.run(["bash", "-n"], input=text, text=True, check=True)
    # Libraries only call into the ones loaded before them.
    order = list(LIBRARIES)
    prelude = "".join(shell.load(dep) for dep in order[: order.index(name)])
    defined = _defined_functions(prelude + text) - _defined_functions(prelude)
    assert defined == functions


def test_cancel_sentinel_is_a_statement_sequence_not_a_library():
    text = shell.load("cancel_sentinel.sh")

    assert "$DT_KCANCEL" in text
    subprocess.run(["bash", "-n"], input=text, text=True, check=True)
    assert _defined_functions(lifecycle.process_identity_shell()) >= {
        "dt_process_owned"
    }


def test_liveness_shell_composes_the_three_libraries_in_dependency_order():
    composed = lifecycle.liveness_shell()

    assert composed.index("dt_pid_ticks()") < composed.index("dt_scope_marker()")
    assert composed.index("dt_scope_marker()") < composed.index("dt_job_live_state()")
    assert _defined_functions(composed) == (
        LIBRARIES["process_identity.sh"]
        | LIBRARIES["runtime_scope.sh"]
        | LIBRARIES["liveness.sh"]
    )


@pytest.mark.parametrize(
    "name", ["probe_gpu.sh", "probe_apps.sh", "probe_system.sh", "cancel_sentinel.sh"]
)
def test_statement_resources_parse_under_posix_shells(name):
    text = shell.load(name)

    subprocess.run(["bash", "-n"], input=text, text=True, check=True)
    if shutil.which("dash"):
        subprocess.run(["dash", "-n"], input=text, text=True, check=True)


def test_probe_resources_carry_the_markers_the_parser_expects():
    from dt import probe

    assert probe.GPU_ERROR in shell.load("probe_gpu.sh")
    assert probe.APP_ERROR in shell.load("probe_apps.sh")
    assert probe.GPU_Q in probe.PROBE_CMD and probe.APP_Q in probe.PROBE_CMD
    assert probe.SYSTEM_Q in probe.PROBE_CMD

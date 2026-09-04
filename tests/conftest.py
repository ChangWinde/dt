"""Session-wide isolation for DT's test suite.

Test fixtures create disposable Git repositories and run the real ``git``
binary. Ambient user or system Git configuration — for example a global
``core.hooksPath`` that enforces a commit-message policy — must not decide
whether DT's fixtures can commit. Neutralize it for every test and every
subprocess so the suite proves DT behavior, not the developer machine's
Git policy. Fixtures that commit still set their own local identity.

The same holds for the caller's terminal decorations: Rich honours TERM,
NO_COLOR, FORCE_COLOR, and COLUMNS from the ambient environment (a dumb
terminal even overrides an explicit ``Console(width=...)``), so an IDE or
CI shell exporting them flips dozens of rendering assertions. Drop them so
the suite always observes DT's own defaults.
"""

import os

import pytest

os.environ["GIT_CONFIG_GLOBAL"] = os.devnull
os.environ["GIT_CONFIG_SYSTEM"] = os.devnull

for _ambient_terminal_variable in (
    "TERM",
    "COLUMNS",
    "LINES",
    "NO_COLOR",
    "FORCE_COLOR",
    "CLICOLOR",
    "CLICOLOR_FORCE",
):
    os.environ.pop(_ambient_terminal_variable, None)


@pytest.fixture(autouse=True)
def _compatible_idle_agent_protocol(monkeypatch):
    """Keep submission tests independent of the user's installed DT release.

    Tests that exercise the real active-command probe override this narrow seam
    explicitly.  The resident-agent branch and the probe implementation retain
    their own focused contract tests.
    """
    import dt.dispatch as dispatch
    from dt.jobs import DISPATCH_PROTOCOL_VERSION

    monkeypatch.setattr(
        dispatch,
        "_active_command_dispatch_protocol",
        lambda: DISPATCH_PROTOCOL_VERSION,
    )


@pytest.fixture
def stub_job_refresh(monkeypatch):
    """Stub job status refresh on a module with a one-job function.

    ``dt ps`` and the agent refresh through ``refresh_statuses`` (one probe
    per node); ``dt wait``/``logs``/``kill`` still refresh one job through
    ``refresh_status``. Tests keep writing ``refresh(cfg, entry)`` (optionally
    accepting ``observation=``) and this stubs both entry points consistently.
    """
    import inspect

    def stub(module, refresh):
        takes_observation = any(
            name == "observation" or param.kind is inspect.Parameter.VAR_KEYWORD
            for name, param in inspect.signature(refresh).parameters.items()
        )

        def one(cfg, entry, timeout=8, *, observation=None):
            if takes_observation:
                return refresh(cfg, entry, observation=observation)
            return refresh(cfg, entry)

        def many(cfg, entries, timeout=8, *, observations=None):
            refreshed = {}
            for entry in entries:
                observation = (
                    {}
                    if observations is None
                    else observations.setdefault(entry.job_id, {})
                )
                observation.update(node_unreachable=False, status_probe_error=None)
                refreshed[entry.job_id] = one(cfg, entry, observation=observation)
            return refreshed

        if hasattr(module, "refresh_status"):
            monkeypatch.setattr(module, "refresh_status", one)
        monkeypatch.setattr(module, "refresh_statuses", many)

    return stub

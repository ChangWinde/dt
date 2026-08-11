"""Minimal installed-command bootstrap.

Keep identity probes cheap while preserving the operation journal contract.
All other commands load the complete Typer application lazily.
"""

from __future__ import annotations

import sys


def _cli_main() -> None:
    from .cli import main

    main()


def _version_main(argv: list[str]) -> None:
    from . import operation_log
    from .version import version_text

    session = operation_log.begin(argv)
    exit_code = 0
    status = "success"
    failure: BaseException | None = None
    try:
        print(version_text())
    except KeyboardInterrupt as exc:
        exit_code = 130
        status = "interrupted"
        failure = exc
        operation_log.mark_problem("interrupted", exc)
        raise
    except BaseException as exc:
        exit_code = 1
        status = "failed"
        failure = exc
        operation_log.mark_problem("internal_exception", exc)
        raise
    finally:
        operation_log.finish(
            session,
            exit_code=exit_code,
            status=status,
            exc=failure if status != "success" else None,
        )
        if session.journal_errors:
            kinds = ", ".join(sorted(set(session.journal_errors)))
            print(
                "operation journal unavailable; this command was not fully "
                f"recorded ({kinds})",
                file=sys.stderr,
            )


def main() -> None:
    argv = sys.argv[1:]
    if argv == ["--version"]:
        _version_main(argv)
        return
    _cli_main()


if __name__ == "__main__":
    main()

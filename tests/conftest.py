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

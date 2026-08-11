"""Session-wide isolation for DT's test suite.

Test fixtures create disposable Git repositories and run the real ``git``
binary. Ambient user or system Git configuration — for example a global
``core.hooksPath`` that enforces a commit-message policy — must not decide
whether DT's fixtures can commit. Neutralize it for every test and every
subprocess so the suite proves DT behavior, not the developer machine's
Git policy. Fixtures that commit still set their own local identity.
"""

import os

os.environ["GIT_CONFIG_GLOBAL"] = os.devnull
os.environ["GIT_CONFIG_SYSTEM"] = os.devnull

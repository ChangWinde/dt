"""Small helpers for keeping operator-private detail out of shared output.

dt runs on multi-tenant clusters, so absolute paths (which embed a username),
journal locations, and similar detail should not cross a trust boundary in
JSON payloads, error text, or diagnostics. These helpers are intentionally
conservative: they only rewrite what is unambiguously private and never raise.
"""

from __future__ import annotations

import ipaddress
import os
import re

_USER_AT_HOST_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@(?=[A-Za-z0-9\[])")
# Hostname-shaped tokens with at least three labels (gpu-node-7.dc2.internal).
# One-dot tokens (config.yaml, OpenSSH_9.6) stay: they are usually files or
# versions, and masking them would gut the diagnostic for no privacy gain.
_DOTTED_TOKEN_RE = re.compile(r"\b[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+){2,}\b")
_IPV6_CANDIDATE_RE = re.compile(r"\b[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f:]+)+\b")
_REMOTE_DETAIL_MAX = 160


def redact_home_path(text: str) -> str:
    """Rewrite the current user's home-directory prefix to ``~`` for display.

    A journal path like ``/home/alice/.local/state/dt/...`` otherwise leaks the
    operator's username. Home resolution failing (no HOME) must not raise, so
    the original text is returned unchanged in that case.
    """
    if not text:
        return text
    try:
        home = os.path.expanduser("~")
    except (RuntimeError, OSError):
        return text
    if not home or home == "/":
        return text
    return text.replace(home, "~")


def _mask_dotted_token(match: re.Match[str]) -> str:
    token = match.group(0)
    try:
        ipaddress.ip_address(token)
        return "<addr>"
    except ValueError:
        return "<host>"


def _mask_ipv6_candidate(match: re.Match[str]) -> str:
    token = match.group(0)
    try:
        ipaddress.ip_address(token)
        return "<addr>"
    except ValueError:
        return token


def redact_remote_detail(text: str) -> str:
    """Mask remote endpoint identities in one bounded diagnostic line.

    Remote stderr (SSH banners, resolver errors) names hosts, addresses, and
    accounts on the other side of a trust boundary; verbatim copies of it end
    up in doctor JSON and similar shareable output. The error vocabulary that
    makes a failure actionable (refused, timed out, no route, permission
    denied) survives; IP literals, multi-label hostnames, home paths, and the
    user part of ``user@host`` do not.
    """
    if not text:
        return text
    text = redact_home_path(text)
    text = _USER_AT_HOST_RE.sub("<user>@", text)
    text = _DOTTED_TOKEN_RE.sub(_mask_dotted_token, text)
    text = _IPV6_CANDIDATE_RE.sub(_mask_ipv6_candidate, text)
    return " ".join(text.split())[:_REMOTE_DETAIL_MAX]

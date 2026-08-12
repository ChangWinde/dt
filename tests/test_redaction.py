"""Redaction helpers must strip operator-private detail and never raise."""

from dt import cli
from dt.redaction import redact_home_path


def test_uncaught_exceptions_never_render_frame_locals():
    # Frame locals routinely hold webhook tokens and whole config mappings;
    # crash output is exactly what operators paste into shared channels.
    assert cli.app.pretty_exceptions_show_locals is False


def test_home_prefix_is_rewritten_to_tilde(monkeypatch):
    monkeypatch.setenv("HOME", "/home/alice")
    assert (
        redact_home_path("/home/alice/.local/state/dt/operations")
        == "~/.local/state/dt/operations"
    )


def test_embedded_home_occurrences_are_rewritten(monkeypatch):
    monkeypatch.setenv("HOME", "/home/alice")
    text = "journal /home/alice/a and /home/alice/b"
    assert redact_home_path(text) == "journal ~/a and ~/b"


def test_text_without_home_is_unchanged(monkeypatch):
    monkeypatch.setenv("HOME", "/home/alice")
    assert redact_home_path("/var/lib/dt/journal") == "/var/lib/dt/journal"


def test_root_home_never_rewrites(monkeypatch):
    # A HOME of "/" would turn every absolute path into ~-relative noise.
    monkeypatch.setenv("HOME", "/")
    assert redact_home_path("/var/lib/dt") == "/var/lib/dt"


def test_empty_text_is_returned_as_is():
    assert redact_home_path("") == ""

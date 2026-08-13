"""Redaction helpers must strip operator-private detail and never raise."""

from dt import cli
from dt.redaction import redact_home_path, redact_remote_detail


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


def test_remote_detail_masks_addresses_hosts_and_accounts():
    # SSH stderr names the far side of a trust boundary; the failure
    # vocabulary must survive while endpoint identities disappear.
    detail = redact_remote_detail(
        "ssh: connect to host gpu-node-7.dc2.internal port 22222: Connection refused"
    )
    assert detail == "ssh: connect to host <host> port 22222: Connection refused"

    assert (
        redact_remote_detail("Connection closed by 198.18.0.77 port 22")
        == "Connection closed by <addr> port 22"
    )
    assert (
        redact_remote_detail("Connection closed by 2001:db8::77 port 22")
        == "Connection closed by <addr> port 22"
    )
    assert (
        redact_remote_detail("Permission denied for alice@rack-b (publickey)")
        == "Permission denied for <user>@rack-b (publickey)"
    )


def test_remote_detail_keeps_files_versions_and_error_vocabulary():
    kept = "load pubkey config.yaml: invalid format (OpenSSH_9.6p1)"
    assert redact_remote_detail(kept) == kept
    # Timestamps are colon-separated but are not addresses.
    assert redact_remote_detail("failed at 12:34:56") == "failed at 12:34:56"


def test_remote_detail_is_bounded_and_never_raises_on_noise():
    noisy = "x" * 500
    assert len(redact_remote_detail(noisy)) <= 160
    assert redact_remote_detail("") == ""

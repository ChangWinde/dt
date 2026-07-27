import os

from dt.snapshot_hash import tree_sha256


def test_tree_sha256_is_stable_and_ignores_mtime(tmp_path):
    root = tmp_path / "code"
    root.mkdir()
    source = root / "train.py"
    source.write_text("print('v1')\n")
    first = tree_sha256(root)

    os.utime(source, (1_000_000, 1_000_000))
    assert tree_sha256(root) == first


def test_tree_sha256_binds_content_mode_and_symlink_target(tmp_path):
    root = tmp_path / "code"
    root.mkdir()
    source = root / "train.py"
    source.write_text("print('v1')\n")
    link = root / "entrypoint"
    link.symlink_to("train.py")
    baseline = tree_sha256(root)

    source.write_text("print('v2')\n")
    content_changed = tree_sha256(root)
    assert content_changed != baseline

    source.chmod(0o755)
    mode_changed = tree_sha256(root)
    assert mode_changed != content_changed

    other = root / "other.py"
    other.write_text("print('v2')\n")
    link.unlink()
    link.symlink_to("other.py")
    assert tree_sha256(root) != mode_changed

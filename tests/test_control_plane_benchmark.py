import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest


def _load_benchmark_module():
    script = Path(__file__).parents[1] / "scripts" / "benchmark_control_plane.py"
    spec = importlib.util.spec_from_file_location("dt_control_plane_benchmark", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _benchmark_input_fixture(tmp_path):
    root = tmp_path / "repository"
    source = root / "src" / "dt"
    scripts = root / "scripts"
    source.mkdir(parents=True)
    scripts.mkdir()
    (root / "pyproject.toml").write_text("[project]\nname='dt'\n", encoding="utf-8")
    (root / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    (source / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    script = scripts / "benchmark_control_plane.py"
    script.write_text("print('benchmark')\n", encoding="utf-8")
    return root, script


def test_benchmark_input_digest_binds_only_behavior_inputs(tmp_path):
    module = _load_benchmark_module()
    root, script = _benchmark_input_fixture(tmp_path)

    baseline = module._benchmark_input_sha256(root, script)
    assert re.fullmatch(r"[0-9a-f]{64}", baseline)
    assert module._benchmark_input_sha256(root, script) == baseline

    documentation = root / "docs" / "generated-report.md"
    documentation.parent.mkdir()
    documentation.write_text("not an input\n", encoding="utf-8")
    assert module._benchmark_input_sha256(root, script) == baseline

    (root / "src" / "dt" / "module.py").write_text("VALUE = 2\n", encoding="utf-8")
    assert module._benchmark_input_sha256(root, script) != baseline


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_benchmark_input_digest_rejects_unsafe_source_objects(tmp_path, kind):
    module = _load_benchmark_module()
    root, script = _benchmark_input_fixture(tmp_path)
    unsafe = root / "src" / "dt" / "unsafe"
    if kind == "symlink":
        unsafe.symlink_to("module.py")
    else:
        os.mkfifo(unsafe)

    with pytest.raises(RuntimeError, match="benchmark input"):
        module._benchmark_input_sha256(root, script)


def test_benchmark_input_digest_rejects_a_file_changed_during_read(
    tmp_path, monkeypatch
):
    module = _load_benchmark_module()
    root, script = _benchmark_input_fixture(tmp_path)
    target = root / "pyproject.toml"
    real_read = module.os.read
    changed = False

    def racing_read(descriptor, size):
        nonlocal changed
        chunk = real_read(descriptor, size)
        if chunk and not changed:
            changed = True
            target.write_text("[project]\nname='changed'\n", encoding="utf-8")
        return chunk

    monkeypatch.setattr(module.os, "read", racing_read)

    with pytest.raises(RuntimeError, match="changed during read"):
        module._benchmark_input_sha256(root, script)


def test_control_plane_benchmark_is_bounded_and_cleans_its_fixture(tmp_path):
    root = Path(__file__).parents[1]
    script = root / "scripts" / "benchmark_control_plane.py"
    output = tmp_path / "result.json"
    report = tmp_path / "result.md"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--terminal-jobs",
            "20",
            "--active-jobs",
            "5",
            "--nodes",
            "3",
            "--warmups",
            "0",
            "--samples",
            "1",
            "--reference-warmups",
            "0",
            "--reference-samples",
            "1",
            "--cold-warmups",
            "0",
            "--cold-samples",
            "1",
            "--probe-warmups",
            "0",
            "--probe-samples",
            "1",
            "--probe-delay-s",
            "0.1",
            "--probe-budget-s",
            "0.02",
            "--json-output",
            str(output),
            "--markdown-output",
            str(report),
        ],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert proc.returncode == 0, proc.stderr
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "dt_control_plane_benchmark_v1"
    assert re.fullmatch(r"[0-9a-f]{64}", payload["benchmark_input_sha256"])
    assert payload["fixture"]["registry_rows"] == 25
    assert payload["fixture"]["removed"] is True
    assert not Path(payload["fixture"]["path"]).exists()
    assert set(payload["metrics"]) == {
        "full_registry_scan_reference",
        "cold_active_index_rebuild",
        "warm_active_entries",
        "idle_agent_tick",
        "agent_status",
        "active_ps",
        "free_scheduler_context",
        "ordinary_free_probe",
    }
    assert all(metric["samples"] == 1 for metric in payload["metrics"].values())
    assert payload["comparisons"]["warm_active_entries"]["speedup_x"] > 0
    assert (
        payload["metrics"]["ordinary_free_probe"]["slow_capacity_schedulable"] is False
    )
    rendered = report.read_text(encoding="utf-8")
    assert "Overall:" in rendered
    assert f"benchmark input SHA-256: `{payload['benchmark_input_sha256']}`" in rendered

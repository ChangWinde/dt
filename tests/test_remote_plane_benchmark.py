from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "benchmark_remote_plane.py"


def _fake_dt(tmp_path: Path) -> tuple[Path, Path]:
    calls = tmp_path / "calls.jsonl"
    command = tmp_path / "dt"
    command.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "with open(os.environ['FAKE_DT_CALLS'], 'a', encoding='utf-8') as f:\n"
        "    f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "args = sys.argv[1:]\n"
        "if args == ['--version']:\n"
        "    print('dt 0.test (abcdef123456)')\n"
        "elif args[:2] == ['topology', '--json']:\n"
        "    print(json.dumps({'schema_version':'dt_topology_v1','summary':"
        "{'sites':1,'edge_limit':8,'direct_edges':1,'unavailable_edges':1},"
        "'control_routes':[{'node':'n1','link_class':'direct'}],"
        "'sites':[{'site':'s','edges':["
        "{'source':'n1','destination':'n2','status':'direct','latency_ms':3},"
        "{'source':'n2','destination':'n1','status':'unavailable',"
        "'error_kind':'authentication'}]}]}))\n"
        "elif args[:2] == ['doctor', '--json']:\n"
        "    print(json.dumps({'schema_version':'dt_doctor_v2','summary':"
        "{'healthy':False,'nodes':2,'errors':1,'warnings':1,'exit_code':5},"
        "'issues':[{'node':'n2','kind':'unreachable','severity':'error'}]}))\n"
        "    raise SystemExit(5)\n"
        "elif args[:3] == ['agent', 'status', '--json']:\n"
        "    print(json.dumps({'schema_version':'dt_agent_status_v1','alive':True}))\n"
        "elif args[:2] == ['free', '--json']:\n"
        "    print(json.dumps({'schema_version':'dt_free_v1','nodes':[]}))\n"
        "elif args and args[0] == 'run' and '--plan' in args:\n"
        "    print(json.dumps({'schema_version':'dt_run_plan_v1','decision':'start_now'}))\n"
        "elif args and args[0] == 'run':\n"
        "    print(json.dumps({'schema_version':'dt_submission_v1','job_id':'canary-job'}))\n"
        "elif args and args[0] in {'wait', 'logs', 'metrics', 'pull'}:\n"
        "    print(json.dumps({'schema_version':'dt_fake_result_v1','status':'ok'}))\n"
        "else:\n"
        "    print(json.dumps({'error':'unexpected','args':args}))\n"
        "    raise SystemExit(9)\n",
        encoding="utf-8",
    )
    command.chmod(0o755)
    return command, calls


def test_remote_benchmark_is_read_only_by_default_and_projects_bounded_evidence(
    tmp_path,
):
    fake_dt, calls_path = _fake_dt(tmp_path)
    output = tmp_path / "report.json"
    markdown = tmp_path / "report.md"
    env = {**os.environ, "FAKE_DT_CALLS": str(calls_path)}

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--dt-command",
            str(fake_dt),
            "--project",
            "tiny",
            "--node",
            "n1",
            "--samples",
            "2",
            "--json-output",
            str(output),
            "--markdown-output",
            str(markdown),
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert proc.returncode == 0, proc.stderr
    payload = json.loads(output.read_text())
    assert payload["schema_version"] == "dt_remote_performance_v1"
    assert len(payload["source_input_sha256"]) == 64
    assert payload["scope"]["mutating_canary"] is False
    assert payload["network"]["direct_edges"] == 1
    assert payload["network"]["unavailable_edges"] == 1
    assert payload["network"]["failure_kinds"] == {"authentication": 1}
    assert payload["control_plane"]["plan"]["samples"] == 2
    assert payload["local_log_data_plane"]["status"] == "passed"
    assert payload["local_log_data_plane"]["input_bytes"] == 32 * 1024 * 1024
    assert payload["local_log_data_plane"]["retained_bytes"] == 16 * 1024 * 1024
    assert 1 <= payload["local_log_data_plane"]["retained_files"] <= 4
    assert payload["remote_experiment"]["status"] == "skipped"
    assert "endpoint" not in json.dumps(payload)
    assert "Remote data plane" in markdown.read_text()
    assert "Operational readiness" in markdown.read_text()

    calls = [json.loads(line) for line in calls_path.read_text().splitlines()]
    plan_calls = [
        call for call in calls if call and call[0] == "run" and "--plan" in call
    ]
    assert plan_calls
    assert all(call[:2] != ["run", "run"] for call in plan_calls)
    assert not any(call and call[0] == "run" and "--plan" not in call for call in calls)


def test_remote_benchmark_runs_the_complete_canary_only_when_explicit(tmp_path):
    fake_dt, calls_path = _fake_dt(tmp_path)
    output = tmp_path / "report.json"
    env = {**os.environ, "FAKE_DT_CALLS": str(calls_path)}

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--dt-command",
            str(fake_dt),
            "--project",
            "tiny",
            "--node",
            "n1",
            "--samples",
            "1",
            "--execute-canary",
            "n1",
            "--json-output",
            str(output),
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert proc.returncode == 0, proc.stderr
    payload = json.loads(output.read_text())
    remote = payload["remote_experiment"]
    assert remote["status"] == "passed"
    assert remote["job_id"] == "canary-job"
    assert set(remote["operations"]) == {"submit", "wait", "logs", "metrics", "pull"}
    assert all(item["exit_code"] == 0 for item in remote["operations"].values())

    calls = [json.loads(line) for line in calls_path.read_text().splitlines()]
    mutating = [call[0] for call in calls if call and "--plan" not in call]
    assert mutating.count("run") == 1
    for command in ("wait", "logs", "metrics", "pull"):
        assert mutating.count(command) == 1

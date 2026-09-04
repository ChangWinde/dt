"""`dt doctor`: audit head and node capability and install identity."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import cast
import json
import shlex
import subprocess

from rich.markup import escape
import typer

from ... import cli as _root
from ... import jobs as jobs_mod
from ...config import HeadConfig
from ...doctor import (
    default_project_status,
    head_capability_checks,
    registry_growth_status,
)
from ...remote import center_worker_count
from ...render import doctor_table, err
from ...sshio import RemoteError
from ...version import parse_version_identity, version_text
from .. import EXIT_UNREACHABLE, JsonDict
from ... import agent as agent_mod

_DOCTOR_DEPENDENCIES = (
    "gpu",
    "uv",
    "tmux",
    "rsync",
    "flock",
    "python3",
    "timeout",
    "dt",
    "bash",
    "supervisor",
)


def _install_identity_checks() -> dict[str, str]:
    """Expose this install's content digests for upgrade acceptance."""
    checks: dict[str, str] = {}
    install = _root.install_digest()
    if install is not None:
        checks["install"] = install
    payload = _root.payload_digest()
    if payload is not None:
        checks["payload"] = payload
    return checks


def _head_content_mismatch(local: dict[str, str], remote: dict[str, str]) -> str | None:
    """Explain a same-version head whose installed content differs from ours.

    A differing version number is already visible in the ``dt`` check. The
    dangerous case is an identical version hiding hot-patched or stale files,
    which only the content digests can reveal.
    """
    if not remote or remote.get("version") != local.get("version"):
        return None
    diffs = [
        f"{field} local {local[field]} != head {remote[field]}"
        for field in ("install", "payload")
        if field in local and field in remote and local[field] != remote[field]
    ]
    if not diffs:
        return None
    return "mismatch: same version, different content: " + "; ".join(diffs)


def _doctor_contract(rows: list[JsonDict], *, exit_code: int) -> JsonDict:
    """Derive machine actions from the same bounded facts rendered to humans."""
    issues: list[JsonDict] = []
    actions: list[JsonDict] = []
    action_keys: set[str] = set()

    def add(
        *,
        node: str,
        kind: str,
        severity: str,
        check: str,
        observed: str,
        action: JsonDict | None = None,
    ) -> None:
        issues.append(
            {
                "node": node,
                "kind": kind,
                "severity": severity,
                "facts": {"check": check, "observed": observed},
            }
        )
        if action is None:
            return
        candidate = {"issue_kind": kind, "node": node, **action}
        key = json.dumps(candidate, sort_keys=True, separators=(",", ":"))
        if key not in action_keys:
            action_keys.add(key)
            actions.append(candidate)

    for row in rows:
        node = str(row.get("node") or "(unknown)")
        checks = row.get("checks")
        if not isinstance(checks, dict):
            add(
                node=node,
                kind="invalid_health_record",
                severity="error",
                check="schema",
                observed="checks missing",
            )
            continue
        ssh = str(checks.get("ssh", "missing"))
        if ssh != "ok":
            add(
                node=node,
                kind=("unreachable" if row.get("unreachable", True) else "ssh_failure"),
                severity="error",
                check="ssh",
                observed=ssh,
                action={"type": "argv", "argv": ["ssh", node, "true"]},
            )
        for dependency in _DOCTOR_DEPENDENCIES:
            observed = str(checks.get(dependency, ""))
            if observed.startswith("missing") or (
                dependency == "gpu" and observed.startswith("error")
            ):
                add(
                    node=node,
                    kind=(
                        "gpu_driver_failure"
                        if dependency == "gpu" and observed.startswith("error")
                        else "missing_dependency"
                    ),
                    severity="error",
                    check=dependency,
                    observed=observed,
                    action={"type": "install_dependency", "dependency": dependency},
                )
        agent = str(checks.get("agent", ""))
        if agent.startswith(("fail", "off")):
            add(
                node=node,
                kind="agent_unavailable",
                severity="error",
                check="agent",
                observed=agent,
                action={"type": "argv", "argv": ["dt", "agent", "status", "--json"]},
            )
        relay = str(checks.get("relay", ""))
        if relay.startswith("fail"):
            add(
                node=node,
                kind="relay_authentication_failure",
                severity="error",
                check="relay",
                observed=relay,
                action={"type": "inspect_gateway_credentials"},
            )
        lan = str(checks.get("lan", ""))
        if lan.startswith("stale"):
            add(
                node=node,
                kind="stale_lan_address",
                severity="error",
                check="lan",
                observed=lan,
                action={"type": "config_edit", "field": "nodes[].lan_address"},
            )
        network = str(checks.get("net", ""))
        if network.startswith(("slow", "blocked")):
            add(
                node=node,
                kind="network_degraded",
                severity="warning",
                check="net",
                observed=network,
                action={"type": "argv", "argv": ["dt", "seed", node, "--plan"]},
            )
        default_project = str(checks.get("default_project", ""))
        if default_project.startswith("unavailable"):
            add(
                node=node,
                kind="default_project_unavailable",
                severity="warning",
                check="default_project",
                observed=default_project,
                action={"type": "config_edit", "field": "default_project"},
            )
        link = str(checks.get("link", ""))
        if link.startswith(("relayed", "proxied")):
            add(
                node=node,
                kind="bulk_route_indirect",
                severity="warning",
                check="link",
                observed=link,
                action={
                    "type": "argv",
                    "argv": ["dt", "topology", "--destination", node, "--json"],
                },
            )
        gpu = str(checks.get("gpu", ""))
        linger = str(checks.get("linger", "unavailable"))
        if gpu and not gpu.startswith(("missing", "error")) and linger != "yes":
            add(
                node=node,
                kind="gpu_runtime_not_persistent",
                severity="error",
                check="linger",
                observed=linger,
                action={"type": "enable_linger"},
            )
        registry = str(checks.get("registry", ""))
        if registry.startswith("large"):
            add(
                node=node,
                kind="registry_growth",
                severity="warning",
                check="registry",
                observed=registry,
                action={"type": "argv", "argv": ["dt", "clean", "--plan"]},
            )
        dt_content = str(checks.get("dt_content", ""))
        if dt_content.startswith("mismatch"):
            add(
                node=node,
                kind="head_content_mismatch",
                severity="warning",
                check="dt_content",
                observed=dt_content,
            )
    errors = sum(issue["severity"] == "error" for issue in issues)
    warnings = len(issues) - errors
    return {
        "schema_version": "dt_doctor_v2",
        "summary": {
            "healthy": exit_code == 0,
            "nodes": len(rows),
            "errors": errors,
            "warnings": warnings,
            "exit_code": exit_code,
        },
        "nodes": rows,
        "issues": issues,
        "actions": actions,
    }


def _render_doctor_actions(payload: JsonDict) -> None:
    actions = payload.get("actions")
    if not isinstance(actions, list):
        return
    for index, action in enumerate(actions):
        if not isinstance(action, dict):
            continue
        prefix = "next:" if index == 0 else "     "
        node = escape(str(action.get("node") or "(unknown)"))
        action_type = action.get("type")
        if action_type == "argv" and isinstance(action.get("argv"), list):
            command = shlex.join([str(item) for item in action["argv"]])
            err.print(f"[dim]{prefix} {escape(command)}[/dim]")
        elif action_type == "config_edit":
            err.print(
                f"[dim]{prefix} update {escape(str(action.get('field')))} "
                f"for {node} in the head configuration[/dim]"
            )
        elif action_type == "install_dependency":
            err.print(
                f"[dim]{prefix} install {escape(str(action.get('dependency')))} "
                f"on {node}[/dim]"
            )
        elif action_type == "inspect_gateway_credentials":
            err.print(
                f"[dim]{prefix} verify gateway-local SSH credentials for {node}[/dim]"
            )
        elif action_type == "enable_linger":
            err.print(
                f'[dim]{prefix} on {node} run: loginctl enable-linger "$(id -un)" '
                "(allowed for your own account on most systems; otherwise ask an "
                "administrator)[/dim]"
            )


def doctor(
    json_: bool = typer.Option(
        False, "--json", help="emit one dt_doctor_v2 object on stdout"
    ),
    rows_json: bool = typer.Option(False, "--rows-json", hidden=True),
) -> None:
    """Verify SSH, GPU, transfer tools, runtime contracts, and network."""
    cfg = _root._cfg()
    if isinstance(cfg, HeadConfig):
        rows = _root.doctor_center(cfg)

        n_queued = len(jobs_mod.queued_entries(cfg))
        agent_state = agent_mod.status(cfg)
        agent_ok = bool(agent_state["alive"])
        relay_status = _root.relay_agent_status(cfg)
        agent_label = (
            "fail: scheduler stalled"
            if agent_state.get("scheduler_stalled")
            else "fail: runtime command stale"
            if agent_state.get("runtime_command_stale")
            else "ok"
            if agent_ok
            else (f"off ({n_queued} queued!)" if n_queued else "off")
        )
        registry_label = registry_growth_status(cfg)
        capability_checks = head_capability_checks()
        identity_checks = _install_identity_checks()
        project_status = default_project_status(cfg)
        local_names = {n.name for n in cfg.nodes if n.local}
        drained_names = {n.name for n in cfg.nodes if n.drained}
        attached = False
        for r in rows:  # agent runs on the head itself -> its local node row
            if r["node"] in drained_names:
                r["checks"]["drained"] = "yes (nodes[].drained)"
            if r["node"] in local_names:
                r["checks"]["agent"] = agent_label
                r["checks"]["registry"] = registry_label
                r["checks"].update(capability_checks)
                r["checks"].update(identity_checks)
                r["checks"]["default_project"] = project_status
                if relay_status is not None:
                    r["checks"]["relay"] = relay_status
                attached = True
        if not attached:
            # A pure-orchestrator head (zero local nodes is a legal config)
            # still runs the agent and the relay; without a synthetic row a
            # dead agent, a backlogged queue, and a broken relay would all
            # be invisible and doctor would exit 0.
            checks: dict[str, str] = {
                "ssh": "ok",
                "agent": agent_label,
                "registry": registry_label,
                **capability_checks,
                **identity_checks,
                "default_project": project_status,
            }
            if relay_status is not None:
                checks["relay"] = relay_status
            rows.append({"node": "(head)", "checks": checks, "unreachable": False})
    else:
        # Computed once: every head is compared against this laptop's own
        # installed content, not merely its version number.
        local_identity = parse_version_identity(version_text())

        def check_head(item: tuple[str, str]) -> JsonDict:
            center, head = item
            proc = None
            detail = ""
            head_unreachable = False
            try:
                proc = _root.remote_dt(head, ["--version"], timeout=15)
            except Exception as exc:
                detail = " ".join(str(exc).split())
                head_unreachable = isinstance(
                    exc,
                    (RemoteError, OSError, subprocess.TimeoutExpired),
                )
            if proc is not None and proc.returncode != 0:
                detail = " ".join(
                    (
                        (proc.stderr or "").strip()
                        or (proc.stdout or "").strip()
                        or f"head version probe exited {proc.returncode}"
                    ).split()
                )
                head_unreachable = proc.returncode == 255
            ver = proc.stdout.strip() if proc and proc.returncode == 0 else "missing"
            checks: dict[str, str] = {
                "ssh": ("ok" if ver != "missing" else (detail or "fail")),
                "dt": (
                    ver.replace("dt ", "") or "missing"
                    if ver != "missing"
                    else ("unknown" if head_unreachable else "missing")
                ),
            }
            if ver != "missing":
                mismatch = _head_content_mismatch(
                    local_identity, parse_version_identity(ver)
                )
                if mismatch is not None:
                    checks["dt_content"] = mismatch
            return {
                "center": center,
                "node": f"{head} (head)",
                "checks": checks,
                "unreachable": head_unreachable,
            }

        def check_heads() -> list[JsonDict]:
            with ThreadPoolExecutor(
                max_workers=center_worker_count(len(cfg.centers))
            ) as pool:
                return list(pool.map(check_head, cfg.centers.items()))

        unreachable_errors: set[str] = set()
        # Version probes and full node diagnostics have no shared mutable
        # state. Start both together so an unreachable center costs one
        # timeout window rather than two consecutive windows.
        with ThreadPoolExecutor(max_workers=2) as pool:
            head_future = pool.submit(check_heads)
            node_future = pool.submit(
                _root.fan_json,
                cfg,
                ["doctor", "--rows-json"],
                120,
                accept_nonzero_json=True,
                unreachable_errors=unreachable_errors,
            )
            rows = head_future.result()
            node_rows, errors = node_future.result()
        rows += cast(list[JsonDict], node_rows)
        for center, e in errors.items():
            rows.append(
                {
                    "center": center,
                    "node": "(doctor failed)",
                    "checks": {"ssh": e},
                    "unreachable": center in unreachable_errors,
                }
            )
    ssh_failures = [row for row in rows if row["checks"].get("ssh") != "ok"]
    dependency_failure = any(
        str(row["checks"].get(key, "")).startswith("missing")
        for row in rows
        for key in _DOCTOR_DEPENDENCIES
    ) or any(
        # A present-but-broken driver reports "error: ..."; it must fail the
        # health check just like a missing dependency.
        str(row["checks"].get("gpu", "")).startswith("error")
        for row in rows
    )
    unreachable_failure = any(row.get("unreachable", True) for row in ssh_failures)
    nontransport_ssh_failure = any(
        row.get("unreachable") is False for row in ssh_failures
    )
    relay_failure = any(
        str(row["checks"].get("relay", "")).startswith("fail") for row in rows
    )
    agent_failure = any(
        str(row["checks"].get("agent", "")).startswith(("fail", "off")) for row in rows
    )
    lan_stale_nodes = [
        str(row["node"])
        for row in rows
        if str(row["checks"].get("lan", "")).startswith("stale")
    ]
    linger_failure = any(
        bool(str(row["checks"].get("gpu", "")))
        and not str(row["checks"].get("gpu", "")).startswith(("missing", "error"))
        and str(row["checks"].get("linger", "unavailable")) != "yes"
        for row in rows
    )
    hard_fail = (
        bool(ssh_failures)
        or dependency_failure
        or agent_failure
        or relay_failure
        or bool(lan_stale_nodes)
        or linger_failure
    )
    if unreachable_failure and not dependency_failure and not nontransport_ssh_failure:
        exit_code = EXIT_UNREACHABLE
    elif hard_fail:
        exit_code = 1
    else:
        exit_code = 0
    contract = _doctor_contract(rows, exit_code=exit_code)
    if rows_json:
        print(json.dumps(rows))
    elif json_:
        print(json.dumps(contract))
    else:
        _root.out.print(doctor_table(rows))
        for row in rows:
            mismatch = str(row.get("checks", {}).get("dt_content", ""))
            if mismatch.startswith("mismatch"):
                err.print(
                    f"[yellow]warning:[/yellow] {escape(str(row.get('node')))}: "
                    f"{escape(mismatch)}"
                )
        _render_doctor_actions(contract)
    raise typer.Exit(exit_code)

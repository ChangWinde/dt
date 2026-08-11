"""Machine-readable job path ownership and lifecycle contracts."""

from __future__ import annotations

from typing import Any, TypeAlias

from .config import HeadConfig
from .dispatch import artifact_root_rel
from .jobs import JobEntry
from .layout import ROLE_LAYOUT, job_control_dir, job_state_dir

JsonDict: TypeAlias = dict[str, Any]


def job_path_contract(cfg: HeadConfig, entry: JobEntry) -> JsonDict:
    """Describe ownership without remote probing or creating managed paths."""
    node = next((item for item in cfg.nodes if item.name == entry.node), None)
    control_root = job_control_dir(entry.job_dir, entry.storage_layout)
    state_root = job_state_dir(entry.job_dir, entry.storage_layout)
    output_root = f"{entry.job_dir}/outputs"
    results_root = cfg.results_root or cfg.head_root / "results"
    pull_destination = (
        results_root / "jobs" / entry.job_id
        if cfg.layout == ROLE_LAYOUT
        else results_root / entry.job_id
    )
    snapshot_root: str | None = None
    if entry.snapshot_sha256:
        current = (
            cfg.head_root / "snapshots" / "source" / entry.snapshot_sha256
            if cfg.layout == ROLE_LAYOUT
            else cfg.root / "snapshots" / entry.snapshot_sha256
        )
        legacy = cfg.root / "snapshots" / entry.snapshot_sha256
        snapshot_root = str(
            current
            if current.exists() or current == legacy or not legacy.exists()
            else legacy
        )
    environment_root = None
    cache_root = None
    artifact_root = None
    if node is not None:
        if entry.env_hash:
            environment_root = f"{cfg.envs_for(node)}/{entry.env_hash}"
        cache_root = cfg.cache_root_for(node)
        artifact_root = artifact_root_rel(entry.project, cfg, node)
    elif entry.worker_root:
        cache_root = f"{entry.worker_root}/worker/cache"
    return {
        "schema_version": "dt_job_paths_v1",
        "snapshot_root": {
            "path": snapshot_root,
            "scope": "head",
            "owner": "content_store",
            "mutable": False,
            "lifecycle": "shared by snapshot digest; retained while referenced",
            "cleanup": "snapshot garbage collection after registry retention",
        },
        "working_directory": {
            "path": f"{entry.job_dir}/code",
            "scope": f"worker:{entry.node}",
            "owner": entry.job_id,
            "mutable": True,
            "lifecycle": "private dispatched snapshot copy",
            "cleanup": "dt compact may remove code; dt clean removes the job",
        },
        "output_root": {
            "path": output_root,
            "scope": f"worker:{entry.node}",
            "owner": entry.job_id,
            "mutable": True,
            "lifecycle": "job-owned recoverable output",
            "cleanup": "dt clean after recovery",
        },
        "artifact_root": {
            "path": artifact_root,
            "scope": f"worker:{entry.node}",
            "owner": f"project:{entry.project}",
            "mutable": True if artifact_root else None,
            "bound_manifest": entry.artifact_manifest,
            "integrity": (
                "verified against the bound manifest before job start"
                if entry.artifact_manifest
                else "not content-bound for this job"
            ),
            "lifecycle": "persistent project input shared by explicit manifest",
            "cleanup": "explicit artifact retention; not removed by job cleanup",
        },
        "control_root": {
            "path": control_root,
            "scope": f"worker:{entry.node}",
            "owner": entry.job_id,
            "mutable": True,
            "lifecycle": "job control metadata",
            "cleanup": "dt clean removes the job",
        },
        "state_root": {
            "path": state_root,
            "scope": f"worker:{entry.node}",
            "owner": entry.job_id,
            "mutable": True,
            "lifecycle": "job lifecycle markers",
            "cleanup": "dt clean removes the job",
        },
        "environment": {
            "path": environment_root,
            "scope": f"worker:{entry.node}",
            "owner": f"env:{entry.env_hash}" if entry.env_hash else None,
            "mutable": True,
            "identity": entry.env_hash,
            "interpreter": (
                f"{environment_root}/bin/python" if environment_root else "python3"
            ),
            "source": "uv lock + setup identity" if entry.env_hash else "node PATH",
            "lifecycle": "shared reproducible environment identity",
            "cleanup": "age-based environment cleanup when unreferenced",
        },
        "cache_roots": [
            *(
                [
                    {
                        "path": (
                            f"{entry.cache_source_job_dir}/{entry.cache_source_path}"
                            if entry.cache_source_job_dir and entry.cache_source_path
                            else entry.cache_source_path
                        ),
                        "scope": f"worker:{entry.node}",
                        "owner": entry.cache_source_job,
                        "mutable": entry.cache_mode != "shared",
                        "lifecycle": f"explicit {entry.cache_mode or 'shared'} reuse",
                        "cleanup": (
                            "source retained while an active consumer references it"
                        ),
                    }
                ]
                if entry.cache_source_path
                else []
            ),
            *(
                [
                    {
                        "path": cache_root,
                        "scope": f"worker:{entry.node}",
                        "owner": "dt-runtime",
                        "mutable": True,
                        "lifecycle": "shared tool cache",
                        "cleanup": "storage maintenance policy",
                    }
                ]
                if cache_root
                else []
            ),
        ],
        "pull_destination": {
            "path": str(pull_destination),
            "scope": "head",
            "owner": entry.job_id,
            "mutable": True,
            "lifecycle": "managed recovered copy",
            "cleanup": "head results retention policy",
        },
    }

# Managed storage lifecycle — 2026-07-27

## Outcome

DT now gives pulled results an explicit managed-collection workflow, keeps
conventional local result trees out of code snapshots, inventories physical
storage across the head and compute nodes, and can preview or remove only
identity-verified managed pull directories.

No historical result or job directory was deleted during this change.

## Failure contract and root cause

Observed state:

- the DT source worktree contained 202 top-level experiment result directories
  occupying about 25 GiB;
- the head-managed result root occupied 2.3 GiB;
- `psibot-ds` retained 815 DT job directories occupying about 662 GiB;
- automatic retention was disabled (`auto_clean_days: null`);
- the documented batch pattern encouraged
  `dt pull ... --to results/<batch>`, which deliberately bypassed DT's already
  managed default result root;
- `/results/` was not a built-in snapshot exclusion, so caller-created result
  trees could be considered source payloads.

Expected state:

- named experiment groups should remain below one configurable DT result root;
- a caller-owned `--to` path should remain available but explicit;
- generated result trees must not enter normal code snapshots;
- storage use and retention candidates must be observable before deletion;
- cleanup must never infer ownership from a directory name alone.

## Implemented behavior

1. `paths.results` optionally places recovered outputs on a dedicated head-side
   data disk. The compatible default remains `paths.root/results`.
2. `dt pull REF... --collection NAME` always writes
   `<results>/collections/NAME/<job-id>/`. Absolute names, `..`, backslashes,
   empty names, and `--to` combinations fail before remote access.
3. `/results/` is a root-anchored built-in snapshot exclusion.
4. `dt storage [--json]` reports allocated bytes and immediate entry counts for
   head results/snapshots/cache/recovery/registry/queue plus every node's
   `dt/jobs` and configured environment root.
5. `dt clean --before DATE --results --envs --plan` provides a bounded preview.
   Removing `--plan` preserves the existing confirmation requirement.
   `--results` only selects a directory below the managed result root when its
   non-symlinked reserved `dt/job.json` names an eligible job.
6. `dt compact --before DATE [--plan] -y` productizes safe source-copy
   reclamation. It selects only old terminal jobs, verifies exact registry and
   path identity, rehashes every unique immutable snapshot before any node
   contact, rejects symlinks, removes only `code/`, and leaves an atomic
   `code-pruned.json` receipt. Repeated runs are idempotent.
7. DT's own `.gitignore` now covers local results, outputs, logs, coverage, and
   type/lint caches. Existing bytes remain recoverable but no longer flood
   normal Git status.

## Verification

| Surface | Evidence | Result |
|---|---|---|
| Regression suite | `uv run pytest -q` | 797 passed |
| Formatting/lint | targeted Ruff check and format check | passed |
| Dedicated result root | config regression | passed |
| Snapshot boundary | sync regression requires `/results/` exclusion | passed |
| Collection containment | managed-root, traversal, and `--to` conflict regressions | passed |
| Cleanup safety | plan-then-clean regression with owned and mismatched records | passed |
| Storage contract | `dt_storage_v1` regression | passed |
| Live managed pull | terminal proof job pulled with `--lite --collection canaries/managed-pull-20260727` | destination was `/home/psibot/dt/results/collections/.../<job-id>` |
| Live storage | all three nodes returned physical-byte inventories | passed |
| Live cleanup preview | cutoff `2026-07-22`, results + envs | 186 ended jobs, no deletion |
| Compaction regression | `tests/test_compact.py` | 8 passed |
| Full regression after compaction | `uv run pytest -q` | 805 passed |
| Live compaction plan | cutoff `2026-07-25` | 209 identity-verified jobs; 44 unique snapshots rehashed; zero failures |
| Live compaction replay | same cutoff, apply mode | 209 `already_compact`; zero mutations or failures |
| Lite-pull cache boundary | 1 MiB live `cache/derived.bin` canary | `report.json` recovered; `cache/` absent locally |

The first live storage canary intentionally failed the accuracy requirement:
apparent bytes counted hard links repeatedly and one node exceeded the
whole-probe timeout. The implementation was corrected to allocated bytes and
bounded per-directory scans before acceptance. The accepted live total was
about 715 GiB, dominated by `psibot-ds` job workdirs.

## Current storage matrix

| Scope | Entries | Allocated size |
|---|---:|---:|
| Head managed results | 64 | 2.3 GiB |
| Head immutable snapshots | 132 | 700 MiB |
| Head cache + recovery | 3 | 469 MiB |
| `psibot-hm` job dirs | 121 | 9.3 GiB |
| `psibot-hm` shared envs | 18 | 20 GiB |
| `psibot-ds` job dirs | 815 | 662 GiB |
| `psibot-ds` shared envs | 27 | 22 GiB |
| `psibot-ys` job dirs | 12 | 401 MiB |
| `psibot-ys` shared envs | 1 | 8 KiB |

The historical 25 GiB worktree `results/` tree is deliberately outside the
managed root because earlier commands explicitly selected it with `--to`.
It is now ignored and excluded from snapshots, but migration or deletion
requires an explicit operator retention decision.

## Deterministic cleanup execution

After explicit operator authorization, cleanup was limited to reproducible
state and verified duplicate source trees:

- permanently removed the worktree's mypy, Ruff, pytest, `__pycache__`, and
  bytecode caches (about 98 MiB);
- removed 36 shared environments last used before 2026-07-25 (15 on
  `psibot-hm`, 21 on `psibot-ds`); no process referenced those environments,
  and the active `af06ac2117d2` environment remained present on both nodes;
- ran `uv cache prune`, not cache purge. `psibot-ds`'s cache namespace fell
  from about 47 GiB to 11 GiB; hm/ys hit the bounded scan timeout and were left
  intact. Much of the removed cache content remained hard-linked from jobs or
  environments, so namespace reduction is not claimed as equivalent freed
  disk;
- selected 209 jobs older than 2026-07-25 only when all of these held:
  terminal status, safe `dt/jobs/<id>` path, 64-hex snapshot identity,
  matching snapshot `meta.json`, and an archived snapshot code directory;
- recomputed all 44 unique archived tree hashes before deletion; all 44
  matched their identities;
- removed only each selected job's redundant `code/` directory, preserving
  outputs, logs, checkpoints, launch payload, exit markers, and registry.
  Every successful operation wrote `code-pruned.json` beside the retained
  job data.

The removed code directories measured 21,150,543,872 bytes before deletion
(65 hm, 139 ds, 5 ys; zero failures). Because of cross-directory hard links,
the directly observed free-space increase was lower: about 7.6 GiB on hm and
5.0 GiB on ds, roughly 12.6 GiB total.

Recovery was tested through the public workflow. An exact fork of pruned
source job
`20260722-1942_launch-lock-fd-smoke-cpu_cc2f` restored snapshot
`dcc9789bd776...`, ran on hm without a GPU, printed
`PRUNED_CODE_EXACT_FORK_OK`, and finished exit 0 in 0.104 seconds.

The accepted procedure is now the public `dt compact` command rather than an
operator-only script. A live plan and apply replay at cutoff `2026-07-25`
selected the same 209 eligible jobs, rehashed all 44 unique archived snapshots
before node access, and reported every job as `already_compact`. There were no
preflight, registry, transport, path, or receipt failures. This proves the
command can safely resume after a completed or interrupted campaign without
turning historical cleanup into a broad recursive deletion.

Intentionally retained:

- the 25 GiB historical worktree result set and all managed results;
- every job's application outputs, logs, checkpoints, and registry entry;
- about 86.7 GB of job-owned `outputs/.cache` while another scientific job
  was live, avoiding a cache-consumer submission race;
- pip and Hugging Face caches, which remain useful on slow-network nodes.

## UO-35 cache follow-up

The UO-35 CPU/GPU split exposed a narrower retention bug after the original
audit: `dt pull --lite` excluded `.cache/` but not an ordinary derived
`cache/` directory. One managed collection therefore received
1,009,008,640 unnecessary bytes. The exact local cache and five exact remote
UO-35 cache directories were removed after the final report was hashed,
reclaiming 5,258,985,472 bytes while retaining plans, cache reports, job
records, logs, telemetry, and the scientific report.

`LITE_PULL_EXCLUDES` now includes both `.cache/` and `cache/`. The focused
regression failed before the change and passed afterward; the full suite
remained 805/805. A live proof job wrote `report.json` plus a 1 MiB
`cache/derived.bin`; the lite pull recovered the report and DT records while
the destination contained no `cache/`.

# DT evaluation closeout and cleanup — 2026-07-27

## Outcome

The bounded DT product goal is complete: SC-1 through SC-8 in
`docs/project/development-history.md` have
source, automated-test, and live-center evidence. Cleanup preserved the
reproducible experiment record while removing caches, duplicate payloads,
standalone smoke infrastructure, temporary fault-injection data, and
recoverable remote source copies.

This host has no `/home/starcosmos` account or directory. The actual DT
development package is `/home/psibot/cw/project/dt`; after cleanup it occupies
about 23 MiB and contains source, tests, documentation, configuration metadata,
and its reusable virtual environment. Experiment outputs no longer live below
the repository.

## Cleanup ledger

| Scope | Action | Verified result |
| --- | --- | --- |
| Repository caches | Removed 15 Torch cache trees, Python/test/lint/type/coverage caches, and pull fault-injection chunks. | Removed about 5.8 GiB of Torch cache, about 993 MiB of fault chunks, and small tool caches. |
| Duplicate checkpoint | Hashed the retained checkpoints and removed only the exact duplicate `epoch=6-step=10000.ckpt`. | 643,084,527 bytes removed; retained `last.ckpt` SHA-256 is `03201354f96bc77de082feeef0424482103688de10f576ef6bcf8bcb47a10325`. Other large checkpoints had distinct hashes and remain retained. |
| Repository results | Moved the complete ignored `results/` tree out of the code worktree. | 202 top-level result groups, 18,847,698,173 bytes, now below the managed closeout collection. |
| `dt-smoke` | Repointed the active DT default project to `dt`, deleted the standalone 8 KiB non-Git smoke project, and removed its historical jobs. | `/home/psibot/cw/project/dt-smoke` is absent; config has `default_project: dt`; 231 terminal `smoke` registry rows and job dirs were removed. |
| Project-scoped cleanup | Added a repeatable `dt clean --project/-p` selector so historical smoke work could be removed without touching OmniStack. | Focused tests passed 24/24; full DT suite passed 808/808. |
| Local temporary UO evidence | Archived unique legacy metadata/results before removing owned `/tmp/uo*` trees. | `legacy-uo-tmp-evidence-20260719-27.tar.zst`, SHA-256 `d40215e1e94fcd3b50ce0f6bdf30176825531e0c45536e5f5f276fff704229e1`; zstd validation passed. |
| Local temporary ER/GEN evidence | Proved duplicated checkpoints were already retained, archived only unique metadata, then removed the multi-GiB temporary trees. | `legacy-er-gen-tmp-metadata-20260719-27.tar.zst`, SHA-256 `df3692ded852c4b785e9bd4bd73b0c4bacbbad6a0298584a4a458e6fb442657a`; zstd validation passed. |
| Other owned temporary data | Removed old DT clones, installer residue, TorchInductor residue, test trees, logs, and this cleanup's temporary inventories. | No matching DT/UO/OmniStack/pytest cleanup targets remain in `/tmp`. |
| Remote recoverable code | Used fail-closed `dt compact`; every eligible immutable snapshot was rehashed before node mutation. | The three passes compacted 359 / 250 / 1 jobs and removed 19,808,018,432 / 18,804,682,752 / 83,017,728 bytes. Total exact recoverable code removed: 38,695,718,912 bytes; zero failures and zero registry damage. |
| `psibot-ds` | Removed recoverable code and project-scoped smoke job data while retaining active and scientifically meaningful outputs. | Job storage fell to 704,883,154,944 bytes across 845 directories. The remaining bulk is predominantly OmniStack experiment output, not deterministically classified garbage, so it was not deleted by date. |

## Retained evidence

The managed closeout collection is:

`/home/psibot/dt/results/collections/dt-evaluation-closeout-20260727`

It occupies 19,006,055,051 logical bytes and contains:

- `repo-results/`: all 202 previously worktree-local result groups, including
  dispatcher canaries, DP performance reports, UO evidence, telemetry, and
  distinct checkpoints;
- `legacy-uo-tmp-evidence-20260719-27.tar.zst`;
- `legacy-er-gen-tmp-metadata-20260719-27.tar.zst`.

The final UO-77 result was pulled separately to:

`/home/psibot/dt/results/collections/libero-universal-optimization/20260727-2015_uo77-shadow-matched-terminal-labels-egl-v1-20260727_bbe8`

This complete 17,617,378-byte result has report SHA-256
`54cf195a06ecff276575847f444f1273b8a53677c579014ddfac8bd738f29670`
and label-artifact SHA-256
`433e7b157b1d388ad1d4ae52c85f15ee36803a4a6b183859a00ebcdeb1a7f22e`.

Remote OmniStack job outputs were retained when their scientific value could
not be disproved. Broad `dt clean --before ...` was deliberately not used:
that command would delete outputs, logs, registry lineage, and potentially
unique checkpoints together.

## Remaining runtime item

There are 530 old `cursorsandbox` processes with PPID 1, zero observed CPU
activity, and 6,432 KiB total RSS. They are in a child user namespace and this
session has neither the required namespace capability nor passwordless sudo;
TERM, KILL, and namespace entry all fail with permission denied. Host
administration is required to remove them. They are not DT GPU consumers.

## UO-77 final closeout

UO-77
`20260727-2015_uo77-shadow-matched-terminal-labels-egl-v1-20260727_bbe8`
finished with exit 0 after 3,271.87 seconds. Its 300 simulator rollouts
produced 235 comparable terminal-label rows: 27 positive, 23 negative, 185
ties, and 5 skipped because the episode ended before anchor 128. Positive and
negative rows each covered 9 tasks. All five frozen coverage/count gates
passed, producing status `shadow_matched_terminal_signal_supported` and
decision `open_seed_generalized_gate_training`.

This is evidence to train and evaluate the next seed-generalized gate; it is
not a direct policy-quality promotion. The run made zero policy optimizer
updates. Final telemetry was 78.53% mean GPU utilization, 90.96% busy-sample
mean, 86.34% busy fraction, 100% peak utilization, 12,505 MiB peak VRAM, 58 C
peak temperature, 13,655 MiB peak anonymous PSS, and zero GPU errors. The
result was pulled before its 83,017,728-byte recoverable code copy was
compacted.

## Positive experimental results

### DT product

- The resident queue, completion wake, dependencies, and collision-safe GPU
  leases work end to end. A live successor started about 0.52 seconds after
  its predecessor; an exit-7 predecessor propagated to a successor as
  failed-before-start with `dt wait` exit 68.
- UO-30 validated one-process sequential DP/LIBERO evaluation with exact
  science parity, one checkpoint load, 1.186x conservative speedup, and
  9,287 MiB peak anonymous PSS.
- UO-31 validated the public persistent evaluation session: exact
  fingerprints, one checkpoint load, zero fallbacks, 9,308 MiB peak anonymous
  PSS, and an automatic CPU-preflight-to-GPU handoff in 2.693 seconds.
- UO-77 validated a shadow-matched terminal-label signal across 300 rollouts.
  All frozen label-coverage gates passed and opened seed-generalized gate
  training, while keeping the claim bounded to label support rather than
  policy improvement.
- Managed result collections, resumable/light pulls, storage inventory, exact
  snapshot and artifact identities, phase/resource telemetry, clock-domain
  labels, adaptive handoff states, and fail-closed compaction are now product
  capabilities rather than one-off procedures.
- Current verification is 808/808 DT tests plus passing Ruff check, Ruff
  format check, shell payload syntax, and `git diff --check`. `dt doctor`
  passes SSH/GPU/uv/tmux/rsync/flock/Python/timeout checks on all three nodes.

### Diffusion Policy training

| Candidate | Frozen comparison | Result | Decision |
| --- | --- | --- | --- |
| Batch 80 at 18k samples | Batch 72 versus 80 | 944.963471 to 952.943687 samples/s, +0.844500%; 0.75% gate passed | Promote for the exact workload |
| Disable channels-last at 1.32M samples | Channels-last versus contiguous | 983.568624 to 1,019.808145 samples/s, +3.684493%; complete duration -3.291899%; peak VRAM 22,945 MiB, peak 73 C, zero GPU errors | Promote for the exact long workload |
| Gradient-noise-scale callback confirmation | Baseline versus callback disabled | 994.958493 to 1,008.968349 samples/s, +1.408084%; duration regression 0.009198%; peak PSS 18,988.746 MiB, zero GPU errors | Keep default-off with explicit opt-in |

## Negative results that saved future GPU time

- UO-26 rejected expert-suffix alignment; UO-28 rejected recorded-state-guided
  completion; UO-29 rejected deterministic width two under the frozen memory
  limit.
- UO-68/UO-69 found an offline benefit signal, but UO-70 through UO-72 failed
  the no-task-regression runtime gate. UO-72's aggregate gain was not promoted
  because task 7 regressed from 8/10 to 5/10.
- UO-73/UO-74 passed their frozen offline gates, but UO-75 reduced
  support-matched runtime success from 16/20 to 12/20. The amplitude-only and
  terminal-utility sidecar families are closed without post-hoc tuning.
- These results do not establish a universal-policy success rate above 90%.
  That target remains outside the bounded DT product completion claim.

## Reusable technical lessons

1. Clean by identity and project, not only by age. Project selection prevents
   a smoke cleanup from erasing scientific workloads.
2. Compact source copies before deleting job records. Exact snapshot
   attestation makes large cleanup recoverable while logs, outputs, lineage,
   and checkpoints stay inspectable.
3. Pull into managed collections by default. Code repositories should contain
   software and concise audit records, not multi-GiB experiment trees.
4. A healthy empty queue is not a scheduler failure. Adaptive campaigns need a
   preregistered successor or an explicit `prepare/ready` handoff; filler work
   wastes GPU time.
5. Promotion requires frozen per-task and safety gates. Aggregate gains cannot
   override a declared regression rule.

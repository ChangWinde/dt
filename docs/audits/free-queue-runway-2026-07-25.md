# `dt free` queue-runway warning — 2026-07-25

## Observed gap

When one or more jobs were running but `queued=0`, human `dt free` rendered
either `active; all GPU capacity is occupied` or
`active; additional GPU capacity is available`. Both were locally true, but
neither warned that the current jobs had no successor and the queue would end
as soon as they finished.

This gap was visible during the real DP/LIBERO-10 optimization sequence: GPU
training itself ran at 97--99%, but a completed experiment branch could still
leave `running=0, queued=0` unless another meaningful job had already been
submitted.

Two UI tests first reproduced the missing behavior:

- all assigned capacity busy with one running job and no successor;
- one node busy while another node was already free.

Both failed against the old generic `active` messages. A second red assertion
then captured a semantic issue found by real use: recommending only the
currently free node conflated “use another GPU now” with “keep the current
training node busy after this job.”

## Contract

Human `dt free` now treats running work with `queued=0` as an exhausted queue
runway:

- if all capacity is occupied, it prints
  `queue ends after N running job(s)` and an executable
  `dt task NODE 'COMMAND' -n NAME` refill shape;
- if another GPU is free, it prints the immediate free-node submission and,
  when that node differs from the unique running node, a separate
  `keep busy` command for the running node;
- if old-head scheduler context lacks running-node identity, or several nodes
  are running, it uses the explicit `NODE` placeholder instead of guessing;
- queued, blocked, dead-agent, reserve, fragmentation, and idle explanations
  retain their existing precedence;
- public `dt free --json` remains the same resource array. The additive
  `running_nodes` field exists only in the internal human-view scheduler
  context and is backward compatible with older heads.

## Verification

Focused UX/probe/reliability verification passed 175 tests. The repository gate
after the first implementation passed 613 tests, Ruff, formatting, Python
compile, shell syntax, and `git diff --check`. The final dual-action refinement
was covered by three focused queue-runway tests. After the subsequent
health-ranked submit suggestion was integrated, the final repository gate
passed 614 tests plus all of the same static and shell checks.

Real `psibot-ds` acceptance used exact snapshot `51b163a02314...`:

1. `20260725-1244_dt-free-queue-runway-canary-20260725_4ac2`
   held GPU 0 at 100% in phase `gpu_canary` while the registry had
   `1 running, 0 queued`.
2. The 80-column view displayed the new queue-empty warning while identifying
   free capacity on `psibot-hm`.
3. Submitting
   `20260725-1244_dt-free-queue-runway-successor-20260725_e167`
   changed the same view to `1 running, 1 queued`, correctly explaining that
   free GPUs elsewhere were ineligible for the pinned `psibot-ds` successor.
4. Both jobs exited 0. The successor started 1.152 seconds after the canary
   finished, and their logs contained `QUEUE_RUNWAY_CANARY_OK` and
   `QUEUE_RUNWAY_SUCCESSOR_OK`.
5. Final refinement canary
   `20260725-1247_dt-free-queue-runway-dual-action-20260725_f1f3`
   ran at 100% and proved the final 80-column line contained both
   `submit: dt task psibot-hm ...` and
   `keep busy: dt task psibot-ds ...`; it exited 0.

After cleanup, the agent reported `0 running, 0 queued` and both dt-managed
cards were free. The warning is accepted.

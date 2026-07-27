# Exact-fork cache override audit — 2026-07-25

## Outcome

A real `dt fork --repeat 2 --reuse-cache` diagnostic exposed a contradiction:
the submission receipt recorded the requested shared cache, while the source
job's replayed command still contained dt's job-local cold-cache wrapper. At
runtime that wrapper reset `TORCHINDUCTOR_CACHE_DIR`, so the command and
provenance disagreed.

The invalid running job was terminated and the queued sibling dequeued before
using either as experimental evidence. A red regression reproduced the exact
source shape before the fix.

## Root cause and fix

`fork_spec_from_entry` replayed `entry.cmd` verbatim. A source job created by a
plain fork of a cache-bound job contains dt's owned `dt-cold-fork` wrapper but
does not itself carry cache provenance. Supplying a new explicit reuse binding
therefore added metadata without removing the older wrapper.

The dispatcher now:

- recognizes only dt's exact owned cold-wrapper marker and script shape;
- unwraps it when explicit cache reuse is requested without a command override;
- leaves arbitrary user `bash -c` commands untouched;
- accepts both `.cache/name` and `outputs/.cache/name`, normalizing either to
  the safe job-relative `outputs/...` form.

Focused unit and CLI integration tests cover both unwrapping and every item in
a repeat submission. The valid real rerun's registry command begins with the
training command, not `dt-cold-fork`, while both job receipts record the same
normalized cache provenance.

## Experimental finding

The corrected two-job run then rejected the shared-cache saturation hypothesis:
R1/R2 modified 637/405 files and retained 32.057/28.005 seconds of unmeasured
startup work. Shared writable reuse is valid for explicitly cumulative warm
workflows, but not for controlled repetitions. The next contract should clone
one verified source cache into a private writable directory for every job.

Evidence:

- protocol:
  `docs/experiments/EXP-DP-COMPILE-CACHE-SATURATION-20260725.md`;
- results:
  `results/dp-compile-cache-saturation-20260725/experiment-summary.json`.

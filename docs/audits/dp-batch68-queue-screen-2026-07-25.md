# DP batch-68 queue screen — 2026-07-25

## Outcome

The real DP/LIBERO-10 screen rejected batch 68 and retained batch 64. It also
accepted dt's exact-snapshot queue, resource contract, group wait, compare,
and compact pull paths on `psibot-ds`.

## Execution

- Pilot: `20260725-0930_dt-dp-batch68-pilot40-20260725_bdf9`.
- Formal A-B-B-A:
  - `20260725-0931_dt-dp-batch68-abba-a1-b64-100_ecda`;
  - `20260725-0932_dt-dp-batch68-abba-b1-b68-100_ffcb`;
  - `20260725-0932_dt-dp-batch68-abba-b2-b68-100_2cfe`;
  - `20260725-0932_dt-dp-batch68-abba-a2-b64-100_d6bf`.
- Exact snapshot:
  `fae000d60af4d06c7c466a48849a00bebba744ef5817aae29237ca0fe1db6f44`.
- Environment `6fb61a247969`; boot
  `968f7d0a-f045-46ce-8233-a6a84b20c5c9`.
- Every job required the resident dataset path, at least 20 GiB free disk,
  one pinned GPU, and a 0.25-hour runaway guard.

## Evidence

The pilot and all four arms exited zero. Peak GPU utilization was 99--100%;
peak VRAM was 20,601 MiB for batch 64 and 21,039 MiB for batch 68. Every
training receipt completed with zero NaN, Inf, exploding, contained, or
uncontained gradient events, and dt recorded zero GPU telemetry errors.

`dt compare` found all controls matched. Batch 64 averaged 797.9005 samples/s;
batch 68 averaged 795.6993 samples/s, a -0.2759% effect. The executable +1.0%
gate returned exit 1 as designed, representing scientific rejection rather
than infrastructure failure.

The agent dispatched all queued successors without operator action. Observed
handoff gaps were 1.147, 1.235, and 1.211 seconds. Group wait reported 4/4
successful processes. `dt pull --lite` recovered five compact result trees,
135 files and about 1.4 MiB total, to
`results/dp-batch68-screen-20260725/`.

## Operational conclusion

Queue dispatch kept the GPU occupied across the predeclared formal sequence,
and the new per-task disk contract was exercised on every job. The remaining
source of low whole-job utilization in very short probes is model/data
initialization and compilation, not queue handoff or a low-utilization
training loop.

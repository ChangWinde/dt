# Watch/task-follow interruption JSON audit — 2026-07-25

## Observed gap

`dt watch --json` streamed complete snapshots but ended with EOF and exit 0 on
Ctrl-C. `dt task -f --json` inherited that ambiguity: automation could not tell
whether monitoring reached a terminal state or detached, and the laptop path
could continue into terminal `wait`. The forwarding helper also collapsed a
head command's explicit exit 130 into the same `None` used for local
KeyboardInterrupt, preventing callers from knowing whether an interruption
object had already been emitted.

## Expected contract

1. Existing submission/watch JSON Lines remain complete.
2. Ctrl-C during watch appends one `watch_interrupted` object and exits 130.
3. The object states that jobs were not cancelled and contains exact resume
   watch plus explicit kill commands.
4. Task-follow does not enter wait after watch detaches.
5. Ctrl-C during task-follow's terminal wait emits `wait_interrupted` with the
   exact wait resume command.
6. Local Ctrl-C and a head-returned 130 remain distinguishable so laptop
   forwarding never duplicates an event or advances to the wrong phase.

## Red-capable reproduction and cause

Five focused tests covered head watch, laptop watch, laptop task watch-phase
detach, laptop task wait-phase detach, and the forwarding helper's remote-130
classification. All five failed before the fix:

- watch paths exited 0 without a final JSON object;
- task-follow printed only human stderr and exited 0;
- the helper returned `None` for a remote 130.

The causal defect was split between the catch boundary, which returned only a
boolean, and the forwarding boundary, which erased signal provenance.

## Causal fix

JSON watch interruption now uses one `_watch_interrupted` emitter. It preserves
refs, poll, lines, JSON mode, and completion policy in the resume command and
returns the stable error envelope with exit 130. Task-follow calls the same
emitter for local watch interruption and the existing `_wait_interrupted`
emitter for wait interruption. The forwarding helper treats a locally signalled
SSH process (`-SIGINT`) as local detach but preserves a remote exit 130.

## Evidence

- Red: all five focused tests failed with exit 0, missing JSON, or erased 130.
- Green: five new paths plus three adjacent human paths passed.
- Adjacent gates: 25 watch/forward-monitor tests and 11 task-follow tests.
- Real combined `psibot-ds` job:
  `20260725-0530_dt-task-follow-json-detach-accept-20260725_dbd8`.
  - `dt task -f --json` emitted submission and running frames;
  - parent-only SIGINT exited 130 in 0.265 seconds;
  - the last JSONL object was `watch_interrupted` with exact resume/kill;
  - immediate `dt info` still reported `running`;
  - the emitted watch command resumed to `finished`, exit 0;
  - terminal wait returned 0;
  - pull recovered `task-follow-proof.txt` containing `started` and `finished`
    plus job, stdout, lifecycle, resource, and telemetry records.

Durable evidence is under
`results/task-follow-json-detach-accept-20260725/`.

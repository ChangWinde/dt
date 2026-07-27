# Log-follow terminal audit — 2026-07-25

## Observed gap

`dt logs REF -f` always launched blocking `ssh -t tail -F`. It did not inspect
known terminal state or bind the tail lifecycle to the job wrapper. A real
finished job was still running its follower 1.5 seconds later and required
Ctrl-C; non-interactive use also printed
`Pseudo-terminal will not be allocated because stdin is not a terminal`.

When tail did return, dt returned the tail process code rather than the remote
training result. A failing job could therefore display an error yet make the
caller wait forever or eventually observe exit 0.

## Expected contract

1. A running job streams logs across transient SSH failures.
2. Wrapper termination drains the final bytes and wakes the follower within a
   bounded sub-second check interval.
3. The follower returns the same stable result as `dt wait`: training 0–125,
   killed 66, lost 67, or failed-before-start 68.
4. An already-terminal job prints the requested tail and returns immediately.
5. Ctrl-C remains detach-only and never cancels the job.
6. Non-interactive following does not request a remote PTY.

## Red-capable reproduction and cause

Two focused tests covered an already-finished exit-7 job and a running wrapper
whose tail process ended. Before the fix:

- the finished path incorrectly attempted to launch `tail -F`;
- the running path omitted wrapper PID binding and returned 0 without refreshing
  the job's exit 7.

The causal defect was the unconditional blocking tail subprocess and loss of job
lifecycle state at its return boundary.

## Causal fix

Running jobs with a valid wrapper PGID now use
`tail --pid=<pgid> -s 0.2 -n N -F PATH`. The remote SSH command does not request
a PTY. After tail exits, dt refreshes the authoritative job marker and maps its
terminal state through the same code contract as wait. If the PID disappears
just before the marker becomes visible, dt re-resolves instead of claiming
false success. Already-terminal jobs bypass `tail -F`, print the parsed tail,
and return immediately. Uncertain launch failures remain followable rather than
being treated as verified terminal state.

## Evidence

- Red: both focused tests failed before the fix.
- Green: both new tests plus four reconnect/Ctrl-C paths passed.
- Adjacent log gate: 35 passing tests.
- Real finished-job check returned 0 in 0.116 seconds and emitted no PTY warning.
- Real failing `psibot-ds` job
  `20260725-0536_dt-logs-terminal-exit7-accept-20260725_ddba`:
  - was followed from `running`;
  - printed `BEGIN` and `REMOTE_LOG_ROOT_CAUSE`;
  - returned training exit 7 in 2.152 seconds for a two-second task;
  - emitted `log stream complete · finished · exit 7`;
  - produced no PTY warning;
  - `dt info` independently reported finished/exit 7;
  - `dt pull` recovered matching stdout, lifecycle, resource, telemetry, and
    `log-proof.txt` evidence.

Durable evidence is under `results/logs-terminal-exit7-accept-20260725/`.

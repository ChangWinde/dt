# ADR 0011: Supervisor-owned runtime lifetimes

## Status

Accepted

## Context

`setsid`, `nohup`, and a separate tmux server detach terminal semantics but do
not escape a systemd service cgroup.  When DT is invoked by a host user service,
stopping that service can therefore kill the queue agent, tmux server, and jobs
while registry work remains queued.

## Candidates

| Candidate | Strengths | Weaknesses | Decision |
| --- | --- | --- | --- |
| Keep cron/nohup and add more polling | Portable | Does not change cgroup ownership; repeats the observed failure mechanism | Reject as primary |
| User systemd service for the agent and user scopes for runtime launch | Restart policy, explicit ownership, no root requirement, observable state | User manager/lingering availability varies by host | Choose with fallback |
| Privileged system service | Strong host lifetime | Requires root deployment and a multi-user authorization model beyond DT's current scope | Reject |

## Decision

`dt agent install` prefers a generated `systemd --user` unit with restart
policy when the user manager is usable.  Status reports the active supervisor,
unit state, and heartbeat age.  Existing crontab installation remains an
explicit compatibility fallback.

Remote job launch enters a dedicated user scope before creating the DT tmux
server when `systemd-run --user --scope` is usable.  The existing tmux launch is
the portable fallback.  Scope and session names are derived only from validated
job ids.

The registry remains authoritative.  Supervisor liveness is evidence about the
control plane, not evidence that a remote job finished.  Stale or absent agent
heartbeats are included in the shared scheduling diagnosis.

## Consequences

Hosts that need queue recovery across logout must enable user lingering.  DT
reports that condition rather than silently claiming persistence. Unit
generation, reload, enable, start, stop, and status are tested as separate
failure boundaries. An unavailable user manager selects the reported cron
fallback; a failed systemd installation is surfaced and leaves an existing
cron installation in place rather than silently claiming weaker supervision.

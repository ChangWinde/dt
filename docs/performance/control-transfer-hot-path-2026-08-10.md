# Control and verified-transfer hot paths — 2026-08-10

## Scope and claim boundary

This record covers local causal benchmarks for the uncommitted development
tree on `feat/intent-state-orchestration`. It is not a release, deployment, WAN
throughput, or live-cluster availability claim. All comparisons used the same
host, data, filesystem cache state, warmup count, and process environment.

The accepted target is at least a 1.5x speedup in each selected bottom-layer
hot path while preserving content identity, SSH trust, timeout behavior, and
bounded memory. A percentage is not generalized to commands dominated by
remote GPU drivers, WAN load, or SSH banner latency.

## SSH workload overlay

The profiler attributed the repeated local setup cost to directory creation,
permission repair, path parsing, stat calls, and rereading an unchanged
generated OpenSSH overlay. The candidate caches only a validated overlay
identity. Every hit still checks the root, workload socket directory, and
configuration inode/mode/timestamps, so replacement or a symlink invalidates
the cache and re-enters the fail-closed path.

Workload: 10 warmups and 10,000 `ssh_base(CONTROL)` calls in one process with
private temporary state and explicit user/system configuration files.

| Metric | Before | Candidate | Change |
|---|---:|---:|---:|
| Median | 0.020959 ms | 0.009208 ms | 2.28x; 56.1% less time |
| Mean | 0.022304 ms | 0.009333 ms | 2.39x |
| Standard deviation | 0.004535 ms | 0.000843 ms | lower variance |

The benchmark measures DT's local preparation, not network connection time.

## One-process configuration handoff

Every CLI begins an operation journal before dispatching the command. Both
boundaries need the same parsed configuration, but the old path parsed the
same YAML twice. The candidate reuses a parsed object only while device,
inode, mode, size, mtime, and ctime match; an atomic replacement reloads it.
The file descriptor is checked before and after parsing to reject an in-place
change during the read.

Workload: three warmups and 20 runs against the real development configuration.
The reference performs two complete safe-load/parse operations; the candidate
starts with an empty process cache, loads once, then performs the normal second
load.

| Metric | Before | Candidate | Change |
|---|---:|---:|---:|
| Median | 16.5066 ms | 8.1055 ms | 2.04x; 50.9% less time |
| Mean | 17.3638 ms | 8.1774 ms | 2.12x |
| Standard deviation | 1.9497 ms | 0.2281 ms | lower variance |

This removes duplicate parsing inside one DT process. It does not make the
first parse in a fresh Python process free.

## Verified artifact convergence

The prior known-digest path combined unconditional `rsync --checksum` with a
complete destination tree digest and, on a cold site-cache publish, repeated
the same cache digest after atomic rename. This read healthy trees more than
once. The candidate uses rsync's metadata fast path, performs one authoritative
tree digest, and retries exactly once with `--checksum` only after a proven
digest mismatch. A mismatch after repair fails closed.

Workload: a 128 MiB allocated file plus 4,096 one-KiB files in identical source
and destination trees; two warmups and seven measured dry-run convergence
passes. The reference performs checksum convergence plus two full digest
checks. The candidate performs quick convergence plus one full digest check.

| Metric | Before | Candidate | Change |
|---|---:|---:|---:|
| Median | 284.3000 ms | 151.9769 ms | 1.87x; 46.5% less time |
| Mean | 285.4487 ms | 152.1434 ms | 1.88x |
| Standard deviation | 3.8868 ms | 0.9765 ms | lower variance |

The 1.87x speedup exceeds the 1.5x performance target. Actual WAN throughput
is deliberately unclaimed; it depends on link bandwidth, RTT, and source churn.

## Reliability and resource behavior

- Concurrent candidate routes from the same seed to one destination share one
  direct-edge probe.
- One site/digest upload lock ends at atomic cache publication. Fan-out to
  different destinations then runs concurrently under per-destination locks;
  cache probe failures remain unknown instead of triggering another upload.
- Configured site-LAN rsync explicitly disables proxy routes and requires
  strict host-key verification, preventing a supposed P2P leg from silently
  re-entering the control-plane relay.
- Transport exceptions raised before a remote return code exists are normalized
  at the Artifact boundary. P2P selection can therefore try the next verified
  source, while cache uncertainty remains fail-closed.
- Rsync attempts run in a new process session. Timeout, cancellation, and
  keyboard interruption signal the complete rsync-plus-SSH process group,
  preventing stale relay traffic after DT has returned.
- Bounded control SSH and local probes use the same process-group cleanup, so
  an implicit ProxyJump/ProxyCommand helper cannot outlive its reported timeout.
- Authentication, host-key, permission, and space failures remain
  non-retryable. Partial files remain resumable for transport failures.
- Bulk transfer duration is separated from liveness: SSH connection setup and
  rsync IO stalls retain short bounds, while a progressing transfer has a
  four-hour safety ceiling. Hitting that ceiling is non-retryable, so DT does
  not repeat a multi-hour congested route automatically.
- The full Python 3.10.20 and 3.11.15 suites completed with 1,279 passing tests
  after the complete reliability review. The cross-site 1.203 GiB/12,316-file
  canary, zero-WAN second delivery, control-latency observation, and interrupted
  resume are recorded in the topology discovery report.

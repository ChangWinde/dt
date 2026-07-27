# Artifact preflight attribution — 2026-07-27

## Finding

Three consecutive UO-24/UO-25 submissions appeared to spend about 17 seconds
between queueing and start. Subtracting each recorded remote launch duration
left only 0.241–0.429 seconds of head-side dispatch response. The resident
queue wake path was healthy.

The remote launcher instead spent 14.660–15.187 seconds in its broad
`preflight` phase. Those jobs reused artifact manifest
`8090596132fed3aa3a4f6ee9378e048ae7f9c9f2c7728218f543388dcbad65f7`,
which bound roughly 43 GiB across eight inputs. UO-22 through UO-25 actually
read only the 711,715,594-byte task07 HDF5 from that inventory.

## Safe correction

`dt sync --artifact` published a one-file manifest,
`0c0bfaed5b47a6876eda4d7a4cc2b8e65fe09e8a41f8818a7657b257b060f8df`,
without retransferring the already-identical file. A CPU-only public
`dt task` canary bound that manifest and retained the same path, mode, size,
and SHA-256 verification contract.

Job `20260727-0721_dt-narrow-artifact-preflight-bench-v1-20260727_bbfe`
exited 0. Its artifact-bound preflight took 0.345 seconds and complete remote
launch took 2.542 seconds, versus 15.187 and 16.533 seconds for the broad
UO-25 production binding. This is about a 44x preflight reduction without
caching or weakening content verification.

The project-code sync path was also exercised directly:

- the actual incremental sync transferred 60 files / 647,929 bytes in
  0.137 seconds;
- the immediately repeated `--plan` reported zero files and zero bytes,
  proving convergence.

## Observability repair

Launcher results now include
`launch_phases_ms.artifact_verification`. The dispatcher converts and retains
it as `launch_phases_s.artifact_verification`, and human `dt info` renders the
phase as `artifact verify`. `preflight` remains the enclosing compatibility
duration, so historical consumers and old registry rows remain valid.

The post-repair public canary
`20260727-0723_dt-artifact-phase-observability-canary-v1-20260727_77ff`
exited 0 and reported 0.353 seconds for artifact verification, 0.374 seconds
for enclosing preflight, and 1.422 seconds for the full remote launch. Its
human `dt info` displayed `artifact verify 353 ms` on the prepare-phases row.

The operational rule is to bind the smallest complete artifact inventory that
the command can read. A broad manifest remains valid and secure, but its
repeated byte verification is visible instead of being mistaken for queue
latency.

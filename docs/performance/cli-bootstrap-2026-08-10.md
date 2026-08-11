# Audited CLI bootstrap benchmark — 2026-08-10

## Scope and claim boundary

This record measures only the exact local `dt --version` path on the
uncommitted `feat/intent-state-orchestration` development tree. It validates
the lightweight console boundary from ADR 0019; it does not claim that remote
commands, SSH connections, topology discovery, or the complete CLI became 50%
faster.

Both variants used the same Python 3.11 interpreter, source tree, missing
configuration, private temporary state directory, operation-journal writes,
warmup count, output redirection, and host. The reference imported
`dt.cli:main`; the candidate imported `dt.entrypoint:main`. Three warmups
preceded 30 latency samples. Peak RSS is the mean and median of ten independent
GNU `time` samples.

## Result

| Metric | Full CLI reference | Audited bootstrap | Change |
|---|---:|---:|---:|
| Median latency | 79.177 ms | 31.456 ms | 2.52x; 60.3% less time |
| p95 latency | 83.289 ms | 35.038 ms | 57.9% less time |
| Mean peak RSS | 30,624 KiB | 21,760 KiB | 28.9% less memory |
| Median peak RSS | 30,682 KiB | 21,760 KiB | 29.1% less memory |

The candidate exceeds the predeclared 1.5x latency threshold. The operation
journal contains both `start` and `finish` events for the fast path, and its
output uses the shared build-identity renderer also used by the Typer callback.

A post-hardening recheck on 2026-08-11, after the complete reliability and
security review, measured 32.231 ms median, 33.118 ms p95, and 22,016 KiB mean
and median peak RSS with the same 3-warmup/30-latency/10-RSS protocol. This is
within 2.5% of the recorded candidate median and retains a 2.46x improvement
over the full-CLI reference; the hardening pass introduced no material fast-
path regression.

A final convergence recheck after centralizing descriptor-bound state I/O used
the same protocol and measured 32.140 ms median, 32.895 ms p95, 31.823 ms
mean, 21,734 KiB mean peak RSS, and 21,760 KiB median peak RSS. The result is
within 2.2% of the original audited median, retains the 2.46x latency
improvement, and confirms that the final hardening did not trade away the fast
path.

The release-quality recheck on 2026-08-11, after destructive-cleanup and
deployment hardening, measured 31.495 ms median, 32.960 ms p95, 31.518 ms
mean, 21,862 KiB mean peak RSS, and 22,016 KiB median peak RSS with the same
protocol. A discarded run used the unsupported `DT_CONFIG_PATH` name and
therefore parsed the operator's real configuration; the recorded run uses the
documented `DT_CONFIG` override and a missing isolated configuration, as the
original benchmark contract requires.

## Interpretation

The result supports lazy command loading as the next Python-side optimization.
It does not support a Rust rewrite by itself: the removed cost is eager module
import and command graph construction, not a demonstrated language-runtime
bottleneck. Native helpers remain an option for a future transport or hashing
profile that proves CPU or memory pressure after the Python boundaries are
small.

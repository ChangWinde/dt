# Verified installation benchmark — 2026-08-11

## Scope and method

This measures the ADR 0023 release bootstrap on the reference workstation. It
does not measure dependency downloads or claim cold-network installation
latency. The release wheel and fully hashed requirements were built first, the
uv cache was warm, and installation ran offline with Python 3.11 into an empty
temporary root.

The first measurement includes relocatable environment creation, mandatory-hash
dependency installation, audited-wheel installation, `uv pip check`, version
validation, content-addressed publication, and atomic command activation. The
reuse measurement invokes the same verified bundle after publication; three
warmups preceded 30 latency samples. GNU `time` recorded peak RSS for ten
additional reuse processes. The temporary installation was measured with
`du -sk` and deleted after the run.

## Result

| Metric | Result |
|---|---:|
| Warm-cache first installation | 263.160 ms |
| Existing-identity reuse median | 142.826 ms |
| Existing-identity reuse p95 | 374.596 ms |
| Existing-identity reuse mean | 174.013 ms |
| Reuse mean peak RSS | 25,083.6 KiB |
| Reuse median peak RSS | 25,088.0 KiB |
| One Python 3.11 installation identity | 12,712 KiB |

## Interpretation

The stronger dependency and activation boundary remains subsecond with a warm
cache and adds about 12.4 MiB per retained Python/wheel/requirements identity.
Reuse deliberately reruns dependency consistency and command validation rather
than trusting the receipt alone; that safety check dominates its roughly
143 ms median. Installation is a release/rollback operation, not a command hot
path, so this cost does not affect the separately measured 31.495 ms median
`dt --version` path.

The p95 spread reflects process scheduling and repeated uv environment checks
on the shared workstation. No claim is made for cold indexes, uncached Python
interpreters, other filesystems, or remote deployment latency; those remain
part of the authorized live-head canary.

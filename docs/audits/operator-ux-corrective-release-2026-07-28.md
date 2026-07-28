# DistTrainer 0.6.2 operator-UX corrective release audit

## Verdict

**PASS** for the compact-reference and cross-version `ps` repair scope.

The final clean release commit must rerun `scripts/release-check.sh`; the
resulting retained `release-manifest.json` is the authoritative artifact and
source identity.

## Corrected findings

- Compact references are computed against the complete head registry. Four
  characters remain the common case; only collisions expand.
- Newly submitted jobs use a 64-bit random suffix while historical
  four-character job ids remain resolvable.
- Ambiguous partial ids fail closed and list safe candidate refs instead of
  silently selecting the newest matching experiment.
- New multi-center rows use `CENTER:REF`. Rows from pre-v2 heads use full job
  ids because those old resolvers cannot consume scoped references safely.
- `dt_ps_window_v2` binds every response to its status, active, issues,
  progress, and limit query. Invalid or undersized windows are rejected.
- New clients explicitly negotiate v2. Unnegotiated responses remain v1
  supersets so both 0.6.0 and 0.6.1 clients can reproduce their own view.
- A v1/unsupported-head fallback fetches the complete registry before applying
  active, status, issues, recent, or limit selection locally.
- Issue totals survive per-center and global windowing; `--issues --limit N`
  returns the requested number whenever that many matching jobs exist.

## Verification

- Python 3.10: `832 passed`.
- Python 3.11: `832 passed`.
- Ruff, Ruff format, strict Mypy boundary checks, Bash syntax, lock validation,
  and `git diff --check`: pass.
- Deterministic wheel/sdist double build, release-content audit, SBOM, exact
  runtime constraints, clean wheel install, and isolated bootstrap: pass.
- Release-content audit found zero secret markers, internal references, or
  absolute local paths.
- Real registry: 956 jobs, 956 unique generated refs, all 956 resolved to the
  intended job; 26 colliding four-character refs expanded to at most six
  characters.
- Real compact-ref computation took 64 ms for 956 jobs.
- Real ambiguous lookup exited 4 and listed both safe candidates.
- Real 80-column active view stayed within 79 columns.

## Compatibility and residual limits

- Default `dt ps --json` remains the complete array; new derived
  `display_ref` data are additive.
- v2-to-v2 human windows remain bounded. Compatibility with old heads is exact
  but intentionally transfers full history.
- Full job ids duplicated across centers fail as ambiguous rather than choosing
  the preferred center.
- Hosted CI remains unavailable because this checkout has no Git remote. The
  complete supported Python matrix was executed locally.
- Publication and deployment remain explicit promotion actions; this audit
  does not upload a package or mutate a configured head.

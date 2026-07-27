# EXP-DT-PYPI-FALLBACK-MANISKILL-GATE-20260727

## Outcome

Accepted. A real OmniStack/ManiSkill deployment on `psibot-hm` exposed and
closed three dt reliability gaps:

1. direct PyPI TLS failure prevented environment creation;
2. project setup did not stop after its first failed command; and
3. Python imports could write `__pycache__` into bound shared artifacts.

The final current contract installs the complete environment, imports
CleanDiffuser and ManiSkill, passes the GPU hardware suite and a real
100-episode PushCube GPU simulator gate, and reduces repeated warm-environment
preparation from 28.577 seconds to 0.309 seconds.

## Causal evidence

The original hm jobs failed before start while resolving build requirements
from `https://pypi.org/simple/`:

- `20260726-2333_dt-gradient-noise-source-maniskill-gate-hm-20260726_08fb`
- `20260726-2335_dt-gradient-noise-source-maniskill-gate-hm-retry-20260726_d7d7`

Both exhausted three retries with `tls handshake eof`. Direct PyPI also failed
in an empty-cache reproduction, while the Aliyun and Tsinghua HTTPS mirrors
were reachable. The same OmniStack lock synced successfully through Aliyun,
isolating the failure to the primary network path rather than dependency
resolution.

The first fallback implementation crossed the environment boundary but the
project setup hook later made its own direct PyPI request. It also revealed
that a multi-command setup script could return zero after an earlier command
failed, causing an invalid persistent success marker.

## Implemented contract

- Retry once only when the captured uv failure names direct PyPI and contains
  a network error signature.
- Probe an allowlist of Aliyun and Tsinghua HTTPS mirrors; never fall through
  from arbitrary/private indexes.
- Preserve the selected fallback for the project setup hook.
- Cache a successful mirror hint for six hours. A hint is accepted only when
  its exact URL is allowlisted and a fresh HTTPS probe succeeds.
- Execute setup through `bash -e`; write the setup marker only after zero exit.
- Explicitly use `set -e` in the active OmniStack dt setup contract, producing
  a new setup/environment identity and repairing the stale marker.
- Export `PYTHONDONTWRITEBYTECODE=1` for manifest-bound runner trees so Python
  cannot poison shared content-addressed artifacts with `__pycache__`.

Deterministic dependency failures are not retried.

## Real validation matrix

| Proof | Job | Result | Environment / launch |
| --- | --- | --- | ---: |
| setup + imports | `20260727-0036_dt-omnistack-hm-setup-failfast-real-proof-20260727_cedf` | CleanDiffuser 0.1.0, ManiSkill 3.0.1, exit 0, setup ran | 29.364 s / 29.530 s |
| current GPU gate | `20260727-0038_dt-maniskill-gpu-gate-hm-current-contract-20260727_bb2f` | 8 passed in 6.67 s, exit 0 | 31.262 s / 31.828 s |
| mirror-hint seed | `20260727-0040_dt-hm-pypi-mirror-hint-seed-real-20260727_843e` | imports passed, exit 0 | 28.577 s / 28.726 s |
| mirror-hint warm | `20260727-0041_dt-hm-pypi-mirror-hint-warm-real-20260727_00e1` | cached hint logged, imports passed, exit 0 | **0.309 s / 0.470 s** |
| PushCube initial | `20260727-0059_dt-maniskill-pushcube-gpu-oracle100-hm-20260727_132d` | first GPU step exposed direct CUDA Tensor → NumPy conversion, exit 1 | 0.712 s / 1.309 s |
| PushCube repaired | `20260727-0101_dt-maniskill-pushcube-gpu-oracle100-hm-cudatensor-fix-20260727_de1d` | **100/100 success**, mean 13.06 steps, exit 0 | 0.319 s / 0.886 s |

The hint reduced environment time by 98.918711% (92.48x) and total launch
time by 98.363742% (61.12x) under the same snapshot, environment, node, and
command.

The final GPU gate used snapshot
`e7f004dd5b971466834ba454fc8763e29555188b7191dd88451f61a97d71ae15`
and environment `af06ac2117d2`.

The first real PushCube run then crossed a boundary the import and vector-GPU
tests did not: ManiSkill's GPU backend returns reward, termination, truncation,
and success values as CUDA tensors. The oracle and demonstration collector now
normalize one device value through `detach().cpu().numpy()` before converting
it to a Python scalar and reject non-scalar inputs. Ten targeted tests pass.
The current-code rerun used snapshot
`746c70903df98ee8cdd5b3bb34776f9097a201e40e8e2b8797326ecfe99b71e1`,
completed 100 episodes in 25.965 seconds, achieved 1.0 success rate, and
recorded zero GPU errors, 2,468 MiB peak VRAM, 46 C peak temperature, and
89.285714% busy samples.

## Regression gates

- Full dt suite: 744 passed.
- OmniStack oracle/collector regression suite: 10 passed; lock check passed.
- Focused fallback coverage includes direct network retry, cached-hint reuse,
  setup inheritance, warm-setup preflight, deterministic-failure no-retry,
  and invalid-wheel cache repair.
- Wrapper coverage proves a bound Python module import creates no
  `__pycache__`.
- Ruff, Bash syntax, and `git diff --check` pass.

Machine-readable evidence:
`results/dt-pypi-fallback-maniskill-gate-20260727/validation-summary.json`.

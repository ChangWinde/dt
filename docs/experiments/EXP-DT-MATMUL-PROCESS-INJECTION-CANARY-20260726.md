# EXP-DT-MATMUL-PROCESS-INJECTION-CANARY-20260726

## Trigger and decision

The first matmul-precision A/B launched successfully, but both recovered
runtime records said `matmul_precision=high`. The parent runner monkeypatch
did not cross the campaign's `omnistack-train` subprocess boundary, so that
experiment is invalid and cannot answer the performance question.

Decision: verify a process-start hook before authorizing any repaired training
protocol.

## Frozen protocol

- One CPU-only dt task on `psibot-ds`.
- Bind a directory containing `sitecustomize.py` and `run.py`.
- The parent starts a fresh child Python process with the bound directory
  prepended to `PYTHONPATH` and requests `medium` through an experiment-only
  environment variable.
- The child asks OmniStack's backend arbiter for `high`.
- Pass only if both the arbiter evidence and
  `torch.get_float32_matmul_precision()` report `medium`, proving the
  process-start hook overrode the later training request.
- Stop after one task. Pass permits a separately frozen repaired A/B; failure
  closes the candidate.

## Reproducibility

- Artifact:
  `outputs/dt-matmul-process-injection-canary-20260726/`.
- Planned output:
  `results/dt-matmul-process-injection-canary-20260726/`.
- Source hashes:
  `sitecustomize.py=29fd396eaa16712db05884d3c5a916b709072ad4147db39d7b7eef6dc19d606a`,
  `run.py=50a102b434b6dd84159cf74bb4676c6327d6e180d099301381af96180b8a4a7b`.
- Artifact manifest:
  `184d7c956d87b31b06605e97d72dd97b3b7ef35a5cdef9092363436ce574f540`.
- Status: COMPLETE — FAIL.

## Outcome

Job `20260726-2153_dt-matmul-process-injection-canary-20260726_5e84`
finished in 2.567603 seconds with exit 1. The child imported the hook, but the
CPU-only backend evidence omitted the `matmul_precision` key and the probe
raised `KeyError` before it could emit the required two-part proof.

The artifact sync also correctly warned that directory selection included two
locally generated Python 3.13 `__pycache__` files. They were content-addressed
and ignored by remote Python 3.10, but future multi-file artifacts should name
their source files individually.

The frozen gate required both arbiter evidence and Torch current-state
evidence. It did not pass. Per the stopping rule, do not modify or rerun this
canary; close the medium-precision candidate and retain `high`.

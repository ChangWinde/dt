# CUDA probe diagnostics and payload identity — 2026-07-26

## Problems

The node-side CUDA allocation probe had two related reliability gaps:

1. its ctypes driver boundary was untyped, leaving 18 strict-mypy findings and
   making function signatures or cleanup behavior easy to change accidentally;
2. launcher redirected the complete probe stderr stream to `/dev/null`, so
   allocation, free, context-destroy, and timeout failures collapsed into a
   generic node-unfit message.

The real GPU canary then exposed a separate reproducibility gap. Job
`20260726-1259_dt-cuda-probe-typed-canary-20260726_5e37` used changed launcher
and CUDA-probe files but retained the same `snapshot_sha256` as the earlier
telemetry canary. This is correct for the documented project-code snapshot,
but no second identity recorded which dt runtime actually executed the job.

## Repair

- `_CudaFunction` and `_CudaDriver` protocols now define every CUDA driver
  symbol, argument contract, return code, and call boundary used by the probe.
- Allocation cleanup preserves the exact `cuMemFree_v2` or
  `cuCtxDestroy_v2` error code instead of reporting a generic failure.
- Launcher captures probe diagnostics, keeps the advisory return-42 fallback
  silent, names the 120-second timeout, and forwards a bounded last error line
  for every other failure.
- Submission freezes the seven static node-runtime files once:
  `launcher.sh`, `wrapper.sh`, `cuda_probe.py`, `telemetry.py`, `phase.sh`,
  `snapshot_hash.py`, and `artifact_verify.py`.
- `payload_sha256` hashes a versioned, path-sensitive manifest of those exact
  contents. It is independent of project `snapshot_sha256`, command, setup,
  and environment identities.
- Direct launch, queued staging, failover, failed-before-start records,
  cancellation-race records, submission JSON, `dt info`, and recovered
  `dt/job.json` all retain the same payload identity.
- Queued jobs verify the staged runtime before capacity probing. Mutation after
  submission fails before a GPU can be leased.
- For every identified direct or queued launch, the head sends the standard-
  library verifier source inline over SSH with the registry identity. It
  re-hashes the seven compute-node files before executing launcher, environment
  sync, setup, cache/artifact validation, or GPU probing. The verifier is not
  loaded from the untrusted job bundle, so changing launcher cannot bypass it.
- A mismatch returns internal launcher code 17, classified as
  `payload-integrity`, with both expected and observed hashes. It is fatal
  before failover because the frozen job contract is already broken.
- Successful verification time is retained as
  `launch_phases_s.payload_attestation` and rendered as the `payload` prepare
  phase. Legacy jobs with no identity skip the new gate.
- `dt compare` treats missing or different payload identity as a required
  control mismatch. Historical registry rows remain loadable; their value is
  `null` and is deliberately not claimed comparable to identified jobs.

The project snapshot contract and content-addressed project cache remain
unchanged. A queued job still freezes both project code and runtime at submit
time, while each identity can be interpreted independently.

This is an integrity check against transfer corruption and unexpected
compute-node file mutation, not a trust proof for a hostile node administrator.
A privileged node actor can alter the SSH command or files after verification;
that threat requires a separate signed/isolated execution design.

## Tests and compatibility

Focused tests cover:

- CUDA init, device, context, allocation, free, and destroy error propagation;
- launcher preservation of the final CUDA diagnostic and explicit timeout;
- deterministic, ordering-independent, path- and content-sensitive payload
  hashing;
- use of an already frozen runtime mapping by support-file generation;
- direct and queued registry propagation;
- rejection and cleanup of a mutated staged payload before capacity probing;
- JSON and human `info` visibility;
- compare acceptance for matching identities and rejection for runtime drift;
- legacy registry and existing submission-receipt compatibility.
- trusted remote verification before launcher side effects, exact mismatch
  diagnostics, fatal dispatcher classification, legacy-bundle execution, and
  payload-attestation phase timing;
- suppression of unrelated `logs/env.log` reads for payload-integrity failures
  across wait, info, and smart log selection, while env-fail behavior remains
  unchanged.

The final focused gate passed 5/5 and the complete repository gate passed
697/697. The earlier 11-test payload-identity gate remains covered by that
complete suite.

## Real psibot-ds evidence

The post-repair task used the public dispatcher path:

```bash
dt run -g 1 -n dt-payload-identity-canary-20260726 \
  -p smoke --node psibot-ds --max-hours 0.05 -- \
  bash -lc 'mkdir -p "$DT_JOB_DIR/outputs";
    printf "%s\n" "payload identity canary" \
      > "$DT_JOB_DIR/outputs/payload-identity.txt";
    sleep 3'
```

Job `20260726-1311_dt-payload-identity-canary-20260726_a6bf`:

- exited 0 after 3.093885 seconds;
- passed real CUDA context/allocation/free/destroy probing in 305 ms;
- used GPU 0 on `psibot-ds`;
- retained project snapshot
  `dcc9789bd7766b1c7a41a3ec6565f7161c6841b80775c317f2fbf390675fbb7d`;
- retained payload
  `f9113ed5881e7d74f2c279a4cffcce9c49b8b5043c4a90438367056fedcec632`;
- produced three valid resource samples and zero GPU telemetry errors;
- recovered its application artifact and reserved diagnostics to
  `results/dt-payload-identity-canary-20260726/`.

The locally computed payload hash exactly matched submission JSON,
`dt info --json`, human `dt info`, and recovered `dt/job.json`.

Comparison against the pre-feature CUDA canary demonstrated the original
reproduction directly: project, snapshot, center, node, GPU, boot, and disk
controls matched, while `payload_sha256` was `null` for the old job and the
new exact digest for the repaired job. `controls_match` was therefore false.

## Compute-node attestation evidence

Ten successful public `dt run` launches on `psibot-ds` reported payload
attestation between 18 and 37 milliseconds, with a 25-millisecond median. The
phase is separately visible from launcher `remote_total`, while head
`launch_duration_s` continues to cover the whole remote operation.

The negative user journey used only public dt jobs:

1. a bounded one-GPU blocker forced the target into the resident FIFO queue;
2. a zero-GPU watcher on the same node waited for the target directory and
   continuously overwrote its `telemetry.py`;
3. after capacity released, the agent transferred the frozen target and the
   head-supplied verifier checked the compute-node bytes.

The first two injection attempts were deliberately not counted: a one-shot
write was overwritten by rsync, then a watcher that waited for every file
arrived after PGID publication. The final watcher performed 870,943 writes,
recorded in
`results/dt-payload-attestation-fault4-watcher-20260726/write-count.txt`.

Target `20260726-1329_dt-payload-attest-fault4-target-20260726_fc3d` failed
before start with:

- expected payload
  `5dcec1e5749ec945d224db61772d77e76b3eb16d7fabf1214fdf9e5879116abd`;
- observed payload
  `3064c6e62b1cd6ef42d3c6f5a0317a4a657fb12d6015026463b7fe6b96e34828`;
- `started_at:null`, no GPU ids, no boot id, no output directory, and stable
  wait exit 68;
- no launcher/env/setup/GPU-probe side effects.

That real failure exposed one UX issue: prestart monitoring assumed every
placed failure owned `logs/env.log`. Wait and info therefore added an unrelated
missing-log warning. Failure classification now reads that log only for
`env-fail`; the same target re-rendered with the payload root cause alone and
no `failure_log` field.

## Terminal verdict

PASS for the bounded CUDA-probe and payload-identity milestone:

- Ruff format and lint: passed on all changed Python surfaces;
- launcher shell syntax: passed;
- focused mypy for dispatcher, registry, and CUDA probe: zero errors;
- focused correctness: 5 passed after the attestation/diagnostic repair;
- full regression: 697 passed;
- real normal launch plus queued remote-mutation failure injection: passed;
- real `wait → info → pull` failure-diagnostic journey: passed;
- final queue state: agent alive, zero queued, zero running, GPU lease released.

Repository-wide mypy reports 135 existing errors in three files. No global
type-clean or release-readiness claim is made.

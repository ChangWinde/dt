# Explicit artifact sync implementation plan

1. Add red tests for safe path validation, exact file/directory transfer,
   read-only planning, CLI routing, and runtime environment propagation.
2. Implement a separately located artifact root without changing code snapshot
   exclusions or snapshot hashes.
3. Run focused and full gates.
4. Stage a small proof artifact to `psibot-ds`, consume it from a real `dt`
   job, then stage only the frozen UO-05 checkpoints.

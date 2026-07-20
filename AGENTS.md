# dt — GPU experiment dispatcher (agent cheatsheet)

dt submits experiments to whichever shared GPU is free in this center.
Everything is machine-readable: add `--json` to any command; exit codes are
stable (0 ok, 2 no free GPU with `--no-queue`, 3 env failure, 4 not found,
5 unreachable).

## Submit and follow

```bash
dt run -g 2 -n exp42 -- python train.py --lr 3e-4
# stdout LAST LINE is the bare job id; progress goes to stderr
dt wait exp42          # blocks; exits with the training process's exit code
dt logs exp42 -n 200   # tail the log (add -f to follow)
```

Typical closed loop: `id=$(dt run -g 2 -n sweep1 -- python train.py | tail -1)`
then `dt wait "$id"`; on non-zero exit read `dt logs "$id"`, fix, resubmit.

## Queueing (default behavior)

When no card is free, `dt run` queues the job (exit 0, job id still printed)
and the head-node agent dispatches it FIFO once capacity frees up. The code
snapshot is taken at submit time — editing the project afterwards does not
change what a queued job will run.

- `dt wait <id>` covers the queued phase too: it blocks through queue →
  running → finished and still exits with the training exit code. From a
  laptop it auto-reconnects if the ssh link drops (the job is unaffected).
- `dt run --no-queue ...` restores fail-fast: exit 2 when nothing is free.
- `dt kill <id> -y` on a queued job removes it from the queue.
- `dt ps` shows queued jobs; `dt agent status --json` gives queue depth.
- A job blocked for job-specific reasons (missing `--require-path`, unfit
  nodes) does NOT hold up the queue behind it; capacity waits stay FIFO.
- `dt rerun <id>` resubmits a past job (same cmd/GPUs/pins, current code) -
  the fix-and-retry primitive after a failure.
- `dt wait` exit codes: 0-125 job's own, 65 not found, 66 killed, 67 lost,
  68 failed-before-start (env failure at dispatch; reason in `dt ps --json`).

## Rules

- Always pass `-n <meaningful-name>`; names are how humans find your runs.
- Write checkpoints/artifacts to `$DT_JOB_DIR/outputs/` so `dt pull` finds them.
- `dt kill <job> -y` (non-interactive kill requires `-y`). Kill verifies death;
  if the group survives TERM it says so (exit 1) - rerun with `--force`.
- `dt pull` resumes interrupted transfers (--partial + retries); on failure
  just rerun it.
- Long jobs: add `--max-hours N` as a runaway guard.
- Disable progress bars in training scripts (`TQDM_DISABLE=1`) to keep logs sane.
- Check capacity first with `dt free --json` when planning multiple submissions.
- Do not bypass dt with raw ssh/nvidia-smi juggling; dt already handles
  collision-safe GPU selection, env sync (uv), snapshots, and logging.

## All commands

```
dt free [--watch] [--who]   free GPUs across nodes (--who: who holds busy cards)
dt run [-g N] [-n NAME] [-c CENTER|auto] [-p PROJECT] [--node NODE]
       [--require-path P] [--max-hours H] [--no-queue] -- CMD...
dt ps [-s STATUS] [-a] jobs + live status (table: last 30 unless -a;
                       --json: always everything)
dt info REF [--json]   one job's full picture: state, node, gpus, timeline,
                       duration, exit code, outputs size, git, reason
dt logs REF [-f] [-n N]
dt attach REF          enter the job's tmux (C-b d to detach)
dt wait REF [--poll S]
dt rerun REF [-n NAME] [--no-queue]   resubmit with current code
dt pull REF [--to DIR] fetch outputs/ back to this head (resumes/retries)
dt kill REF... [-y] [--force]   running job: TERM (KILL with --force) the
                       group, confirms death; queued job: dequeue
dt clean --before YYYY-MM-DD [--envs] [-y]   old jobs (+stale shared venvs)
dt seed NODE... [--hf] push uv cache/pythons (+HF models) to slow-net nodes
dt doctor              verify ssh/gpu/uv/tmux/net(+speed)/agent on all nodes
dt agent status|start|stop|run|install    queue agent lifecycle
```

Prefer `dt info <id> --json` over parsing `dt ps` when inspecting one job.

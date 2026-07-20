#!/usr/bin/env bash
# Runs as the tmux pane process, which tmux already makes a session/group
# leader: $$ IS the process group id `dt kill` needs. Never setsid here --
# that would move the training process out of this group and break kill.
set -u

echo $$ > "$DT_JOB_DIR/pgid"
date +%s > "$DT_JOB_DIR/started_at"

cd "$DT_JOB_DIR/code"

runner=(bash "$DT_JOB_DIR/cmd.sh")
if [ -n "${DT_UV_ENV:-}" ]; then
    export UV_PROJECT_ENVIRONMENT="$DT_UV_ENV"
    export UV_PYTHON_PREFERENCE=only-managed
    runner=("$DT_UV" run --no-sync bash "$DT_JOB_DIR/cmd.sh")
fi

if [ -n "${DT_MAX_HOURS:-}" ]; then
    timeout --signal=TERM --kill-after=60 "${DT_MAX_HOURS}h" "${runner[@]}"
else
    "${runner[@]}"
fi
rc=$?

echo $rc > "$DT_JOB_DIR/exit_code"
date +%s > "$DT_JOB_DIR/finished_at"

# job-end webhook (best effort, never fails the job). Not reached on
# `dt kill` (TERM takes this shell down too) - kills are user-initiated.
if [ -n "${DT_WEBHOOK:-}" ]; then
    dur=$(( $(date +%s) - $(cat "$DT_JOB_DIR/started_at" 2>/dev/null || date +%s) ))
    curl -m 10 -s -o /dev/null -X POST -H 'Content-Type: application/json' \
        -d "{\"event\":\"finished\",\"job_id\":\"${DT_JOB_ID:-}\",\"name\":\"${DT_JOB_NAME:-}\",\"center\":\"${DT_CENTER:-}\",\"node\":\"$(hostname)\",\"exit_code\":$rc,\"duration_s\":$dur}" \
        "$DT_WEBHOOK" || true
fi
exit $rc

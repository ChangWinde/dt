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
exit $rc

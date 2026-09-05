# shellcheck shell=sh
# Compute apps per GPU with their owning user, command name, and memory. All
# unique PIDs are resolved in one ps call; process-heavy nodes otherwise pay
# one remote fork per row.
# Output per app: gpu_uuid,pid,user,comm,used_mib
#   comm is the executable basename as `ps -o comm=` prints it (commas and
#   blanks replaced by _ so the row stays a CSV record); used_mib is empty when
#   the driver reports no per-process figure ([N/A] inside containers).
dt_app_raw=$(nvidia-smi --query-compute-apps=gpu_uuid,pid,used_memory --format=csv,noheader,nounits 2>&1); dt_app_rc=$?
if [ "$dt_app_rc" -ne 0 ]; then
  echo ---DT-APP-ERROR---; printf '%s\n' "$dt_app_raw"
else
  dt_app_pids=$(printf '%s\n' "$dt_app_raw" | awk -F, '{ p=$2; gsub(/[[:space:]]/, "", p); if (p ~ /^[0-9]+$/ && !seen[p]++) { if (out != "") out=out ","; out=out p } } END { print out }')
  dt_app_users=
  if [ -n "$dt_app_pids" ]; then
    # user gets an explicit width: procps clips a middle column to 8 chars
    # ("starcos+") and only lets the last column grow.
    dt_app_users=$(ps -o pid=,user:64=,comm= -p "$dt_app_pids" 2>/dev/null)
  fi
  { printf '%s\n' "$dt_app_users"; echo ---DT-APP-ROWS---; printf '%s\n' "$dt_app_raw"; } | awk '$0 == "---DT-APP-ROWS---" { rows=1; next } !rows { if ($1 == "") next; pid=$1; users[pid]=$2; name=$0; sub(/^[[:space:]]*[^[:space:]]+[[:space:]]+[^[:space:]]+[[:space:]]*/, "", name); gsub(/[,[:space:]]+/, "_", name); comms[pid]=name; next } { split($0, f, ","); gsub(/[[:space:]]/, "", f[1]); gsub(/[[:space:]]/, "", f[2]); gsub(/[[:space:]]/, "", f[3]); if (f[1] == "") next; if (f[2] != "") { key=f[1] SUBSEP f[2]; if (seen[key]++) next } u=(f[2] in users && users[f[2]] != "") ? users[f[2]] : "?"; c=(f[2] in comms) ? comms[f[2]] : ""; m=(f[3] ~ /^[0-9]+$/) ? f[3] : ""; print f[1] "," f[2] "," u "," c "," m }'
fi

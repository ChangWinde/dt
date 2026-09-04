# shellcheck shell=sh
# Compute apps per GPU with their owning user. All unique PIDs are resolved in
# one ps call; process-heavy nodes otherwise pay one remote fork per row.
# Output per app: gpu_uuid,pid,user
dt_app_raw=$(nvidia-smi --query-compute-apps=gpu_uuid,pid --format=csv,noheader 2>&1); dt_app_rc=$?
if [ "$dt_app_rc" -ne 0 ]; then
  echo ---DT-APP-ERROR---; printf '%s\n' "$dt_app_raw"
else
  dt_app_pids=$(printf '%s\n' "$dt_app_raw" | awk -F, '{ p=$2; gsub(/[[:space:]]/, "", p); if (p ~ /^[0-9]+$/ && !seen[p]++) { if (out != "") out=out ","; out=out p } } END { print out }')
  dt_app_users=
  if [ -n "$dt_app_pids" ]; then
    dt_app_users=$(ps -o pid=,user= -p "$dt_app_pids" 2>/dev/null)
  fi
  { printf '%s\n' "$dt_app_users"; echo ---DT-APP-ROWS---; printf '%s\n' "$dt_app_raw"; } | awk '$0 == "---DT-APP-ROWS---" { rows=1; next } !rows { users[$1]=$2; next } { split($0, f, ","); gsub(/[[:space:]]/, "", f[1]); gsub(/[[:space:]]/, "", f[2]); if (f[1] == "") next; if (f[2] != "") { key=f[1] SUBSEP f[2]; if (seen[key]++) next } u=(f[2] in users && users[f[2]] != "") ? users[f[2]] : "?"; print f[1] "," f[2] "," u }'
fi

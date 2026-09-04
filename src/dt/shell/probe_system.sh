# shellcheck shell=sh
# Host capacity in one line: cores,load1,mem_total_kib,mem_avail_kib,disk_total_kib,disk_avail_kib,io_pressure_avg10
cores=$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 0)
load1=$(awk '{print $1}' /proc/loadavg 2>/dev/null || echo 0)
mem=$(awk '
  /^MemTotal:/ {total=$2}
  /^MemAvailable:/ {avail=$2}
  END {printf "%d %d", total, avail}
' /proc/meminfo 2>/dev/null)
mem_total=${mem%% *}; mem_avail=${mem##* }
disk=$(df -Pk "$HOME" 2>/dev/null | awk 'NR==2 {print $2, $4}')
disk_total=${disk%% *}; disk_avail=${disk##* }
io=$(awk '
  /^some / {
    for (i=1; i<=NF; i++) if ($i ~ /^avg10=/) {
      split($i, v, "="); print v[2]; exit
    }
  }
' /proc/pressure/io 2>/dev/null)
printf '%s,%s,%s,%s,%s,%s,%s\n' \
  "${cores:-0}" "${load1:-0}" "${mem_total:-0}" "${mem_avail:-0}" \
  "${disk_total:-0}" "${disk_avail:-0}" "${io:--1}"

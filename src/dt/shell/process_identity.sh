# shellcheck shell=sh
# POSIX sh helpers proving a PID still belongs to one dt job.
# Loaded by dt.lifecycle.process_identity_shell(); sourced into remote probes.

dt_pid_ticks() {
  dt_pt_line=$(cat "/proc/$1/stat" 2>/dev/null) || return 1;
  dt_pt_tail=${dt_pt_line##*) };
  [ "$dt_pt_tail" != "$dt_pt_line" ] || return 1;
  # shellcheck disable=SC2086  # split /proc/<pid>/stat fields into positional parameters
  set -- $dt_pt_tail; [ "$#" -ge 20 ] || return 1;
  case "${20}" in *[!0-9]*|"") return 1;; esac;
  printf '%s\n' "${20}"; };
dt_pid_group() {
  dt_pg_line=$(cat "/proc/$1/stat" 2>/dev/null) || return 1;
  dt_pg_tail=${dt_pg_line##*) };
  [ "$dt_pg_tail" != "$dt_pg_line" ] || return 1;
  # shellcheck disable=SC2086  # split /proc/<pid>/stat fields into positional parameters
  set -- $dt_pg_tail; [ "$#" -ge 3 ] || return 1;
  case "${3}" in *[!0-9]*|"") return 1;; esac;
  printf '%s\n' "${3}"; };
dt_pid_state() {
  dt_ps_line=$(cat "/proc/$1/stat" 2>/dev/null) || return 1;
  dt_ps_tail=${dt_ps_line##*) };
  [ "$dt_ps_tail" != "$dt_ps_line" ] || return 1;
  # shellcheck disable=SC2086  # split /proc/<pid>/stat fields into positional parameters
  set -- $dt_ps_tail; [ "$#" -ge 1 ] || return 1;
  printf '%s\n' "${1}"; };
dt_pid_has_live_task() { dt_pht_seen=0;
  for dt_pht_path in "/proc/$1/task/"[0-9]*; do
    [ -e "$dt_pht_path" ] || continue; dt_pht_seen=1;
    dt_pht_tid=${dt_pht_path##*/};
    # An unreadable extant task is not proof of death. Fail toward live
    # so maintenance never deletes a capsule under an uncheckable thread.
    dt_pht_state=$(dt_pid_state "$dt_pht_tid") || return 0;
    case "$dt_pht_state" in Z|X|x) :;; *) return 0;; esac; done;
  [ "$dt_pht_seen" -eq 1 ] && return 1; return 0; };
dt_pid_zombie() {
  dt_pz_st=$(dt_pid_state "$1") || return 1;
  case "$dt_pz_st" in Z|X|x) dt_pid_has_live_task "$1" && return 1; return 0;;
    *) return 1;; esac; };
dt_pid_cwd_owned() {
  dt_pc_cwd=$(readlink "/proc/$1/cwd" 2>/dev/null) || return 1;
  case "$dt_pc_cwd" in "$2"|"$2"/*) return 0;; *) return 1;; esac; };
dt_process_owned() {
  dt_po_pid=$1; dt_po_identity=$2; dt_po_job=$3; dt_po_boot=$4;
  case "$dt_po_pid" in *[!0-9]*|""|0) return 1;; esac;
  kill -0 "$dt_po_pid" 2>/dev/null || return 1;
  # An unreaped zombie passes kill -0 and keeps matching start ticks
  # forever, but it exited: nothing it owned can still run under it.
  # Reporting it live would pin kill at ALIVE, refresh at RUNNING, and
  # the completion watcher in a busy loop with no state to advance.
  if dt_pid_zombie "$dt_po_pid"; then return 1; fi;
  if [ -n "$dt_po_boot" ]; then
    dt_po_current_boot=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null) || return 2;
    [ "$dt_po_current_boot" = "$dt_po_boot" ] || return 2; fi;
  if [ -e "$dt_po_identity" ] || [ -L "$dt_po_identity" ]; then
    [ -f "$dt_po_identity" ] && [ ! -L "$dt_po_identity" ] || return 2;
    dt_po_size=$(wc -c <"$dt_po_identity" 2>/dev/null) || return 2;
    case "$dt_po_size" in *[!0-9]*|"") return 2;; esac;
    [ "$dt_po_size" -gt 0 ] && [ "$dt_po_size" -le 64 ] || return 2;
    dt_po_expected=$(cat "$dt_po_identity" 2>/dev/null) || return 2;
    case "$dt_po_expected" in *[!0-9]*|"") return 2;; esac;
    dt_po_actual=$(dt_pid_ticks "$dt_po_pid") || {
      kill -0 "$dt_po_pid" 2>/dev/null && return 2; return 1; };
    [ "$dt_po_actual" = "$dt_po_expected" ] && return 0; return 2; fi;
  case "$dt_po_job" in /*) :;; *) dt_po_job=$(readlink -f -- "$dt_po_job" 2>/dev/null) || return 2;; esac;
  dt_pid_cwd_owned "$dt_po_pid" "$dt_po_job" && return 0;
  kill -0 "$dt_po_pid" 2>/dev/null && return 2; return 1; };

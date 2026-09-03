# shellcheck shell=sh
# Fail-closed helpers for a job's recorded systemd user scope.
# Loaded by dt.lifecycle.runtime_scope_shell(); requires process_identity.sh.

dt_scope_marker() {
  dt_sm_path="$1/runtime_scope"; dt_sm_expected=$2;
  [ -e "$dt_sm_path" ] || [ -L "$dt_sm_path" ] || return 1;
  [ -f "$dt_sm_path" ] && [ ! -L "$dt_sm_path" ] || return 2;
  dt_sm_size=$(wc -c <"$dt_sm_path" 2>/dev/null) || return 2;
  case "$dt_sm_size" in *[!0-9]*|"") return 2;; esac;
  [ "$dt_sm_size" -gt 0 ] && [ "$dt_sm_size" -le 64 ] || return 2;
  dt_sm_value=$(cat "$dt_sm_path" 2>/dev/null) || return 2;
  [ "${#dt_sm_value}" -eq 37 ] || return 2;
  case "$dt_sm_value" in dt-runtime-[0-9a-f]*.scope) :;; *) return 2;; esac;
  dt_sm_hex=${dt_sm_value#dt-runtime-}; dt_sm_hex=${dt_sm_hex%.scope};
  [ "${#dt_sm_hex}" -eq 20 ] || return 2;
  case "$dt_sm_hex" in *[!0-9a-f]*) return 2;; esac;
  [ -z "$dt_sm_expected" ] || [ "$dt_sm_value" = "$dt_sm_expected" ] || return 2; printf "%s\n" "$dt_sm_value"; };
dt_containment_marker() {
  dt_cm_path="$1/runtime_containment";
  [ -e "$dt_cm_path" ] || [ -L "$dt_cm_path" ] || return 1;
  [ -f "$dt_cm_path" ] && [ ! -L "$dt_cm_path" ] || return 2;
  dt_cm_size=$(wc -c <"$dt_cm_path" 2>/dev/null) || return 2;
  case "$dt_cm_size" in *[!0-9]*|"") return 2;; esac;
  [ "$dt_cm_size" -gt 0 ] && [ "$dt_cm_size" -le 64 ] || return 2;
  dt_cm_value=$(cat "$dt_cm_path" 2>/dev/null) || return 2;
  case "$dt_cm_value" in systemd_scope_pending|systemd_scope_verified|portable_unproven) printf "%s\n" "$dt_cm_value";;
    *) return 2;; esac; };
dt_requested_gpus() {
  dt_rg_path="$1/runtime_gpus_requested";
  if [ -e "$dt_rg_path" ] || [ -L "$dt_rg_path" ]; then
    [ -f "$dt_rg_path" ] && [ ! -L "$dt_rg_path" ] || return 2;
    dt_rg_size=$(wc -c <"$dt_rg_path" 2>/dev/null) || return 2;
    case "$dt_rg_size" in *[!0-9]*|"") return 2;; esac;
    [ "$dt_rg_size" -gt 0 ] && [ "$dt_rg_size" -le 16 ] || return 2;
    dt_rg_value=$(cat "$dt_rg_path" 2>/dev/null) || return 2;
    case "$dt_rg_value" in *[!0-9]*|"") return 2;; esac;
    printf "%s\n" "$dt_rg_value"; return 0; fi;
  dt_rg_path="$1/gpus";
  [ -e "$dt_rg_path" ] || [ -L "$dt_rg_path" ] || { echo 0; return 0; };
  [ -f "$dt_rg_path" ] && [ ! -L "$dt_rg_path" ] || return 2;
  dt_rg_size=$(wc -c <"$dt_rg_path" 2>/dev/null) || return 2;
  case "$dt_rg_size" in *[!0-9]*|"") return 2;; esac;
  [ "$dt_rg_size" -le 1024 ] || return 2;
  dt_rg_value=$(cat "$dt_rg_path" 2>/dev/null) || return 2;
  [ -z "$dt_rg_value" ] && { echo 0; return 0; };
  case "$dt_rg_value" in *[!0-9,]*|,*|*,|*,,*) return 2;; esac;
  echo 1; };
dt_gpu_containment_unproven() {
  # A job without either new marker predates containment attestation.
  # Preserve its existing lifecycle semantics; new launchers always
  # publish runtime_gpus_requested before starting a session.
  dt_gc_requested="$1/runtime_gpus_requested";
  if [ ! -e "$dt_gc_requested" ] && [ ! -L "$dt_gc_requested" ]; then
    dt_gc_value=$(dt_containment_marker "$1"); dt_gc_rc=$?;
    [ "$dt_gc_rc" -eq 1 ] && return 1;
    [ "$dt_gc_rc" -eq 0 ] || return 2; fi;
  dt_gc_count=$(dt_requested_gpus "$1"); dt_gc_rc=$?;
  [ "$dt_gc_rc" -eq 0 ] || return 2;
  [ "$dt_gc_count" -gt 0 ] 2>/dev/null || return 1;
  dt_gc_value=$(dt_containment_marker "$1"); dt_gc_rc=$?;
  [ "$dt_gc_rc" -eq 0 ] || return 0;
  [ "$dt_gc_value" = systemd_scope_verified ] || return 0;
  dt_scope_marker "$1" "" >/dev/null 2>&1; dt_gc_rc=$?;
  [ "$dt_gc_rc" -eq 0 ] && return 1; return 0; };
dt_scope_census() {
  dt_sc_unit=$1; command -v systemctl >/dev/null 2>&1 || { echo DEGRADED; return 0; };
  dt_sc_load=$(systemctl --user show "$dt_sc_unit" --property=LoadState --value 2>/dev/null) || { echo DEGRADED; return 0; };
  case "$dt_sc_load" in not-found|"") echo ABSENT; return 0;; esac;
  dt_sc_active=$(systemctl --user show "$dt_sc_unit" --property=ActiveState --value 2>/dev/null) || { echo DEGRADED; return 0; };
  dt_sc_cg=$(systemctl --user show "$dt_sc_unit" --property=ControlGroup --value 2>/dev/null) || { echo DEGRADED; return 0; };
  if [ -z "$dt_sc_cg" ]; then case "$dt_sc_active" in
      inactive|failed|dead) echo ABSENT;; *) echo DEGRADED;; esac; return 0; fi;
  case "$dt_sc_cg" in /*) :;; *) echo DEGRADED; return 0;; esac;
  case "/$dt_sc_cg/" in */../*|*/./*) echo DEGRADED; return 0;; esac;
  dt_sc_root="/sys/fs/cgroup$dt_sc_cg";
  [ -d "$dt_sc_root" ] || { echo DEGRADED; return 0; };
  dt_sc_raw=$(find "$dt_sc_root" -type f -name cgroup.procs -exec cat -- {} + 2>/dev/null); dt_sc_rc=$?;
  [ "$dt_sc_rc" -eq 0 ] || { echo DEGRADED; return 0; };
  for dt_sc_pid in $dt_sc_raw; do
    case "$dt_sc_pid" in *[!0-9]*|""|0) echo DEGRADED; return 0;; esac;
  done; echo OK; for dt_sc_pid in $dt_sc_raw; do
    if dt_pid_zombie "$dt_sc_pid"; then continue; fi;
    [ -e "/proc/$dt_sc_pid" ] || continue;
    printf "%s\n" "$dt_sc_pid"; done; };

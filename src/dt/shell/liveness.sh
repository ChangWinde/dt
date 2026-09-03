# shellcheck shell=sh
# dt_job_live_state JOB_DIR PGID BOOT_ID IDENTITY_FILE -> LIVE | DEAD | UNPROVEN.
# Loaded by dt.lifecycle.liveness_shell(); requires process_identity.sh and runtime_scope.sh.

dt_job_live_state() {
  dt_jl_jd=$1; dt_jl_pg=$2; dt_jl_boot=$3; dt_jl_ident=$4;
  case "$dt_jl_jd" in /*) :;; *) dt_jl_jd="$PWD/$dt_jl_jd";; esac;
  # find -lname treats its operand as a glob: a configured path holding
  # [ ] * ? \ would silently match nothing and report a live job DEAD.
  # Escape the metacharacters; without sed, use the literal readlink
  # walk instead of the find fast path.
  dt_jl_pat=$(printf '%s\n' "$dt_jl_jd" | sed 's/[][\*?]/\\&/g' 2>/dev/null) || dt_jl_pat=;
  case "$dt_jl_pg" in *[!0-9]*|"") dt_jl_pg=0;; esac;
  if [ -n "$dt_jl_boot" ]; then
    dt_jl_cur=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null) || { echo UNPROVEN; return 0; };
    [ "$dt_jl_cur" = "$dt_jl_boot" ] || { echo DEAD; return 0; }; fi;
  dt_jl_state=${dt_jl_ident%/*};
  dt_gpu_containment_unproven "$dt_jl_state"; dt_jl_gcrc=$?;
  [ "$dt_jl_gcrc" -eq 1 ] || { echo UNPROVEN; return 0; };
  dt_jl_scope=$(dt_scope_marker "$dt_jl_state" ""); dt_jl_src=$?;
  [ "$dt_jl_src" -eq 2 ] && { echo UNPROVEN; return 0; };
  if [ "$dt_jl_src" -eq 0 ]; then
    dt_jl_sc=$(dt_scope_census "$dt_jl_scope");
    dt_jl_shead=$(printf "%s\n" "$dt_jl_sc" | sed -n "1p");
    case "$dt_jl_shead" in DEGRADED) echo UNPROVEN; return 0;;
      OK) [ "$(printf "%s\n" "$dt_jl_sc" | sed -n "2p")" ] && { echo LIVE; return 0; };; esac; fi;
  dt_process_owned "$dt_jl_pg" "$dt_jl_ident" "$dt_jl_jd" ""; dt_jl_rc=$?;
  if [ "$dt_jl_rc" -eq 0 ] || [ "$dt_jl_rc" -eq 2 ]; then
    echo LIVE; return 0; fi;
  dt_jl_deg=0; dt_jl_open=0;
  if [ "$dt_jl_pg" -gt 0 ]; then
    if [ ! -e "/proc/$dt_jl_pg" ]; then dt_jl_open=1;
    elif dt_pid_zombie "$dt_jl_pg"; then
        dt_jl_zpg=$(dt_pid_group "$dt_jl_pg") && [ "$dt_jl_zpg" = "$dt_jl_pg" ] && dt_jl_open=1; fi; fi;
    if [ "$dt_jl_open" -eq 1 ]; then
      dt_jl_gp=$(pgrep -g "$dt_jl_pg" 2>/dev/null); dt_jl_grc=$?;
      [ "$dt_jl_grc" -gt 1 ] && dt_jl_deg=1;
      for dt_jl_x in $dt_jl_gp; do
        if dt_pid_zombie "$dt_jl_x"; then continue; fi;
        [ -e "/proc/$dt_jl_x" ] || continue;
        echo LIVE; return 0; done; fi;
    if command -v find >/dev/null 2>&1 && [ -n "$dt_jl_pat" ]; then
      dt_jl_cwd=$(find /proc -mindepth 2 -maxdepth 2 -type l -name cwd \( -lname "$dt_jl_pat" -o -lname "$dt_jl_pat/*" \) -printf '%h\n' 2>/dev/null); dt_jl_frc=$?;
      [ "$dt_jl_frc" -gt 1 ] && dt_jl_deg=1;
      [ -n "$dt_jl_cwd" ] && { echo LIVE; return 0; };
    else for dt_jl_p in /proc/[0-9]*; do
        case "$(readlink "$dt_jl_p/cwd" 2>/dev/null)" in "$dt_jl_jd"|"$dt_jl_jd"/*) echo LIVE; return 0;; esac; done; fi;
    [ "$dt_jl_deg" -eq 1 ] && { echo UNPROVEN; return 0; };
    echo DEAD; };

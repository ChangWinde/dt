# shellcheck shell=sh
# Helpers for the termination probe: group_open, sig_scan, survivors.
# Loaded by dt.lifecycle.termination_probe(); requires process_identity.sh and runtime_scope.sh.

group_open() {
  [ "$DT_KPG" -gt 0 ] || return 1;
  [ ! -e "/proc/$DT_KPG" ] &&
  { command -v pgrep >/dev/null 2>&1 || return 1;
    pgrep -g "$DT_KPG" >/dev/null 2>&1; return $?; };
  dt_pid_zombie "$DT_KPG" || return 1;
  dt_go_pg=$(dt_pid_group "$DT_KPG") || return 1;
  [ "$dt_go_pg" = "$DT_KPG" ] || return 1;
  if [ -f "$DT_KIDENT" ] && [ ! -L "$DT_KIDENT" ]; then
    dt_go_exp=$(cat "$DT_KIDENT" 2>/dev/null) || return 1;
    case "$dt_go_exp" in *[!0-9]*|"") return 1;; esac;
    dt_go_act=$(dt_pid_ticks "$DT_KPG") || return 1;
    [ "$dt_go_act" = "$dt_go_exp" ] || return 1; fi; return 0; };
# The signal targets and the survivor census are deliberately
# different sets. A live-but-unproven leader (rc=2) means the PGID may
# belong to a reused, unrelated group, so its in-group members must
# never be *signalled*; but a process whose cwd is inside our private
# capsule is almost certainly ours (foreign reuse cannot land there),
# so it must still *count as alive*. Splitting the two stops a
# corrupt-but-present identity file from being reported falsely dead.
sig_scan() {
  if command -v find >/dev/null 2>&1 && [ -n "$DT_KPAT" ]; then
    dt_sig_raw=$(find /proc -mindepth 2 -maxdepth 2 -type l -name cwd \( -lname "$DT_KPAT" -o -lname "$DT_KPAT/*" \) -printf '%h\n' 2>/dev/null);
    for dt_sig_h in $dt_sig_raw; do printf '%s\n' "${dt_sig_h##*/}"; done;
  else for dt_sig_p in /proc/[0-9]*; do
      case "$(readlink "$dt_sig_p/cwd" 2>/dev/null)" in "$DT_KJD"|"$DT_KJD"/*) printf '%s\n' "${dt_sig_p#/proc/}";; esac; done; fi; };
# survivors() prints OK|DEGRADED on the first line, then every PID that
# proves the job is still alive. DEGRADED marks an enumeration failure
# (missing/br0ken pgrep or find, fork exhaustion) so an empty census
# under a broken probe reports UNVERIFIED, never a false DEAD.
survivors() { dt_su_deg=0; dt_su_pids=''; dt_su_grun=0;
  dt_su_sc=$(expected_scope);
  dt_su_sh=$(printf "%s\n" "$dt_su_sc" | sed -n "1p");
  case "$dt_su_sh" in DEGRADED) dt_su_deg=1;; OK) dt_su_sp=$(printf "%s\n" "$dt_su_sc" | sed -n '2,$p');
    dt_su_pids="$dt_su_pids $dt_su_sp";; esac;
  if [ "$DT_KGROUP_OWNED" -eq 1 ]; then dt_su_grun=1;
  elif [ "$DT_KLEADER_GONE" -eq 1 ] && group_open; then dt_su_grun=1; fi;
    if [ "$dt_su_grun" -eq 1 ]; then
      dt_su_gp=$(pgrep -g "$DT_KPG" 2>/dev/null); dt_su_grc=$?;
      [ "$dt_su_grc" -gt 1 ] && dt_su_deg=1;
      # A zombie in the group census is not a survivor: it already exited
      # and merely awaits reaping.  Counting it would report ALIVE forever
      # for a job whose every real process is gone.  A pid that vanished
      # between pgrep and the state read is equally not a survivor; when
      # the state cannot be read for a pid that still exists, keep it and
      # fail toward ALIVE rather than invent a death certificate.
      for dt_su_x in $dt_su_gp; do
        if dt_pid_zombie "$dt_su_x"; then continue; fi;
        [ -e "/proc/$dt_su_x" ] || continue;
        dt_su_pids="$dt_su_pids $dt_su_x"; done; fi;
    if command -v find >/dev/null 2>&1 && [ -n "$DT_KPAT" ]; then
      dt_su_cwd=$(find /proc -mindepth 2 -maxdepth 2 -type l -name cwd \( -lname "$DT_KPAT" -o -lname "$DT_KPAT/*" \) -printf '%h\n' 2>/dev/null); dt_su_frc=$?;
      # find exits 1 merely because it could not stat other users' /proc
      # entries; only >=2 (missing/incompatible find, fork failure) is a
      # real enumeration failure worth flagging degraded.
      [ "$dt_su_frc" -gt 1 ] && dt_su_deg=1;
      for dt_su_h in $dt_su_cwd; do dt_su_pids="$dt_su_pids ${dt_su_h##*/}"; done;
    else for dt_su_p in /proc/[0-9]*; do
        case "$(readlink "$dt_su_p/cwd" 2>/dev/null)" in "$DT_KJD"|"$DT_KJD"/*) dt_su_pids="$dt_su_pids ${dt_su_p#/proc/}";; esac; done; fi;
    [ "$dt_su_deg" -eq 1 ] && echo DEGRADED || echo OK;
    for dt_su_x in $dt_su_pids; do printf '%s\n' "$dt_su_x"; done; };

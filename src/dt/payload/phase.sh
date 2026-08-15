#!/usr/bin/env bash
# Record one application phase without requiring Python or project packages.
set -u

if [ "$#" -ne 1 ]; then
    echo "usage: \$DT_PHASE PHASE_NAME" >&2
    exit 2
fi

dt_phase=$1
if [ -z "$dt_phase" ] || [ "${#dt_phase}" -gt 64 ]; then
    echo "phase name must contain 1-64 safe characters" >&2
    exit 2
fi
case "$dt_phase" in
    *[!A-Za-z0-9_.:-]*)
        echo "phase name may contain only A-Z a-z 0-9 _ . : -" >&2
        exit 2
        ;;
esac

if [ -z "${DT_PHASE_CURRENT:-}" ]; then
    echo "DT_PHASE_CURRENT must be set" >&2
    exit 2
fi

dt_phase_timestamp=$(date +%s.%N 2>/dev/null) || dt_phase_timestamp=""
case "$dt_phase_timestamp" in
    *N*|"") dt_phase_timestamp=$(date +%s) ;;
esac

umask 077
if [ -n "${DT_PHASE_FD:-}" ]; then
    case "$DT_PHASE_FD" in *[!0-9]*|"") exit 2;; esac
    printf '{"schema_version":"dt_phase_v1","phase":"%s","timestamp":%s}\n' \
        "$dt_phase" "$dt_phase_timestamp" >&"$DT_PHASE_FD"
elif [ -n "${DT_PHASE_FILE:-}" ]; then
    # Compatibility for standalone payload use. New wrappers retain a
    # nofollow-open descriptor for the complete runtime.
    if [ -L "$DT_PHASE_FILE" ] \
       || { [ -e "$DT_PHASE_FILE" ] && [ ! -f "$DT_PHASE_FILE" ]; }; then
        echo "unsafe DT_PHASE_FILE" >&2
        exit 2
    fi
    mkdir -p "$(dirname "$DT_PHASE_FILE")"
    printf '{"schema_version":"dt_phase_v1","phase":"%s","timestamp":%s}\n' \
        "$dt_phase" "$dt_phase_timestamp" >>"$DT_PHASE_FILE"
else
    echo "DT_PHASE_FD or DT_PHASE_FILE must be set" >&2
    exit 2
fi
dt_phase_tmp="${DT_PHASE_CURRENT}.tmp.$$"
printf '%s\n' "$dt_phase" >"$dt_phase_tmp"
mv -f "$dt_phase_tmp" "$DT_PHASE_CURRENT"

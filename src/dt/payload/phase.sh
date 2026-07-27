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

if [ -z "${DT_PHASE_FILE:-}" ] || [ -z "${DT_PHASE_CURRENT:-}" ]; then
    echo "DT_PHASE_FILE and DT_PHASE_CURRENT must be set" >&2
    exit 2
fi

dt_phase_timestamp=$(date +%s.%N 2>/dev/null) || dt_phase_timestamp=""
case "$dt_phase_timestamp" in
    *N*|"") dt_phase_timestamp=$(date +%s) ;;
esac

umask 077
mkdir -p "$(dirname "$DT_PHASE_FILE")"
printf '{"schema_version":"dt_phase_v1","phase":"%s","timestamp":%s}\n' \
    "$dt_phase" "$dt_phase_timestamp" >>"$DT_PHASE_FILE"
dt_phase_tmp="${DT_PHASE_CURRENT}.tmp.$$"
printf '%s\n' "$dt_phase" >"$dt_phase_tmp"
mv -f "$dt_phase_tmp" "$DT_PHASE_CURRENT"

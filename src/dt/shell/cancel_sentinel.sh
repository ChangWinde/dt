# shellcheck shell=sh
# Atomically write the cancellation sentinel ($DT_KCANCEL) before signalling.
# Loaded by dt.lifecycle.termination_probe().

dt_k_cancel_parent=${DT_KCANCEL%/*};
dt_k_cancel_tmp="$DT_KCANCEL.tmp.$$";
if [ "$dt_k_cancel_parent" != "$DT_KCANCEL" ] &&
mkdir -p -- "$dt_k_cancel_parent" 2>/dev/null &&
[ -d "$dt_k_cancel_parent" ] && [ ! -L "$dt_k_cancel_parent" ] &&
chmod 700 -- "$dt_k_cancel_parent" 2>/dev/null &&
rm -f -- "$dt_k_cancel_tmp" 2>/dev/null &&
printf "%s\n" "$DT_KCANCEL_VALUE" >"$dt_k_cancel_tmp" 2>/dev/null &&
chmod 600 -- "$dt_k_cancel_tmp" 2>/dev/null &&
mv -f -- "$dt_k_cancel_tmp" "$DT_KCANCEL" 2>/dev/null; then :; else
  rm -f -- "$dt_k_cancel_tmp" 2>/dev/null;
  echo "cancel sentinel write failed" >&2; exit 69; fi;

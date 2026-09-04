# shellcheck shell=sh
# GPU inventory for one node: nvidia-smi rows plus dt's own lease state.
# Output per GPU: idx,uuid,used,total,util,temp,leased,lease_owner
dt_gpu_raw=$(nvidia-smi --query-gpu=index,uuid,memory.used,memory.total,utilization.gpu,temperature.gpu --format=csv,noheader,nounits 2>&1); dt_gpu_rc=$?
if [ "$dt_gpu_rc" -ne 0 ]; then
  echo ---DT-GPU-ERROR---; printf '%s\n' "$dt_gpu_raw"
else
  printf '%s\n' "$dt_gpu_raw" | while IFS=, read -r idx uuid used total util temp; do
    idx=$(printf %s "$idx" | tr -d " ")
    lease="${DT_GPU_LEASE_ROOT:-$HOME/dt/gpu-leases}/gpu-$idx.lock"
    leased=0; lease_owner=
    # A lease file whose lock cannot be checked must read busy, not free:
    # flock vanishing (PATH regression, rebuilt container) while a wrapper
    # holds the lease would otherwise double-allocate a busy GPU. Stale-file
    # false-busy is visible and fixable (doctor reports DT_FLOCK=missing).
    if [ -e "$lease" ] && { ! command -v flock >/dev/null 2>&1 || ! flock -n -s "$lease" -c true; }; then
      leased=1; lease_owner=$(head -n 1 "$lease" 2>/dev/null)
    fi
    echo "$idx,$uuid,$used,$total,$util,$temp,$leased,$lease_owner"
  done
fi

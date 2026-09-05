
echo DT_SSH=ok
if command -v bash >/dev/null 2>&1; then echo DT_BASH=ok; else echo DT_BASH=missing; fi
doctor_net() {
    fmt_speed() {
        awk -v s="${1:-0}" 'BEGIN{
            if (s >= 1048576) printf "%.0fMB/s", s/1048576;
            else if (s >= 1024) printf "%.0fKB/s", s/1024;
            else printf "<1KB/s" }'
    }
    if curl -m 3 -sI https://pypi.org >/dev/null 2>&1; then
        # reachability is not usability: measure actual download speed. This
        # is the node's own path to PyPI - what uv sync sees on a cold
        # environment - not the head-to-node transfer link (dt topology and
        # dt seed measure that one); the label names the peer so the two are
        # not compared.
        spd=$(curl -m 8 -so /dev/null -w "%{speed_download}" https://pypi.org/simple/pip/ 2>/dev/null)
        label="pypi $(fmt_speed "$spd")"
        if awk -v s="${spd:-0}" 'BEGIN{exit !(s >= 1048576)}'; then
            echo "DT_NET=ok($label)"
        else
            echo "DT_NET=slow($label)"
        fi
    elif curl -m 3 -sI https://mirrors.aliyun.com/pypi/simple/ >/dev/null 2>&1 \
      || curl -m 3 -sI https://pypi.tuna.tsinghua.edu.cn/simple >/dev/null 2>&1; then echo DT_NET=mirror
    else echo DT_NET=blocked; fi
}
# Network access and the local runtime contract are independent. Keep them in
# one SSH channel but overlap their slow paths; every emitted record is one
# short line, so the parser does not depend on output order.
doctor_net &
dt_net_pid=$!
if command -v nvidia-smi >/dev/null 2>&1; then
    # nvidia-smi prints classic failures (NVML mismatch, lost devices) on
    # stdout; only a plain version number may count as healthy. A present
    # but broken driver is "error", distinct from a CPU-only "missing".
    v=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null | head -1)
    case "$v" in
        ""|*[!0-9.]*) echo "DT_GPU=error: ${v:-no driver output}" ;;
        *) echo "DT_GPU=$v" ;;
    esac
else
    echo DT_GPU=missing
fi
if [ -x "$HOME/.local/bin/uv" ] || command -v uv >/dev/null 2>&1; then echo DT_UV=ok; else echo DT_UV=missing; fi
if command -v tmux >/dev/null 2>&1; then echo DT_TMUX=ok; else echo DT_TMUX=missing; fi
if command -v rsync >/dev/null 2>&1; then echo DT_RSYNC=ok; else echo DT_RSYNC=missing; fi
if command -v flock >/dev/null 2>&1; then echo DT_FLOCK=ok; else echo DT_FLOCK=missing; fi
if command -v python3 >/dev/null 2>&1; then echo DT_PYTHON3=ok; else echo DT_PYTHON3=missing; fi
if command -v timeout >/dev/null 2>&1; then echo DT_TIMEOUT=ok; else echo DT_TIMEOUT=missing; fi
# A user scope is not a durable GPU runtime when logind may tear down the user
# manager after the final login session. CPU-only jobs retain their explicit
# portable fallback, so report this fact separately from generic dependencies.
if command -v loginctl >/dev/null 2>&1; then
    dt_linger=$(loginctl show-user "$(id -u)" --property=Linger --value 2>/dev/null) || dt_linger=unavailable
else
    dt_linger=unavailable
fi
case "$dt_linger" in yes|no) echo "DT_LINGER=$dt_linger" ;; *) echo DT_LINGER=unavailable ;; esac
# What this sshd observed about the control connection: a loopback peer
# means the route enters through a local tunnel endpoint (frp/autossh).
# Consumed and redacted head-side; never rendered raw.
dt_peer="${SSH_CONNECTION%% *}"
echo "DT_PEER=$dt_peer"
dt_peer_server=$(printf '%s' "${SSH_CONNECTION:-}" | awk '{print $3}')
echo "DT_PEER_SERVER=$dt_peer_server"
# Both families: an IPv6-pinned lan_address would otherwise always read
# "stale" because the reported set only ever contained IPv4 addresses.
dt_addrs=$({ ip -4 -o addr show scope global 2>/dev/null; ip -6 -o addr show scope global 2>/dev/null; } | awk '{print $4}' | cut -d/ -f1 | paste -sd, -)
# Minimal containers may lack `ip`; hostname -I matches topology discovery.
[ -n "$dt_addrs" ] || dt_addrs=$(hostname -I 2>/dev/null | tr ' ' '\n' | grep -v '^$' | paste -sd, -)
echo "DT_ADDRS=$dt_addrs"
wait "$dt_net_pid"

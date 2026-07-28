# Support contract

## Supported environment

DistTrainer 0.6 supports:

- Linux head and compute nodes;
- Python 3.10 or 3.11 on the client/head;
- OpenSSH client/server, rsync, tmux, flock, timeout, and uv;
- NVIDIA compute nodes with `nvidia-smi` and a usable CUDA driver for GPU
  jobs;
- one trusted Unix identity per configured center.

macOS may be used as a laptop client when the required command-line tools are
available. Windows, password-interactive SSH, multi-tenant authorization, and
non-NVIDIA accelerators are not supported.

## Compatibility

The documented command names, JSON schemas, exit codes, and non-follow submit
contract are compatibility surfaces. Release notes call out any intentional
change. Experimental or undocumented internal commands are not covered.

## Operational support

Run `dt doctor --json` before first use and after host or driver changes. When
requesting support, provide:

- `dt --version`;
- a redacted `dt doctor --json`;
- the job ID and `dt info JOB --json`;
- the shortest relevant `dt logs JOB -n 200` excerpt;
- whether the problem reproduces after a network reconnect.

Never send SSH keys, tokens, full environment dumps, private datasets, model
weights, or unrelated job output.

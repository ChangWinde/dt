# 大体积 `dt pull` 占满节点链路，调度器快照 rsync 超时、tick 停摆数分钟

- 报告时间：2026-09-06 02:15 | 来源：RAT-Image 项目代理 | dt 0.13.10 | head headstar → 节点 gc6d（frp 隧道，带宽有限）
- 严重度：高（队列有作业、GPU 空闲，却数分钟不派发）

## 经过
02:00 左右，我方脚本对两个已完成作业执行 `dt pull`（其中 `chunk7b-01` 的 outputs 2.4 GB，`chunk11-01` 552 MB，含训练目录）。
同一时刻两个新作业排队，两卡空闲：
```
dt free --explain
   next job 20260906-0155_chunk13-01-001-dt_chunk_cell_5ac365cebf378393
   reason dispatching: gc6d
dt info 8393
   placement failures  gc6d: snapshot failed: code convergence on gc6d failed: rsync error: unexplained error (code 255)
                       at rsync.c(716) [sender=3.2.7] rsync timed out after 60s
dt agent status
   scheduler  stalled  ·  210s since last tick
```
杀掉本地 `dt pull` 与 worker 侧残留的 `rsync --server` 后约 2 分钟，两个作业正常派发。

## 问题
1. 拉回与派发共用一条链路且没有带宽/并发限制；一次大拉回就让代码快照 rsync 超时。
2. 快照失败把整个 agent tick 卡住（`stalled · 210s since last tick`），而不是标记该作业 backoff 后继续处理其他工作。
3. `dt pull` 默认拉全部 outputs：训练作业的 hydra 目录带 replay buffer（GB 级）。`--lite` 又把 checkpoint 全跳过，没有中间档。

## 建议
- `dt pull --bwlimit` / 全局 `transfer.bwlimit`，并限制并发 pull 数；或让 dispatch 的快照 rsync 优先（独立 ssh 连接 + 更长/自适应超时）。
- 快照失败作为 job-level backoff，不阻塞 tick。
- `dt pull` 增加 `--only PATTERN`（或按大小阈值 `--max-file-mb`），并在 payload 文档里建议训练输出把 buffer 放到 `$DT_JOB_DIR/scratch`。

# 容量判定：GPU 上任一外来进程（0.4 GB、0% util 的桌面程序）即视为 busy，48 GB 空闲卡永不派发

- 报告时间：2026-09-06 01:50 | 来源：RAT-Image 项目代理 | dt 0.13.10 | 节点 star-0（本机，48 GB 卡）
- 严重度：中

## 现象
2026-09-05 19:05 起，他项目在 star-0 排了 8 个作业（`lrd-star0-001..008`）。本机 GPU 上只有 rustdesk（424 MiB，0% util）：
```
dt 0/9 GPU free · 2 running · 12 queued · next needs 1 GPU on star-0
   reason waiting: no free capacity (star-0: 0 free < 1 wanted; busy: gpu0 starcosmos 0.4/48.0GiB util0%)
```
这些作业在队列里停留数小时未派发（后来本机被我方项目的本地进程占用，结果一样）。

## 预期
0.4/48 GB、0% 利用率不应等同于"卡被占满"。至少应有阈值（例如显存占用 < 10% 且 util < 10% 视为空闲），或提交时的
`--allow-shared` / 节点级 `share_gpu: true` 让调度器按剩余显存放置。

## 备注
同一模型也让我们无法在 dt 内做"每卡多作业"：一个 dt 作业占住 GPU 后，后续作业排队，即便该作业只用了 1–3 GB 显存、~50% 利用率
（单个 DrQ-v2 训练的典型值）。我们只能在一个作业内部并行多个 cell（`dt_chunk_cell.sh`）来把卡填到 80% 以上。

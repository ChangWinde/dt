# `dt pull` 传输不完整却以成功返回（截断的 .pt 文件、mtime 1970）

- 报告时间：2026-09-06 01:50 | 来源：RAT-Image 项目代理 | dt 0.13.4 → 0.13.10 期间 | head headstar → 节点 gc6d（frp 隧道）
- 严重度：高（静默数据损坏）

## 现象
作业 `chunk8-01-001-dt_chunk_cell`（ref `13bd`，状态 finished/1）由脚本执行
`dt pull 13bd --to results/dt_pull/13bd --force >/dev/null 2>&1`（返回 0）。本地得到：
```
results/dt_pull/13bd/models/victim/dagger2_seed1/
  -rw------- 8124568  1月 1 1970  window-open-v2_DRQV2.pt        # 只有这一个文件，8.1 MB，时间戳 1970-01-01
```
worker 端同目录（`~/dt/worker/jobs/20260905-2313_chunk8-01-001-dt_chunk_cell_f59bf6c3ffd113bd/outputs/models/victim/dagger2_seed1/`）
有 20 个文件（45.8 MB 的 `.pt` 各 10 个 + manifest），`window-open-v2_DRQV2.pt` 为 45,795,397 字节。
删除本地目录后 `dt pull 13bd --to … --force` 重拉，全部文件完整。同日 `chunk9-01`（ref `6590`）同样不完整，重拉后正常。

## 预期
传输中断/失败时非零退出并报错；不应留下截断文件且退出码为 0。截断文件的 mtime 为 epoch 0，说明是未写完的临时文件被当作成品留下。

## 影响
下游脚本按"已拉回"跳过该作业（我们的 `dt_collect_chunks.sh` 用 `results/dt_pull/<ref>/dt` 目录存在判定已拉回），权重文件缺失/损坏直到人工发现。

## 建议
1. 拉回后按 worker 端 manifest 校验文件数与大小/哈希，缺失或不匹配即非零退出并删除残留。
2. 临时文件写完再改名（原子落盘），避免半成品带 epoch mtime 落地。
3. `dt pull --json` 输出 transferred/expected 文件数与字节数。

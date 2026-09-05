# 调度：`-g 0` 的 CPU 作业排在等 GPU 的作业之后，CPU 空闲也不派发

- 报告时间：2026-09-06 01:50 | 来源：RAT-Image 项目代理 | dt 0.13.4（现 0.13.10）| 节点 gc6d（40 核，两卡均忙）
- 严重度：中

## 复现
2026-09-05 18:22，gc6d 两卡各跑一个 GPU 作业，队列里有若干等 GPU 的作业。提交两个纯 CPU 作业：
```
printf 'sleep 90\nsleep 90\n' > /tmp/sleep_batch.txt
dt batch gc6d -p ratimage_c -g 0 -n locktest -F /tmp/sleep_batch.txt
```
40 秒后 `dt ps`：`locktest-001-sleep … queued #23/24`、`locktest-002-sleep … queued #24/24`；`dt free --explain`：
`next needs 1 GPU on gc6d … reason waiting: no free capacity (gc6d: 0 free < 1 wanted; busy: gpu0 …)`。CPU 作业一直未运行（随后手动 kill）。

## 预期
CPU 作业不与 GPU 作业竞争容量，按 AGENTS.md 的口径不应被前面等 GPU 的作业挡住；节点 CPU 有余量时应立即派发。

## 影响
无法用 `-g 0` 作业在 GPU 满载时跑评测/汇总类 CPU 工作；也无法用它做"第三路填卡"（`-g 0` 作业自选 GPU）。

## 建议
队列按资源类别分别应用 FIFO：GPU 作业之间 FIFO，CPU 作业只看 CPU/内存容量。

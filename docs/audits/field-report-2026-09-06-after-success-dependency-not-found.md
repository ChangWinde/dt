# `dt run --after-success <ref>`：前驱已 `finished/0`，依赖作业却永久 `waiting: dependency <ref> was not found`

- 日期：2026-09-06 18:48–19:25；报告方：RAT-Image 项目（agent 分支 B）；dt 0.13.12；节点 gc6d
- 严重度：中（依赖链不可用；作业静默卡死并占住队首 "dispatching"）

## 现象
```
dt run -p ratimage --node gc6d -g 1 --artifact-manifest 1c08c2c0… \
  --after-success 20260906-1653_chunk29-01-001-dt_chunk_cell_33386d7425092b8c -n r18-followup -- env MAXJOBS=3 bash scripts/dt_r18_followup.sh …
```
提交时前驱正在运行（`dt batch` 提交的块作业，ref 取自 `dt ps` 第二列的完整 ref）。提交返回 `reason: waiting: dependency … 32b9`。
前驱 16:53 提交、约 19:10 `finished/0`。之后依赖作业：
```
status  queued  waiting: dependency 20260906-1653_chunk29-01-001-dt_chunk_cell_33386d7425092b8c was not found
```
`dt free --explain`：`next is dispatching on gc6d … queue model 0 runnable · 1 blocked · 0 waiting`。GPU0 空闲 15 分钟以上。
前驱在 `dt ps -a` 里可见（`chunk29-01-001-dt_chunk_cell finished/0`）。

## 猜测
`--after-success` 记录的是完整 ref 字符串，而依赖检查用另一种键（短 ref / 注册表行 id / batch 项的父 id）查询，
前驱完成后从"活动队列"移入历史，查不到就报 not found 而不是读历史行的终态。

## 期望
- 依赖检查对 `dt ps -a` 能列出的任何 ref（完整 / 短）都能解析，包括已进入历史的终态行；
- "dependency not found" 应立刻判为 `skipped / dependency_missing` 并说明，而不是永久 waiting；
- `--after-success` 在提交时若解析不到前驱应直接拒绝（此处提交时它是能解析的，问题在完成后）。

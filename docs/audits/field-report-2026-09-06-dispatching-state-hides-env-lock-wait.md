# 补充证据：派发已落到 worker 但卡在环境 flock 时，head 显示 "dispatching" 20 分钟；同时另一次 `dt batch` 提交也挂住

- 日期：2026-09-06 05:22–05:43；报告方：RAT-Image 项目（agent 分支 B）；dt 0.13.10（head）；节点 gc6d
- 关联：`docs/audits/field-report-2026-09-06-*`（环境锁队首阻塞）与 PR #91（fix/env-lock-concurrency）。本报告是修复合入**之前**
  抓到的一组精确证据，供回归测试参考；若 #91 已覆盖，请忽略状态部分，但"提交也挂住"这一点建议单独确认。

## 现象
1. `chunk18-01`（项目 `ratimage`，env-key `cedde55e5751`）在 GPU0 运行；提交同项目 `chunk19-01`。GPU1 空闲。
2. `dt ps`：`chunk19-01 … queued`；`dt free --explain`：`next is dispatching on gc6d / reason dispatching: gc6d`，持续 > 20 分钟。
3. worker 侧：
   ```
   bash …/20260906-0522_chunk19-01-…/.dt/payload/launcher.sh
     \_ flock --close /home/baifengshuo/dt/envs/cedde55e5751.lock env UV_PROJECT_ENVIRONMENT=/home/baifengshuo/dt/envs/cedde55e5751 … uv sync …
   ```
   即作业已在 worker 上等 `chunk18-01` 持有的环境锁；head 却把它记作 dispatching，不释放位次，也不在 explain 里说原因。
4. 同一时段 head 上另一个 `dt batch gc6d -p ratimage …`（提交 `chunk19-02`）挂了约 20 分钟；它在 worker 上的远端进程停在
   `bash -c '# POSIX sh helpers proving a PID still belongs to one dt job …'` 的 `sleep 0.1` 轮询。我 `dt kill` 掉 `chunk19-01` 后
   该提交才完成注册并立刻被派发到 GPU1。提交路径看起来在等派发路径持有的某个东西。
5. 把 `chunk19-01` 改到 `ratimage_b` 项目重提，立刻在 GPU1 运行。

## 期望
- worker 侧等环境锁应回报为 `blocked: env-lock(<env-key>, held by <ref>)`，并让后续可运行作业越过（#91 应已处理）；
- `dt batch` / `dt run` 的提交不应被一个正在等锁的派发挂住；若共享同一把 head 侧锁，请在等待超过 N 秒时打印在等什么。

## 复现
两卡节点；同项目提交 A（长跑）与 B；观察 B 在 worker 上的 `flock` 与 head 的 `dispatching` 状态；同时再 `dt batch` 提交任意作业，观察其阻塞。

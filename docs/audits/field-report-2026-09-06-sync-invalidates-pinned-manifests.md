# 对同一项目再次 `dt sync --artifact` 后，已排队作业固定的旧 manifest 变为 `artifact-unverified` 并阻塞队首

- 报告时间：2026-09-06 01:50 | 来源：RAT-Image 项目代理 | dt 0.13.10 | 节点 gc6d
- 严重度：高（队列静默阻塞，两卡空转）

## 复现
1. 2026-09-05 上午：`dt sync gc6d -p ratimage --artifact models/victim/migrated … --artifact models/victim/migrated_seed3 …`，
   manifest `cd4e1e77a06f…`。多批作业用 `dt batch … --artifact-manifest cd4e1e77a06f…` 提交并正常运行。
2. 18:09：本地 `models/victim/migrated_seed3/` 新增了文件（door-open 权重），再次 `dt sync gc6d -p ratimage --artifact …`（同一组目录），
   得到新 manifest `3f241425a90f…`。
3. 之后用旧 digest 提交的排队作业全部卡住，`dt free --explain`：
   ```
   next is blocked by a job constraint
   reason blocked: gc6d: artifact-unverified: [launcher] artifact-unverified: /home/baifengshuo/dt/worker/artifacts/ratimage
   drifted from manifest cd4e1e77a06f (artifact size mismatch for models/victim/migrated_seed3: expected 228977082, got 274772356);
   republish it with dt sync --artifact before jobs pinned to it can start here
   ```
   两卡 0%，直到人工 `dt kill` 并用新 digest 重提。

## 预期 / 问题
- 新增文件是旧 manifest 内容的超集，旧 manifest 引用的每个文件都未变化；按文件级哈希校验应仍然有效。至少 `dt sync` 时应警告
  "N 个排队作业固定在即将失效的 manifest 上"，而不是让它们静默阻塞。
- `dt sync` 只打印 12 位前缀（`synced gc6d … manifest 3f241425a90f`），而 `--artifact-manifest` 要求 64 位完整 digest（前缀被拒），
  只能 ssh 到 worker 读 `~/dt/worker/artifacts/<project>/.dt/manifests/*.json` 取全名。

## 建议
1. manifest 按文件内容寻址：目录新增文件不使旧 manifest 失效；或允许多个 manifest 共存。
2. `dt sync` 完成时列出受影响的排队作业并给出 `dt requeue --artifact-manifest NEW` 之类的一键迁移。
3. `dt sync` 打印完整 digest（或 `--artifact-manifest` 接受唯一前缀），并提供 `dt artifacts list -p PROJECT`。

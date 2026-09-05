# `dt batch` 缺少 `--artifact-target`（与 `dt run` 不对等）

- 报告时间：2026-09-06 01:50 | 来源：RAT-Image 项目代理 | dt 0.13.4 → 0.13.10
- 严重度：低（功能缺口，但导致了上面那份"符号链接污染"事故）

`dt run` 支持 `--artifact-target TARGET[=SOURCE]` 把校验过的 artifact 挂到代码相对路径；`dt batch` 只有 `--artifact-manifest`。
同节点批量提交（避免 `dt run` 每次约 4 分钟的同步派发）时，payload 只能自己在工作目录里 `ln -s "$DT_ARTIFACT_ROOT/<rel>" <rel>`。
这套 shim（`scripts/lib/dt_artifacts.sh`）在同作业并发 cell 下出过竞态，把链接写进了 artifact 目录（见
`2026-09-06-artifact-dir-poisoned-by-job-symlink.md`）。

建议：`dt batch` 增加 `--artifact-target`，语义与 `dt run` 一致；并在文档里说明 `$DT_ARTIFACT_ROOT` 是只读输入。

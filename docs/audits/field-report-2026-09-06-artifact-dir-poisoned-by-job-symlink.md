# 作业写进 artifact 目录一个符号链接后，该项目所有后续作业 env-fail；错误只在 `dt info` 可见

- 报告时间：2026-09-06 01:50 | 来源：RAT-Image 项目代理 | dt 0.13.4 | 节点 gc6d
- 严重度：中（根因在我方作业，但 dt 的防护与可观测性不足）

## 经过
我们的 payload 在作业工作目录里为 `$DT_ARTIFACT_ROOT/<rel>` 建符号链接（`dt batch` 没有 `--artifact-target`，见另一份报告）。
同一作业内两个并发 cell 竞态：第二个 `ln -s` 在第一个链接已存在时"钻进"目标目录，于是在 worker 的 artifact 目录里留下了
`~/dt/worker/artifacts/ratimage_b/models/victim/migrated/migrated -> …`（以及 `migrated_seed2..8/`、`data/il_demos/il_demos` 等 19 个链接）。
之后该项目每个作业都失败：
```
placement failures  gc6d: env-fail: [launcher] artifact integrity failed; see logs/env.log
failure log         artifact verification failed: artifact directory contains symlink:
                    /home/baifengshuo/dt/worker/artifacts/ratimage_b/models/victim/migrated/migrated
```
（2026-09-05 11:27–11:41，`chunk-02/04/05/07/08` 五个作业 failed。）

## dt 侧的问题
1. artifact 目录对作业进程可写。一次误写就让整个项目的 staging 失效，且需要 ssh 到 worker 手工 `find … -type l -delete` 修复。
2. `dt logs REF` 对 env-fail 作业无法显示环境日志：
   ```
   active log: logs/env.log
   [log-capture] unsafe or unavailable log storage
   ```
   没有 `--env` 选项（`Error: No such option: --env`），失败原因只能从 `dt info REF` 的 `failure log` 字段读到。
3. 这类项目级故障不会在 `dt ps` 里区别于普通失败，第二个作业失败时就该有"同一 artifact 已连续 N 次校验失败"的提示。

## 建议
- 把 artifact 根目录以只读方式呈现给作业（chmod -w / bind ro），或校验失败时给出 `dt artifacts repair`。
- `dt logs REF --env`（或默认在 env-fail 时打印 env.log 尾部）。
- 连续 env-fail 归因到同一 artifact 时，在 `dt ps --issues` 里聚合提示。

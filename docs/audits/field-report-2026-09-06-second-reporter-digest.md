# 现场报告合集：RAT-Image 项目 agent 分支 B（2026-09-06 04:56 收到）

同一项目的第二个报告方对同一天的问题的独立记录，逐字保留。除 `run-dispatch-latency` 外均与 2026-09-06 已归档的报告同源；
其中"环境锁队头阻塞"与"dt run 单次提交约 4 分钟"的根因相同——launcher 对每次启动都排他地取环境锁做 `uv sync`，而运行中作业的 wrapper 以共享方式持有同一把锁直至结束。

---

<!-- 2026-09-06-artifact-dir-symlink-breaks-all-jobs.md -->

## artifact 目录里出现一个符号链接后，该项目所有后续作业在环境阶段失败；`dt logs` 看不到 env.log

- 日期：2026-09-05 11:41；报告方：RAT-Image 项目（agent 分支 B）
- dt 版本：0.13.4；节点 gc6d
- 严重度：中（根因在我们的作业脚本，但 dt 的存储没有自我保护，且错误很难看到）

### 现象
我们的载荷用 `ln -s "$DT_ARTIFACT_ROOT/<rel>" "<rel>"` 把暂存的 artifact 链接进代码树（因为 `dt batch` 没有 `--artifact-target`）。
一个作业里两个并发 cell 同时执行这句，第二个 `ln -s` 落到了第一个链接指向的目录里，于是在 worker 的 artifact 暂存目录内部
产生了 `artifacts/ratimage_b/models/victim/migrated/migrated -> …` 这样的链接（共 19 个，见下）。之后该项目**所有**作业：

```
status  failed  gc6d: env-fail: [launcher] artifact integrity failed; see logs/env.log
failure log  artifact verification failed: artifact directory contains symlink:
  /home/baifengshuo/dt/worker/artifacts/ratimage_b/models/victim/migrated/migrated
```

- `dt logs <ref>` 只输出 `active log: logs/env.log` 与 `[log-capture] unsafe or unavailable log storage`，看不到内容；
  `dt logs --env` 不存在。真正的原因只在 `dt info <ref>` 的 `failure log` 字段里。
- 5 个块作业（每个 6 个 cell）因此失败，直到我们 ssh 上去 `find artifacts/* -type l -delete`。

### 期望
- 暂存的 artifact 根目录应对作业只读（`chmod -R a-w` 或只读 bind），作业写不进去；
- 完整性校验失败时把原因直接放进 `dt ps`/`dt logs` 可见处；`dt logs` 提供 `--env` 或自动回落到 env.log；
- `dt batch` 支持 `--artifact-target`（见另一份报告），就不需要作业自己建链接。

### 复现
在作业里对 `$DT_ARTIFACT_ROOT` 下任一目录创建一个符号链接，然后再提交同项目任何作业。

---

<!-- 2026-09-06-artifact-manifest-drift-after-sync.md -->

## 同项目再次 `dt sync --artifact` 后，钉住旧 manifest 的排队作业永久 `artifact-unverified`；`dt sync` 只打印 12 位摘要

- 日期：2026-09-05 18:10；报告方：RAT-Image 项目（agent 分支 B）
- dt 版本：0.13.10；节点 gc6d
- 严重度：中高（队列静默卡死，两张卡空转直到人工发现）

### 现象
1. 早上用 `dt sync gc6d -p ratimage --artifact models/victim/migrated_seed3 ...` 暂存了 artifact，manifest 摘要 `cd4e1e77…`。
2. 之后往 `models/victim/migrated_seed3/` 加了新文件（door-open 权重），再次 `dt sync -p ratimage --artifact ...`，得到新摘要 `3f241425…`。
3. 用旧摘要 `--artifact-manifest cd4e1e77…` 提交的 6 个作业全部变成：

```
placement failures  gc6d: artifact-unverified: [launcher] artifact-unverified:
  /home/baifengshuo/dt/worker/artifacts/ratimage drifted from manifest cd4e1e77a06f
  (artifact size mismatch for models/victim/migrated_seed3: expected 228977082, got 274772356);
  republish it with dt sync --artifact before jobs pinned to it can start here
```

作业停在 queued，`dt free --explain` 显示 `next is blocked by a job constraint`。两张卡空闲。只能 `dt kill` 后用新摘要重提。

4. `dt sync` 的输出只给 12 位前缀（`manifest 3f241425a90f`），而 `--artifact-manifest` 要求完整 64 位十六进制
（12 位会被拒绝）。我们只能 ssh 到 worker 读 `~/dt/worker/artifacts/<project>/.dt/manifests/*.json` 的文件名取完整摘要。

### 期望
- 旧 manifest 里列出的每个文件若内容未变（新 sync 只是超集），旧摘要仍应可验证通过（按文件校验，而不是按目录总大小）；
- 若设计上一个项目只保留一份 manifest，那么 `dt sync` 时应警告并列出会被作废的排队作业（"N queued jobs pinned to cd4e1e77… will be blocked"），
  最好提供 `--repin` 一键改钉新摘要；
- `dt sync` 打印完整 64 位摘要，或 `--artifact-manifest` 接受唯一前缀。

### 复现
sync → 提交钉住该摘要的作业（排队） → 往同一 artifact 目录加文件并再 sync → 观察排队作业变 `artifact-unverified` 且永不恢复。

---

<!-- 2026-09-06-batch-missing-artifact-target.md -->

## `dt batch` 缺少 `--artifact-target`（与 `dt run` 不对等）

- 日期：2026-09-05；报告方：RAT-Image 项目（agent 分支 B）；dt 0.13.4 / 0.13.10
- 严重度：低（功能缺口，但直接导致了 artifact 目录被符号链接污染的事故）

`dt run` 可以用 `--artifact-target TARGET[=SOURCE]` 把校验过的 artifact 按仓库相对路径挂进代码树；`dt batch` 只有 `--artifact-manifest`，
作业只能自己从 `$DT_ARTIFACT_ROOT` 建链接（我们写了 `scripts/lib/dt_artifacts.sh`）。并发 cell 下这段用户代码出过竞态
（见 `2026-09-06-artifact-dir-symlink-breaks-all-jobs.md`）。

建议：`dt batch` 支持与 `dt run` 相同的 `--artifact-target`，并由 launcher 在作业启动前完成链接（单点、无竞态）。

---

<!-- 2026-09-06-env-lock-head-of-line-blocking.md -->

## 队首作业等待同项目环境锁时，后面其他项目的可运行作业也被卡住（第二张卡空转）

- 日期：2026-09-05（11:00 与 23:20 两次）；报告方：RAT-Image 项目（`~/cw/project/RAT-Image`，agent 分支 B）
- dt 版本：11:00 时 0.13.4；23:20 时 0.13.10（`dt --version`）
- 节点：gc6d（2× 4090 D，经 frp 转发 ssh），head = headstar
- 严重度：高（直接把双卡节点变成单卡）

### 现象
同一项目（同一 uv 环境）的作业共用一把"环境锁"，同项目作业只能一个一个跑。当队首作业与正在运行的作业同属一个项目时，
队首在等环境锁；此时**另一张 GPU 空闲，而队列后面属于其他项目、本可以立刻运行的作业也不被派发**。

两次实例：
1. 2026-09-05 11:00 左右：`b18x-faucetopen-ms100`（项目 `ratimage`）在 GPU0 运行；队首 `bc240-door-open-500000-s1`（也是 `ratimage`）
   等环境锁；其后是 `ratimage_b` 项目的多个作业。GPU1 空闲约 30 分钟，直到 GPU0 作业结束。
2. 2026-09-05 23:20：`chunk7-01`、`chunk8-01`、`chunk9-01`、`chunk10-01`（全部 `ratimage`）排队，`chunk7-01` 运行中；GPU1 空闲。
   把 chunk8/9/10 撤回并改成 `ratimage_b` / `ratimage_c` 项目重提后，GPU1 立刻开始运行。

`dt free --explain` 在两种情况下都显示 `queue model N runnable · 0 blocked · M waiting`，把队首归为"waiting: no free capacity"，
没有把"等环境锁"作为阻塞原因暴露出来。

### 期望（对照 docs/architecture.md 第 97–110 行）
"A job-specific placement blocker … also lets later work pass"。等待同项目环境锁是典型的 job-specific blocker：它不消耗 GPU，
后面不同项目、能用空闲卡的作业应当越过它被派发；`dt free --explain` 应把"env-lock wait"列为 blocked 原因。

### 复现
1. 一个项目 P 提交作业 A（1 GPU，运行 30 分钟）和 B（1 GPU）；另一个项目 Q 提交作业 C（1 GPU）。顺序 A、B、C。
2. 节点有 2 张空卡。观察：A 运行；B 等 P 的环境锁；C 一直排队直到 A 结束。
3. 期望：C 在 A 运行期间就在第二张卡上运行。

### 我们的规避
建了三个只差一个空 extra 的 dt 项目（`ratimage` / `ratimage_b` / `ratimage_c`）轮转提交，见
`~/cw/project/RAT-Image/dev/reports/dt.md`（2026-09-05 条目）。这让每张卡多占一份 7.9 GB 的 artifact 暂存（见另一份报告）。

### 建议
- 环境锁等待归入 job-specific blocker，允许后续可运行作业越过；
- 或者环境一旦构建完成就不再需要串行锁（只在构建阶段互斥），让同项目多个作业并行；
- `dt free --explain` 与 `dt ps` 的 state 字段显式给出 `blocked: env-lock(<project>)`。

---

<!-- 2026-09-06-gpu-busy-by-idle-desktop-process.md -->

## 一个占 424 MiB、0% 利用率的桌面进程让 48 GB GPU 被判定为 busy，排队作业永不派发

- 日期：2026-09-05 19:10 起持续；报告方：RAT-Image 项目（agent 分支 B）；dt 0.13.10；节点 star-0（本机，1× 48 GB）
- 严重度：中（另一项目 `lrd-online-clean` 的 8 个作业在 star-0 上排队数小时）

`dt free --explain`：

```
next job 20260905-1811_lrd-star0-001-bash_200918f8a5e1fbe1
reason waiting: no free capacity (star-0: 0 free < 1 wanted; busy: gpu0 starcosmos 0.4/48.0GiB util0%)
```

GPU 上只有 `/usr/share/rustdesk/rustdesk`（远程桌面）占 424 MiB、利用率 0%。容量模型把"任何进程占用"都当作 busy，
于是这张卡对 dt 永远不可用。

建议：可配置的 busy 判定阈值（例如显存占用 < 5% 且 util < 5% 视为 free），或节点级进程白名单；
`--explain` 里列出占用进程名以便用户判断。

---

<!-- 2026-09-06-per-project-artifact-duplication.md -->

## 同一份 artifact 内容按项目重复暂存；新项目首次 sync 要重传/重整 7.9 GB（24 分钟）

- 日期：2026-09-05 10:20；报告方：RAT-Image 项目（agent 分支 B）；dt 0.13.4；节点 gc6d
- 严重度：低中（磁盘与时间成本；与环境锁问题叠加后放大）

### 现象
为规避环境锁串行（见 `2026-09-06-env-lock-head-of-line-blocking.md`），我们建了 `ratimage_b`、`ratimage_c` 两个项目，代码路径与
`ratimage` 完全相同。每个项目都要单独 `dt sync --artifact`，worker 上出现三份相同内容：
`~/dt/worker/artifacts/{ratimage,ratimage_b,ratimage_c}/`（各 7.9 GB）。`ratimage_c` 的首次 sync 用了 24 分钟，
尽管相同内容已经在同一台机器的另一个项目目录下。

### 建议
- artifact 按内容摘要存到节点级共享存储（`~/dt/worker/artifacts/blobs/<sha256>`），项目/manifest 只记引用；
- 或至少在同节点同用户下按文件摘要去重（硬链接），新项目 sync 时先查本机已有内容。

---

<!-- 2026-09-06-ps-name-truncation.md -->

## `dt ps -a` 截断作业名，靠名字去重的自动化会误判

- 日期：2026-09-05 08:30；报告方：RAT-Image 项目（agent 分支 B）；dt 0.13.4
- 严重度：低

`dt ps -a` 的 name 列在较长名字上显示为 `bc240-door-open-500000-s1-001-dt_bc_…`（省略号），同批提交里 `…-s1-…` 与 `…-s10-…`、
或只差后缀的名字无法区分。我们的提交脚本用 `dt ps -a` 的名字前缀判断"已完成/已排队则跳过"，因此把已完成的
`b18-windowopen-ms200` 重复提交了一次。

建议：`-a`/`--recent` 下 name 列不截断（或提供 `--wide`）；文档里提示自动化应改用 `dt ps --json` 的完整字段。

---

<!-- 2026-09-06-pull-partial-file-exit-0.md -->

## `dt pull` 返回 0 但拉回的文件被截断（7.8 MB / 45.8 MB，mtime 1970-01-01）

- 日期：2026-09-05 23:50；报告方：RAT-Image 项目（agent 分支 B）
- dt 版本：0.13.10；节点 gc6d；head headstar
- 严重度：高（静默的数据损坏）

### 现象
对已结束作业 `20260905-2313_chunk8-01-001-dt_chunk_cell_f59bf6c3ffd113bd`（状态 `finished/1`）执行
`dt pull <ref> --to results/dt_pull/<ref> --force`，命令**退出码 0**，但目标目录里只有：

```
results/dt_pull/13bd/models/victim/dagger2_seed1/window-open-v2_DRQV2.pt   8124568 字节，mtime 1970-01-01
```

worker 上同目录实际有 12 个文件（3 个 checkpoint × .pt + .manifest.json，每个 .pt 45 795 733 字节）；其他两个
`dagger2_seed{2,3}` 目录和 3 个训练日志完全没有拉回。删除目标目录后再次 `dt pull --force` 拉回了全部文件。

调用方式：`scripts/dt_collect_chunks.sh` 顺序对多个 ref 调用 `dt pull`（上一个 ref 的 pull 刚结束），网络为 frp 转发的 ssh。
同一脚本对其他 ~20 个作业的 pull 均完整。

### 期望
- 传输不完整时应非零退出并说明（哪个文件、期望字节数、实际字节数）；
- 拉回后按 outputs 清单校验文件数与大小（worker 端可以先生成清单）；
- 部分写入的文件不应留下 1970 时间戳的残片，或至少应写到临时名再原子改名。

### 影响
下游脚本把"目录存在"当作"已拉回"，跳过了重拉；权重文件残缺，若不是我们的 checkpoint loader 强制校验 sha256 manifest，
会加载失败或加载到错误模型。

### 复现线索
- head 端 `dt pull` 的日志（若有）时间 2026-09-05 23:50–23:51；
- worker 作业目录：`~/dt/worker/jobs/20260905-2313_chunk8-01-001-dt_chunk_cell_f59bf6c3ffd113bd/outputs/`。

---

<!-- 2026-09-06-run-dispatch-latency.md -->

## `dt run` 单次提交约 4 分钟（同步派发 + 5.5 GB artifact 逐作业校验）

- 日期：2026-09-05 上午；报告方：RAT-Image 项目（agent 分支 B）；dt 0.13.4；节点 gc6d（frp 转发 ssh）
- 严重度：低中（0.13.10 上未复测，可能已改善）

`dt run -p ratimage --node gc6d -g 1 --artifact-manifest <sha> -n <name> -- <cmd>` 每次约 4 分钟才返回，
其中 worker 端对 5.5 GB artifact 做逐作业 manifest 校验约 5 分钟并串行化派发。36 个 cell 用 `dt run` 需要 2 小时以上；
改用 `dt batch -F file` 后同一批 4 分钟内全部入队。

建议：`dt run` 默认异步入队并立即返回 ref（同步等待用 `--wait`）；artifact 校验按 manifest 摘要缓存（同摘要只校验一次）。


# dt — DistTrainer

在多人共享、无 root 的 GPU 服务器集群上派发实验：找空闲卡、同步代码快照、
uv 复现环境、tmux 挂后台、收结果，全部收敛成一条命令。设计文档见
`tasks/DistTrainer.md`（Tools 仓库）。

## 安装（发布制品）

正式安装只接受 `scripts/release-check.sh` 生成并通过 SHA-256 审计的 wheel；
安装命令仍为 `dt`，发行包名为 `disttrainer`。节点无需 root，但必须预先安装
经过组织审核的 `uv`。

先从干净 release commit 生成制品：

```bash
scripts/release-check.sh dist
```

在主节点或笔记本安装：

```bash
bash bootstrap.sh \
  dist/disttrainer-0.6.1-py3-none-any.whl \
  dist/runtime-constraints.txt
```

bootstrap 会同时校验相邻 `SHA256SUMS` 中的 wheel 与运行时依赖约束，拒绝
symlink、缺失摘要和内容漂移。计算节点仍无需安装 dt：launcher / wrapper
随作业快照下发。

## 部署与回滚

发布部署必须显式给出目标，不再同步活动工作树，也不使用 `rsync --delete`：

```bash
./deploy.sh --plan dist HEAD_A HEAD_B
./deploy.sh dist HEAD_A HEAD_B
```

每个版本保留在远端
`~/.local/share/disttrainer/releases/<version>/`。回滚只重新安装已经验证并
保留的旧制品：

```bash
./deploy.sh --plan --rollback 0.6.1 HEAD_A
./deploy.sh --rollback 0.6.1 HEAD_A
```

开发仍使用 `uv sync --locked --all-groups` 与 `uv run dt`，但 editable 工作树
不属于正式部署路径。`dt --version` 在源码仓库中会附 git SHA；安装的 wheel
只显示发布版本。

## 配置

`~/.config/dt/config.yaml`，按角色二选一（bootstrap 会生成带注释的骨架）：

```yaml
# 主节点
center: psibot
nodes:
  - {name: psibot-hm, local: true}
  - {name: psibot-ds}
  - {name: psibot-ys}
projects:
  vla: ~/cw/project/vla
  omnistack:                       # 长形式：本地 libs 依赖走 setup 钩子
    path: ~/cw/project/OmniStack   # （环境锁内执行，相同 setup 输入幂等）
    setup: uv pip install --no-deps ./libs/CleanDiffuser
    setup_inputs:                  # 可选：setup 真正读取的项目内文件/目录
      - libs/CleanDiffuser         # 其他训练代码改动可复用环境
default_project: vla
paths:
  root: ~/dt
  envs: ~/dt/envs        # home 在 NFS 时改指节点本地盘
  results: /data/dt-results  # 可选：pull 托管结果放独立数据盘
disk_min_gib: 10         # 所有任务的远端启动安全底线
queue:                   # 可选：排队与自我约束旋钮
  poll_s: 60             # 无排队任务时的 agent 维护轮询间隔
  active_poll_s: 2       # 有任务排队时的容量重试间隔
  max_my_jobs: 4         # 本人最大并发作业数
  reserve_free_per_node: 0   # 每节点至少留空 N 张卡
  auto_clean_days: 14    # agent 每日自动清理 N 天前的作业与闲置环境
webhook: https://...     # 可选：作业开始/结束/失败 POST 通知

# 笔记本
default_center: psibot
centers:
  psibot: {head: psibot-hm}
  zgca:   {head: zgca-r0}
  star:   {head: star-0}
```

有 `uv.lock` 时，节点环境保存在 `paths.envs`，`dt info` 的 `env` 字段给出
12 位环境身份。只依赖 lock 的项目沿用 lock 哈希以复用旧缓存；配置了
`extras` 时，不同 extras 使用不同环境，避免可选依赖串用；配置了 `setup`
时，默认还绑定 setup 内容和完整 `snapshot_sha256`，保证任意 hook 安全隔离。
若 hook 只读取少数项目内路径，可显式配置 `setup_inputs`；环境键此时绑定这些
路径、setup 内容和根 `pyproject.toml`，无关训练代码改动不再新建整套环境，
而 setup 输入或项目入口元数据变化仍会隔离。所有 uv 任务都会优先从本作业的
`code/` 和 `code/src/` 导入，因此多个快照即使复用环境也不会串用 editable
源码。`setup_inputs` 必须完整列出 hook 会安装或读取的项目内源码；遗漏时应
删除该配置，回到完整快照隔离。节点启动和任务包装层会先清除调用端继承的
`VIRTUAL_ENV` / `UV_PROJECT_ENVIRONMENT`，因此 setup 和任务只会使用 dt
选定的隔离环境。若节点重启或传输中断留下明确的 invalid-wheel 缓存，launcher
会从 uv 错误中提取精确包名，执行一次包级 `uv cache clean` 并只重试一次；
普通解析、Python 版本、构建或依赖错误不会被掩盖或盲目重试。首次失败和恢复
动作都保留在 `logs/env.log`。闲置环境由 `dt clean --envs` 回收。
任务启动成功行会直接显示 snapshot 与 prepare（uv/setup/launch）阶段耗时、
12 位 env 身份、环境是 new/existing，以及 setup hook 是 ran/cached；
`--json`、`dt info --json` 和回收的 `dt/job.json` 保留对应的
`snapshot_duration_s/launch_duration_s/env_preexisting/setup_ran` 字段。
GPU 任务还会在 `launch_phases_s` 中细分 preflight、artifact verification、
environment、launch-lock wait、GPU probe、session start 与 remote total；人类版
`dt info` 用一行紧凑展示这些阶段，便于直接定位排队交接或环境启动瓶颈。

配好先跑 `dt doctor`，它会逐项验证配置声明的连通性与依赖。窄终端保留完整
节点名和列标题，并把常见 SSH 故障压缩成 `offline: no route/timeout/refused`；
`--json` 保留原始完整错误，健康检查失败时与人类表格一致返回非零，适合 CI
和守护脚本直接判断。每个节点另有结构化 `unreachable` 布尔值；只有 SSH/链路
不可达时整体返回 5，已连接但缺少 GPU、uv、tmux、rsync、flock、dt 或协议异常
仍返回 1。laptop 会接受远端 doctor 的合法非零健康 JSON，因此一台计算节点
离线不会导致整个 center 的逐节点诊断被丢弃或截断成 `doctor failed`。

## 快速上手

```bash
dt free --who                             # GPU/显存/CPU/内存/磁盘/IO + 占用者
dt free --json --explain                 # 资源 + 队列状态 + 可执行下一步
dt run -n exp42 -f -- python train.py     # 主入口：自动选卡、提交、跟随并透传退出码
dt run --node psibot-ds -p omnistack -n eval42 -f \
  --artifact outputs/run42/model.pt -- python eval.py
                                            # 固定节点时可同步显式输入并自动绑定内容
dt sync psibot-ds -p omnistack            # 增量预热节点代码缓存（rsync）
dt sync psibot-ds -p omnistack --artifact outputs/run42/model.pt
                                            # 显式预置被快照排除的大文件
dt run --node psibot-ds -p omnistack \
  --artifact-manifest <sync返回的SHA256> -n eval42 -f -- python eval.py
                                            # 绑定大文件内容，漂移则拒绝启动
dt task psibot-ds "python train.py" -p vla -n exp42 -f
                                            # 兼容快捷方式：固定节点 + shell 字符串
dt task psibot-ds "python train.py" -p vla -n exp42 \
  --require-disk-gib 80 -f                 # 预计会写大文件时声明任务磁盘合约
dt task psibot-ds "python train.py" -p vla -n exp42 \
  --max-hours 12 --max-vram-mib 23500 \
  --max-job-memory-mib 60000 -f             # 超时、显存或任务主机内存越界即回收
dt batch psibot-ds "python train.py --lr 1e-4" \
  "python train.py --lr 3e-4" -p vla -n lr-sweep
                                            # 一次装入同节点 FIFO，共用精确代码快照
dt chain psibot-ds "python guard.py" "python train.py" \
  "python evaluate.py" -p vla -n guarded
                                            # 前项成功才自动启动后项；失败项不占 GPU
dt chain psibot-ds --stage-gpus 0 --stage-gpus 1 \
  "python preflight.py" "python train.py" -p vla -n guarded-train
                                            # CPU 预检成功后才申请 GPU
dt task psibot-ds "python next.py" -p vla -n next \
  --after-success EXISTING_JOB
                                            # 用当前代码追加到已有运行任务之后
dt run -g 2 -n exp42 -- python train.py   # 非跟随提交（stdout 末行是 job id）
dt run -c auto -g 2 -- python train.py    # 笔记本：自动挑空闲最多的中心
dt ps                                     # 默认只看 queued/running，不混入历史
dt ps --recent                            # active + 最近 10 条结束记录
dt ps -a                                  # 明确请求完整历史
dt ps --active --json                     # 只取 queued/running，适合轮询与自动化
dt ps --json --limit 30                   # 只取最新 30 条，避免大历史库反复传全量
dt ps -s failed                           # 失败任务自动显示根因，不额外探测日志/GPU
dt ps --issues                            # 只显示失败、lost、非零退出和阻塞任务
dt ps --watch -s running                  # 多任务实时状态、进度、GPU 与异常
dt ps -w                                  # 展开 job id 与命令
dt info exp42                             # 单作业全景 + 最近 3600 条资源样本摘要
dt info exp42 --metrics-tail 0            # 同一视图汇总全部持久化资源样本
dt watch exp42                            # 持续刷新资源、状态与日志直到结束
dt watch exp42 exp43 exp44                # 同屏跟踪同一 center 的多个任务直到全部结束
dt metrics exp42                          # 结束后汇总 GPU/CPU/内存/IO 历史指标
dt logs exp42 -f                          # 持续看日志；SSH 抖动自动重连
dt wait exp42                             # 等结束；失败透传退出码并自动附日志尾部
dt wait exp42 exp43 exp44                 # 并发等整组；汇总全部失败与日志
dt rerun exp42                            # 当前代码 + 相同命令/资源/前序依赖，保留 rerun_of 谱系
dt fork exp42 -n exp42-ab -- python ab.py # 精确复用旧代码快照，可替换命令
dt compare exp42 exp42-ab                 # 审计快照/环境/节点/GPU 等实验控制
dt compare a1 b1 b2 a2 --metric 'runs/**/training_report.json::throughput.samples_per_sec' --groups ABBA --unit samples/s
                                            # 同时汇总 A-B-B-A 均值、波动与提升
dt compare a1 b1 b2 a2 --metric '@job::duration_s' --groups ABBA --lower-is-better --unit s
                                            # 直接比较 head 注册表中的完整任务时长
dt compare a1 b1 b2 a2 --metric 'runs/**/training_report.json::throughput.samples_per_sec' --groups ABBA --min-improvement 1 --max-spread 0.5
                                            # 候选提升不足或任一组波动过大时返回 1
dt compare a1 b1 b2 a2 --metric 'runs/**/training_report.json::throughput.samples_per_sec' --groups ABBA --max-regression 0.5 --max-spread 1
                                            # 非劣门槛：允许候选最多回退 0.5%
dt pull exp42                             # outputs/ 拉回主节点（断点续传）
dt pull exp42 exp43 --collection sweep42  # 托管整组，不在代码仓库制造 results/
dt pull exp42 --to ./report                # 确实需要时才显式写入当前项目
dt pull exp42 --lite                      # 只拉轻量证据，跳过 checkpoint、缓存与原始 profiler trace
dt pull exp42 --exclude checkpoints/      # 只拉监控/报告，跳过大 checkpoint
dt storage                                # 盘点 head、节点、环境和结果占用
dt compact --before 2026-07-01 --plan     # 校验恢复快照并预览可移除的冗余 code/
dt compact --before 2026-07-01 -y         # 只移除可从精确快照恢复的老任务 code/
dt clean --before 2026-07-01 -p smoke --plan
                                            # 精确预览某项目；-p/--project 可重复
dt clean --before 2026-07-01 --results --envs --plan
                                            # 先预览；去掉 --plan 并确认后才清理
dt kill exp1 exp2                         # 终止指定任务
```

`dt compact` 与 `dt clean` 的用途不同：前者保留任务目录、输出、日志、checkpoint、
启动载荷和注册表，只回收已经由不可变精确快照覆盖的 `code/` 副本；后者按显式范围
清理旧任务、托管结果或环境，并可用可重复的 `--project` 限定历史项目。`compact`
会在接触节点前重算所有相关快照的
SHA-256，并拒绝路径、符号链接、身份或归档不一致的候选。成功后写入
`code-pruned.json`，重复执行会稳定报告 `already_compact`。

要让一张卡连续跑一组实验，提前把任务都提交到同一节点即可；不要给每条任务加
`-f`，否则 shell 会停在第一条的本地监控上：

```bash
dt task psibot-ds "python train.py --cfg a" -p omnistack -n dp-a
dt task psibot-ds "python train.py --cfg b" -p omnistack -n dp-b
dt task psibot-ds "python train.py --cfg c" -p omnistack -n dp-c
dt ps --watch                             # 一屏看 running + queued
```

第一条会占用空闲卡，其余任务默认进入 FIFO 容量队列；前一条进入终态后，常驻
agent 自动派发下一条，前一条失败也不会让后续任务饿死。队列活跃时，agent
会为正在运行的 dt wrapper 建立轻量完成观察连接，退出标记一出现就立刻唤醒
派发；连接断开或 GPU 被外部任务占用时仍由 `active_poll_s` 可靠兜底。
运行任务存在但队列已经为空时，`dt free` 会提前提示
`queue ends after N running job(s)`，并给出固定到当前节点的
`dt task NODE 'COMMAND' -n NAME` 后继提交命令；若此刻还有空卡，则直接指出
可立即提交的新节点，并在它与唯一运行节点不同时同时给出 `keep busy` 命令。
这样既不会把“扩展到空卡”和“给当前节点续航”混为一谈，也无需等当前任务结束、
GPU 真正空闲后才发现队列断粮。
`dt agent status`
可检查 agent、队列深度、当前队首和历史记录规模，并给自适应控制器提供稳定的
`handoff_state`：`covered` 表示已有排队后继，`prepare` 表示运行任务结束后
队列会断粮，`ready` 表示队列已空、可以提交下一任务；agent 停止或 registry
损坏时分别 fail-closed 为 `agent_stopped` / `registry_degraded`。dt 只报告
交接时机，不会在 head 上执行任意回调或虚构实验任务。agent 每轮只解析一次
registry 快照，并在队列内部增量维护运行数，因此历史和队列增长不会产生
“每个排队任务重扫全部历史”的开销。要撤掉尚未运行的任务，用
`dt kill JOB -y`。所有任务在提交时各自冻结代码快照，所以排队期间继续改源码
不会悄悄改变已排任务。若节点有多张空闲卡，调度器会按每条任务的 `-g` 尽量
并行利用容量；固定到单卡节点时自然就是逐条连续运行。
常驻 agent 的日志使用 copy-truncate 自动轮转：`agent.log` 达到 10MiB 后保留
两份历史，不替换 nohup/crontab 已打开的 inode；`dt agent status` 同时显示
当前日志大小、上限和 JSON 中的 `log_bytes/log_max_bytes/log_backups`。
检测到 dt 源码变化时，agent 会在仍持有调度锁的情况下先对包内全部 Python
文件做无 bytecode 写入的语法检查，再验证新 CLI 可导入；`agent.py` 等惰性
模块的中间态错误也不会让旧 agent 退出。同一失败指纹只记录一次，下一次源码
变化后才重试热重启。

命令清单可用一次调用装入队列：
`dt batch NODE "CMD1" "CMD2" ...`，或把每行一个 shell 命令写入
`dt batch NODE --file commands.txt`。空行和整行 `#` 注释会被忽略，
`--file -` 从 stdin 读取。dt 只捕获一次当前代码：首项正常调度，其余项以
同节点、同 snapshot 的精确 fork 直接进入 FIFO，不重复探测已被首项占用的
GPU。每项仍是独立 job，拥有自己的状态、退出码、日志、telemetry、pull 和
rerun；前项失败不会阻止 agent 启动后项。名称默认为
`<文件名或batch>-001-<命令名>`，可用 `-n PREFIX` 替换前缀。

有前后依赖的阶段改用
`dt chain NODE "GUARD" "TRAIN" "EVAL"`，或
`dt chain NODE --file stages.txt`。它同样只捕获一次代码、为每一阶段保留独立
job，但阶段 N 只有在阶段 N-1 以退出码 0 结束后才会被派发。等待依赖时不探测、
不租用 GPU，也不阻塞队列里无关的可运行任务；任一前项失败、被终止、丢失或
不存在时，所有后继项会在同一 agent 轮次内短路为 failed-before-start。
`--json` 返回 `dt_chain_v1`，并明确给出
`runtime_failure_policy: stop`、`dependency_policy: previous_success` 以及每项的
`after_success`。独立参数扫描仍应使用 `batch`，因为它的失败继续语义不变。
各阶段资源需求不同可按顺序重复传入 `--stage-gpus N`，次数必须与阶段数一致；
例如 `--stage-gpus 0 --stage-gpus 1` 会让预检阶段不租用 GPU，成功后训练阶段
才进入 GPU 容量队列。后继阶段在同一节点启动时可通过
`$DT_PREDECESSOR_OUTPUTS` 读取前序成功阶段的 `outputs/`，并用
`$DT_PREDECESSOR_META_PATH` 审计其调度元数据；活跃链路会阻止 `dt clean`
提前删除该目录。这样 CPU 预检生成的计划或缓存可以直接交给 GPU 阶段，无需
重新计算。未传该参数时，所有阶段仍统一使用 `-g/--gpus`。
若前项已经存在，可用 `dt task ... --after-success REF` 或
`dt run ... --after-success REF` 把当前代码的新任务追加到它后面。dt 会先冻结
新任务快照、强制入队，并在可确定时自动固定到前项节点；前项失败则后项在占用
GPU 前短路失败。该选项与 `--no-queue` 互斥。

人类模式在每项注册成功后立即向 stdout flush 一个裸 job id，可直接重定向
保存；`--json` 返回一个 `dt_batch_v1` 回执，并以
`runtime_failure_policy: continue` 明确运行期失败策略。回执中的
`next_commands.watch/wait/pull/kill` 是可直接执行的 argv；两项及以上还包含
`compare`。其中 kill argv 故意不带 `-y`，不会把回执本身变成破坏性授权。
人类模式在 stderr 给出常用的 monitor/wait/recover 下一步，同时保持 stdout
只有裸 ID。ID 较多时可一次保存并贯穿后续链路，无需复制长 ID：

```bash
dt batch psibot-ds --file commands.txt -p omnistack -n dp-sweep \
  | tee dp-sweep.jobs
dt watch --file dp-sweep.jobs
dt wait  --file dp-sweep.jobs
dt pull  --file dp-sweep.jobs --collection dp-sweep
dt compare --file dp-sweep.jobs
# 只在确实需要取消整组时：
dt kill --file dp-sweep.jobs -y
```

`watch/wait/pull/compare/kill -F/--file` 都接受每行一个 job id、前缀或名称，
忽略空行和整行 `#` 注释；`--file -` 从 stdin 读取，不能和位置 refs 同时
使用。文件顺序保持不变：它决定 compare 的指标/groups 对齐和各聚合结果的顺序。
`kill --file` 仍逐项执行原有确认与死亡验证；JSON 或其他非交互调用必须显式
`-y`，无法确认死亡的任务保持原状态。
中途提交失败时，回执
保留全部已注册 job、结构化根因和未提交数量，不回滚或重复提交。head 端
Ctrl-C 返回 130：human 已确认 ID 不会丢失；JSON 输出一个 partial/unknown
回执，区分 `confirmed_submitted` 与 `uncertain_batch_index`，明确当前项
outcome unknown、已确认任务未取消，并提示按 prefix 检查，禁止盲目重提。
若 Ctrl-C 发生在 artifact sync 阶段，回执明确尚未注册任何 job，原 partial
传输可由相同命令恢复。从 laptop 使用 `--file` 时文件在本地解析后才发送到
head；断线但未收到完整回执时结果标记为 unknown，必须按名称前缀检查
`dt ps -w`。重复 `--artifact` 可一次同步输入，并把得到的 manifest 自动绑定
到批次内每个任务。

`dt task ... -f` 在任务正常结束时透传训练退出码；按 Ctrl-C 只停止本地
监控，不会取消排队中或运行中的任务。命令会同时显示恢复监控的
`dt watch ID` 和显式终止任务的 `dt kill ID -y`。从 laptop 使用时，task
只提交一次并取得 job ID，之后 `watch` 与终态 `wait` 都会在到 head 的 SSH
断开后自动重连；因此恢复监控不会重复提交实验。若提交阶段在收到 ID 前断线，
命令会把结果标为 unknown 并提示按任务名检查，绝不会盲目重提。
`dt task ... -f --json` 把提交回执、完整 watch 帧和最终 wait 结果依次输出为
JSON Lines，并保留训练退出码；从 laptop 仍然只提交一次，后续 JSON watch/wait
连接可以安全重连，适合 CI、实验控制器和无人值守任务直接消费。若 Ctrl-C
发生在 watch 阶段，最后追加一个 `watch_interrupted` 对象并退出 130，其中
包含保留原 poll/lines 的恢复 watch 命令和显式 kill 命令；不会继续误入 wait。
若发生在终态 wait 阶段，则追加 `wait_interrupted` 和精确 wait 恢复命令。
两者都不取消远端任务，控制器无需用 EOF 猜测是终态还是主动分离。
`dt watch`、`dt wait`（因此也包括 `dt task ... -f`）会为每个运行任务最多
保持一条静默 completion 连接；远端退出标记一出现就唤醒下一次权威状态刷新，
因此 `--poll` 只决定周期性的资源/日志刷新和断链兜底，不再给快速结束额外增加
完整轮询周期。连接异常后该任务在本次监控中只退回轮询，不会形成重连风暴；
终态、异常和 Ctrl-C 都会回收连接。
独立 `dt watch --json` 使用相同的 JSON Lines 分离契约：已有完整监控帧不会
被破坏，末行增加 `watch_interrupted` 并退出 130。laptop 转发会区分本地
Ctrl-C 与 head 已经返回的 130，避免重复中断对象或错误进入下一监控阶段。
失败任务的 primary 日志已在最终 watch 面板显示时，终态 wait 不会再重复打印；
它仍会解析安全的 `see outputs/...log` 引用并补充真正的嵌套根因。
若嵌套日志显示 SIGKILL/137，且持久化遥测同时证明主机内存超过 95%、作业
PSS/RSS 占主机内存至少 75%，`dt wait` 还会输出结构化
`failure_hint.kind=probable_host_oom`，列出主机峰值、作业 RSS/PSS 与降内存
建议；证据不足时保持原始退出码和日志，不把普通 SIGKILL 猜成 OOM。

同一安全提交契约覆盖 laptop 上所有会创建实验的命令：
`dt run/task/rerun/fork` 都只发送一次并暂存完整 stdout。即使 SSH 最后返回
255 或 Ctrl-C 的 130，只要完整 job ID 已返回，就说明 head 注册表已经保存
该实验，dt 会原样输出 ID/JSON、返回成功且不重提；没有 ID 时则分别返回稳定
退出码 5/130。`--json` 输出统一的 `submission_unknown` 对象，人类模式明确
提示 `outcome unknown`、`Do not resubmit blindly` 和 `dt ps -w` 恢复路径。

`dt run/task` 在访问配置、探测节点或创建快照前校验提交请求：`-g 0`
表示 CPU-only，GPU 数不能为负；提供 `--max-hours` 时必须是有限正数；
`--max-vram-mib` 必须是正整数且只能用于 GPU 任务；
`--max-job-memory-mib` 必须是正整数且也支持 CPU 任务；空命令也会被拒绝；
提供 `--require-disk-gib` 时必须是正整数。加 `--json` 时这些错误
统一为 `invalid_argument`，退出 1。`dt run`/`dt fork` 的远端命令必须放在
显式 `--` 之后；此前置的未知 dt 选项会在本地返回 2，并给出最接近的合法选项，
不会再被静默当成远端可执行文件。
提交成功或入队时，人类界面和 JSON 都会显示最终解析出的 `project`；从未配置
目录触发 `default_project` 回退时因此会立即可见，可用 `-p PROJECT` 显式覆盖。
若 uv 或项目 `setup` 在训练进程启动前失败，提交仍以退出码 3 结束，但不会
丢失任务身份：注册表会保存 job id、尝试节点和失败原因；人类输出自动附
`logs/env.log` 尾部，`--json` 在单个对象中增加 `job_id/node/failure_log`。
明确的损坏 wheel 会先经过上述一次精确缓存自愈；仅当重试仍失败时才进入这条
env-fail 契约。
之后仍可用 `dt info ID`、`dt logs ID`、`dt watch ID` 或 `dt wait ID`
复查同一根因；`info --json` 同样包含结构化 `failure_log`。

`dt sync NODE... -p PROJECT` 把代码精确镜像到节点的
`~/dt/sync/<project>/code/`。它只写 dt 专用缓存，支持断点续传和重试；
执行前可加 `--plan`，由 rsync 真实比较本地代码和远端缓存，预览将传输的
精确字节数及将删除的项；即使缓存尚不存在也不会为预览创建目录或写入远端。
重试只用于 SSH/链路/超时以及传输时刚好消失的源文件；权限、磁盘、路径、
协议等确定性错误立即返回，不会先静默等待 5 秒、10 秒再显示同一根因。
链路类故障默认在首次尝试后重试 2 次；排障时可用 `--retries 0` 快速失败，
也可用 `--retries N` 明确限制额外尝试次数。每次真实退避会立即在 stderr
显示节点、sync phase、尝试数、退出码、等待时间和简短原因；stdout JSON
仍保持纯净，最终对应节点行仅在确实重试时增加 `retry_events`。命令级
`transferred_bytes/transferred_files/deleted_files` 会合并所有尝试中 rsync
已报告的工作，因此“远端更新已落地、最终响应丢失、幂等重试无变化”不会把
本次同步误报成零传输。
`dt run` 仍创建不可变快照，但会把这个缓存作为服务端复制基线，因此首次派发
也只传变化的文件，同时不会让可变缓存与作业快照共享 inode。

若任务需要读取被快照排除的 checkpoint、数据或其他大文件，用可重复的
`--artifact PROJECT_RELATIVE_PATH` 显式同步。只要出现该选项，本次命令就只
同步点名的文件/目录，不改代码缓存；远端保持原相对路径，任务内从绝对路径
`$DT_ARTIFACT_ROOT/<relative-path>` 读取。文件增量覆盖，目录是精确镜像；
绝对路径、`..`、重叠选择、缺失项、软链接和特殊文件会在联网或写远端前失败。
`--plan` 同样只读，真实传输支持 partial、checksum、重试和跨进程串行。
显式 artifact 保持逐字节精确语义，不套用代码 snapshot 的 ignore 规则；
若目录含 `__pycache__`、`*.pyc`、pytest/mypy/Ruff 缓存等常见临时文件，
sync 会在联网前提示，并在 JSON 的 `transient_files` 中列出有界清单，但仍将
这些文件纳入 hash 和传输。若不是有意输入，应先移除或改为逐文件
`--artifact`。
artifact 位于 `~/dt/artifacts/<project>/`，不属于不可变代码 snapshot；要求
权重不可变的实验应把 sync 返回的 `artifact_manifest_sha256` 传给
`dt run/task --artifact-manifest SHA256`。sync 会在传输前后重算来源，拒绝
同步期间发生的写入，并把内容寻址清单保存在远端
`.dt/manifests/<sha256>.json`。launcher 在 uv setup 和训练前重新验证所选
文件/目录；路径、类型、权限、大小或内容发生漂移时任务以 failed-before-start
结束，`logs/env.log` 保留精确根因。校验耗时单独记录在
`launch_phases_s.artifact_verification`，人类 `dt info` 显示为
`artifact verify`；大型实验应只声明任务实际读取的最窄文件/目录集合，
避免每次启动重复校验无关数据，同时不降低逐字节完整性。绑定会写入 registry、`dt info`、
submission JSON 和 pull 回的 `dt/job.json`，并由 rerun/fork 自动继承。
指定单台服务器时，推荐直接使用
`dt run --node NODE --artifact PATH -- COMMAND`；兼容的
`dt task NODE "COMMAND" --artifact PATH [--artifact PATH...]` 具有相同语义。
dt 会先增量同步这些项目相对路径，再自动绑定返回的 manifest 后提交任务。
JSON 响应额外包含 `artifact_sync` 传输证据；它与手工
`--artifact-manifest` 互斥。

Python 工具缓存 `.mypy_cache/`、`.ruff_cache/`、`.hypothesis/` 与覆盖率/
pytest 缓存不会进入 sync 或作业快照；过滤规则新增后，下一次精确同步也会
清理远端镜像中旧版本遗留的 excluded 文件，而不是只停止后续传输。
多节点同步有界并行执行、按调用顺序输出结果，整体耗时由最慢节点主导而不是
逐节点相加。JSON 会保留每个节点的结果；若失败全部属于 SSH/rsync 链路
不可达，整体返回 5，任一数据或权限类失败则返回通用 1，便于脚本准确决定重试。
在 head 上按 Ctrl-C 会协作终止本次多节点同步的所有本地 rsync 子进程，
保留 cache 和 partial 数据，退出 130，并输出包含原参数、可直接复制的恢复
命令；`--json` 仍只输出一个稳定的 `sync_interrupted` 对象。
同一节点、同一项目的多个 sync 会跨进程串行，避免两个 `--delete` 同时修改
缓存；作业快照读取缓存期间也受共享锁保护。若 sync 已在写，提交不会排队等待
这个可选加速层，而是立即绕过缓存、继续从不可变源码快照传输。
从 laptop 发起时，dt 会先只读确认 head 可达，再允许同步开始；启动后的外层
SSH 链路若中断，会按退避等待 head 恢复并重新进入同一个带锁同步，所以不会
并发破坏缓存。失败尝试的半截 stdout 不会污染终端或 `--json`；Ctrl-C 只停止
laptop 上的等待，保留 head 缓存和 rsync partial 数据，并打印可直接复制的
恢复命令。
未知节点或项目解析等前置失败在 `--json` 下也会输出稳定的
`error/message/exit_code` 对象，不会留下空 stdout。多节点部分失败的每一行
额外提供稳定的 `error_kind/message/exit_code`；兼容字段 `error` 继续保留原始
自由文本，因此旧脚本无需迁移，新脚本也不必解析 SSH 错误字符串。
成功 JSON 同时提供精确 `transferred_bytes`、`deleted_files` 和兼容的
`transferred_gib`，并直接报告 rsync 实际复制的 `transferred_files` 与包含
锁等待、远端准备和传输的 `duration_s`；plan 结果另含 `plan:true` 与
`cache_present`；
人类界面按 B/KiB/MiB/GiB 自适应显示，小改动不再被四舍五入成 `0.00 GiB`，
同时显示文件数和毫秒/秒耗时；删除同步会显示 `1 deleted`，严格幂等则明确
显示 `no changed bytes`。2026-07-24 在 `psibot-ds` 对 OmniStack 实测：
零变更 plan 5 次为 0.21–0.23 秒（中位数 0.22 秒），770,950 B 增量实传
0.24 秒，46 B 单文件实传 0.16 秒，精确删除 1 项 0.13 秒。该结果没有证明
冷缓存首传性能（远端缓存原本已存在），因此保留 `--checksum` 的强一致性，
不以未测量的假设换取风险。

`dt doctor` 发现远端 PyPI 网络慢或阻断时，会先提示
`dt seed NODE... --plan`。plan 只统计 head 上的本地源规模，不连接或写入远端；
这是一份传输上界预览，目标端已有内容会由 rsync 跳过。确认容量后去掉
`--plan` 执行，seed 会在连接远端前再次显示本地源规模。seed 从 head 增量推送
uv 下载缓存和 uv 管理的 Python；加 `--hf` 才额外推送本地 Hugging Face 模型。
多节点有界并行且按输入顺序报告，重复执行显示 `no changed bytes`。`--json`
为每个节点和组件提供 `status/source_bytes/transferred_bytes`；远端准备或 rsync
链路失败返回 5，权限/数据失败返回 1，已有成功组件时标记 `partial:true`，
便于修复后直接重跑。同一计算节点的 seed 会跨进程串行，避免两个 rsync 同时
改写 uv/HF 缓存；不同节点仍并行。从 laptop 发起时会先只读确认 head 可达，
启动后的外层 SSH 断线按退避重连，并重新进入同一个节点锁；失败尝试的半截
stdout 不会污染终端或 `--json`。Ctrl-C 只停止 laptop 上的等待，远端缓存和
rsync partial 数据均保留，同时打印包含原参数、可直接复制的恢复命令。
seed 默认在首次尝试后重试 1 次；`--retries 0/N`、逐节点/组件 stderr 进度、
按需出现的 `retry_events` 以及跨尝试累计传输统计与 sync 使用同一契约。

无空闲卡时 `dt run` 默认进队列（快照在提交时落盘，排队期间改代码不影响
已提交作业），主节点 agent 空闲时按 `poll_s` 做维护轮询；队列活跃时，自己
运行的 dt wrapper 由持久完成观察连接在退出时立即唤醒，外部 GPU 占用或观察
连接断开则按 `active_poll_s` 快速重试容量。新任务入队也会用轻量 wake 文件
立即打断空闲睡眠。`--no-queue` 恢复
"无卡直接退出码 2"。agent 由 `dt run` 入队时自动拉起，`dt agent install`
写 crontab `@reboot` 保证主节点重启后队列不停摆。每轮派发前，agent 会并行
刷新 running 任务：完成或丢失的任务会自动收敛并立即释放 `max_my_jobs`
配额，不再依赖用户先运行一次 `dt ps/info/wait`。节点暂时不可达时保持最后
可信状态且不重复写日志；新出现的 lost 只通知一次，并在短暂窗口内继续探测
迟到的退出标记，历史 lost 不产生永久后台 SSH 流量。

缺少 `--require-path`、不满足 `--require-disk-gib`、节点工具不完整等
job-specific 条件会保留任务等待重试，但不会阻塞后续可运行任务；
`dt task/run`、`dt info`、`dt watch` 和
`dt ps --watch` 都会显示 `blocked: ...` 的具体原因。条件恢复或转为纯容量等待
后，旧 blocked 原因会自动清除，避免把过期错误继续展示给用户。
调度器会用同一次资源探针提前排除“已知磁盘不足”的节点，避免无效 snapshot
传输；遥测缺失时不会武断拒绝，而由远端 launcher 在真实 job 文件系统上复核。
实际生效值是任务声明和中心 `disk_min_gib` 的较大者，并被冻结到 registry，
因此 queue、rerun、fork、batch 与 laptop 转发都不会丢失该资源合约。

显式指定节点并使用 `--no-queue` 时，GPU/配额确实不足返回 2；如果所有实际
候选尝试都失败在 SSH/远端链路边界，则返回 5，不再误报成“无卡”。带 `--json`
时，提交失败只在 stdout 输出一个稳定对象：
`error/message/reasons/exit_code`；环境或 setup 失败继续使用退出码 3。进度信息
仍只写 stderr，因此 JSON 可以直接送入 `jq`。节点探针已明确判断不可达时会
跳过必然失败的 snapshot/launch 往返，快速返回；不加 `--no-queue` 时仍在本地
完成不可变快照并入队，等待节点恢复。`dt free --json` 对应行包含
`unreachable: true`，GPU 工具自身报错则保持 false，便于脚本区分链路和环境。
从 laptop 使用 `dt run -c auto` 时也遵守同一边界：找到任一满足容量的可达
center 就正常提交；没有候选且部分/全部 capacity probe 不可达时返回 5，
协议错误返回 1；只有所有 center 查询可信且确实无容量时才返回 2。以上三种
`--json` 路径都有完整 `error/message/reasons/exit_code`，不再留下空 stdout。
常驻 agent 重试 pinned 队列任务时也只探测该节点；已知 busy/offline 会在
snapshot/launch 前短路，避免每个轮询周期重复传输或制造失败日志。离线入队
会立即记录 `waiting: <node> unreachable: <detail>`，agent 轮询期间持续保留；
节点恢复为可达后自动清除。该原因可在提交结果、`info/watch`、JSON 和
`ps --watch` 中看到，紧凑 `dt ps` 则直接标为 `queued offline`。`dt wait`
启动时也立即打印当前排队原因，只在 offline/blocked/cleared 状态切换时提示，
不会因 timeout/no-route 文本来回变化而每轮刷屏。
提交瞬间容量不足时，stderr 会同时保留每个候选节点的探针结论，并直接附上
忙卡占用者、显存和利用率（例如
`psibot-hm: 0 free < 1 wanted; busy: gpu0 psibot 3.8/31.8GiB util25%`），
便于解释“刚才 free、现在却排队”的瞬时竞争；这些详情复用触发调度决策的同一
次探测，不会为了报错再查一次而制造时序错觉。容量等待会立即持久化为
`waiting: no free capacity (...)`，后续轮询按最新占用者更新；它不是
job-specific blocker，agent 仍按 FIFO 自动重试。批量强制排队项使用
`waiting: batch FIFO`，配额等待使用 `waiting: max_my_jobs=N reached`。

如果 launcher 的 SSH 在返回启动结果前掉线，dt 只在远端取消哨兵、tmux session
和 job-cwd 进程都得到明确死亡确认后才尝试下一节点。取消无法确认时立即停止
failover，避免同一实验在两个节点重复运行；直接提交返回 5，并保存包含 job ID、
实际尝试节点和 `launch outcome uncertain` 的 failed registry 记录，queued
任务则原地转为同样可检索的 failed 状态。取消成功但 launcher 已短暂启动时，
历史记录会保留实际节点、PGID、GPU 和真实起止时间。节点恢复后可对这类
failed 记录再次执行 `dt kill REF -y`：dt 会重写取消哨兵、关闭 session 并扫描
job-cwd；只有明确确认无存活进程才转为 killed，仍不可确认则保留原失败证据。
这类记录仍可用 `logs/watch/pull/metrics/attach` 访问实际节点上的证据，不会被
误报成“failed before starting”；`wait` 会明确提示检查证据和重试清理命令。

`dt kill REF -y` 只有在远端进程组确认死亡后才写 `killed`；无法确认时保留
running，避免伪造终态。running kill 和 queued dequeue 都记录 `finished_at`
与明确的用户终止原因，因此 `info/ps` 的历史时间线不会残缺或继续展示旧 blocker。
同一 job 的 refresh 与 kill 在头节点串行化：wrapper 迟到的 exit 143 不会把
显式 killed 覆盖成 finished，正在等待的 `dt wait` 稳定返回 killed 码 66。
queued dequeue 与 agent 派发使用原子状态提交：耗时的 rsync/uv setup 不持锁，
所以 `dt kill` 可立即 dequeue；dispatcher 在每个出口重新确认状态，若 launcher
已成功则用与 running kill 相同的进程组 + job-cwd 扫描确认死亡。最终
`queued → running/failed` 提交仍按 job 加锁，不会把 `killed` 静默覆盖。
若 SSH、异常响应或存活进程使取消无法确认，registry 会恢复为真实的 `running`
节点/PGID，agent 写入并通知 `cancel_failed`，`dt ps` 标红
`running cancel!`；该原因会保留到再次 kill 或任务结束，不会被普通 RUNNING
探针清除。
自动化清理可用 `dt kill REF... -y --json`；stdout 是按输入顺序排列的单个结果
数组，逐项包含 `outcome/status/reason/message/exit_code`。`dequeued`、
`killed`、`already_terminal`、`not_found`、`survived` 和 `unverified` 不需要
解析人类文本；JSON 模式强制 `-y`，且任何 `unverified` 仍保持原 registry 状态，
不会为了输出成功而弱化死亡确认。
整组 refs 可来自 `dt batch` 保存的 ID 文件：
`dt kill -F JOBS.txt -y --json`；读取文件不改变上述确认、结果顺序或死亡证明
要求，也不会把 `--force` 隐式打开。

`run/task/rerun/fork --json` 共享同一提交基础字段：
`job_id/status/node/gpus/session/job_dir/snapshot_sha256/payload_sha256/reason`；rerun/fork
只追加各自的 lineage 字段。若成功前有节点因 snapshot、链路或 launcher
条件被拒，额外返回 `placement_failures:{node:reason}`；没有 failover 时不输出
该字段。完整映射会持久化到 registry、`info`、`wait` 和 pull 回来的
`dt/job.json`，不再只存在于提交时 stderr。这样自动化脚本不必按入口维护不同
解析器。

`dt ps` 默认只显示 queued/running，一项任务只占一行，集中显示名称、四位
短 ref、节点、GPU、状态/退出码和时间；短 ref 可直接传给 `info/logs/wait/pull`。
80 列下优先保留 ref、完整节点、GPU 和异常状态，名称再按剩余宽度省略。
`dt ps --recent` 追加最近 10 条 terminal 记录，`dt ps -a` 才读取完整历史。
`dt ps -w` 增加完整 job id 与命令；默认 `dt ps --json` 仍返回全部任务和完整
机器可读字段，自动化契约不受人类默认视图变化影响。
laptop 的人类表格不会再为此拉取每个 center 的全部历史：head 返回带原始总数
的 `dt_ps_window_v1`，包含全部 active 任务和足以合并出全局最近 10 项的
窗口。`-a` 和公开 `--json` 继续走全量
契约；旧 head 不支持窗口时自动回退兼容的全量数组。
过滤 `failed` 或 `lost` 时，最后一列自动从时间切换为 `issue`，直接显示
registry 原因；`--issues` 进一步过滤为真正需要处理的任务，成功和正常 killed
记录不会混入。该模式只使用已有状态字段，不会为了展示失败原因额外访问日志
或 GPU。已结束但退出码非零的训练会直接提示带短 ref 的 `dt logs abcd`；
旧 registry 中没有 reason 的 lost
任务显示 `exit marker missing`。节点可达并再次确认 wrapper 已消失后，
dt 会把包含 wrapper PID 和 exit marker 路径的精确原因回填到 registry/JSON。
运行任务的状态探针连不上节点时，紧凑表直接显示 `running? offline`；若注册表
计时同时超过 `--max-hours`，追加 `>max`。JSON 对应提供
`node_unreachable`、`status_probe_error`、`max_hours_exceeded` 和
`max_hours_overdue_s`。这只是最后可信状态上的瞬时告警，不会把断网误判成任务
死亡。排队任务会优先显示原始 pinned 节点，并把 job-specific 阻塞标为
`queued blocked`、链路不可达标为 `queued offline`，无需进入详情页才发现问题。
所有 queued 行同时显示 `#位置/深度`，例如 `queued #2/5`；`ps --json` 和
`info --json` 提供 `queue_position`、`queue_depth`、`queue_ahead_count`、
`queue_head_job_id`、`queue_predecessor_job_id`。`dt info` 的人类输出也直接列出
队列位置、队首和直接前项，任务启动或离队后这些字段统一回到 `null`。

对显存敏感的任务可设置 `--max-vram-mib N`。dt 的 1 Hz telemetry 会检查每张
已分配 GPU 的整卡已用显存；任一卡严格超过阈值时，先原子写入
`outputs/dt/resource-guard.json`，再向完整任务进程组发送 TERM，同时逐个终止
已逃逸进程组的后代，短暂宽限后用 KILL 清理仍存活的后代。该记录包含
GPU、观测值、阈值、阶段和时间；`dt info --json` 返回
`max_vram_mib/resource_guard`，人类版 info 也显示熔断详情。run、task、batch、
排队重放、rerun 和精确 fork 都会保留该合约；fork 可显式覆盖阈值。

对主机内存敏感的任务可设置 `--max-job-memory-mib N`。dt 对完整任务进程树
归因内存：优先匿名 PSS，缺失时依次回退到 PSS、RSS，避免把共享页和 CUDA
设备映射简单重复计数。严格越界后的证据与回收流程和显存保护相同；
`resource_guard.observed_metric` 明确记录本次使用的计量口径。该保护同时支持
GPU 与 CPU-only 任务，并由 run、task、batch、rerun 和 fork 继承。

`dt ps --watch` 原位刷新多任务面板，并从每个运行任务的活跃 stdout 或
`outputs/**/*.log` 并行提取 step、百分比、ETA 和吞吐；解析或日志访问异常
显示在同一 `progress` 列。常见 SSH 错误压缩为
`offline: no route/timeout/refused`，终态任务不再显示陈旧的 queued blocker。
未按状态过滤且存在 running、没有 queued 后继时，caption 会提前显示
`queue ends after N running job(s)` 和可执行的 `dt task` 续航命令；补交任务后
告警会随下一帧自动消失。laptop 单 center 命令显式带 `-c CENTER`，多个 center
同时断粮时只汇总数量并引导 `dt free`，不会猜错节点。`-s STATUS` 因可能过滤
queued 行而不做该推断；JSON 流保持原数组契约。
`live` 列对 GPU 使用 `index:util%/used-GiB/temperature°` 紧凑显示（例如
`0:100%/21.7G/70°`），对 CPU-only 任务显示
`C<load>/R<used-GiB>/I<IO-pressure>`（例如 `C1.5/R8.0G/I0.2%`）。同一节点
每帧只运行一次资源探针，再按任务分配卡映射，不会为每个任务重复调用
nvidia-smi。状态刷新、每节点资源探针和每任务日志读取在同一个有界并发波次中
完成，因此一个离线节点只拖慢一次最慢探测，不会串行叠加三轮 SSH timeout。
CPU-only 任务也复用节点探针；JSON 的 `resources.system` 包含 CPU、内存、
磁盘和 IO，而不是空值。用 `--poll S` 调整刷新间隔，Ctrl-C 正常退出。配合
`--json` 时，每次刷新输出一个完整 JSON 数组，包含逐卡显存总量等结构化字段，
便于脚本持续消费；进度和资源提取都在各 center 的 head 上完成，laptop 不需要
直连计算节点。
一次性 laptop `dt ps --json` 若所有 center query 都失败，会输出统一错误对象：
纯链路失败返回 5，坏 JSON/协议失败返回 1；不再以空数组和退出 0 伪装成“没有
任务”。只要仍有 center 可用，就保留其完整任务数组、在 stderr 报告缺失 center
并返回 0。持续 `--watch` 模式则保持运行，等待故障 center 恢复。

`dt run --json` 和 `dt info --json` 的 `snapshot_sha256` 标识实际下发的完整
`code/` 树（路径、类型、权限、符号链接目标和文件内容；忽略 owner/mtime）。
它在 dirty worktree 下也能区分不同实验源码，旧作业没有该字段时显示 `null`。
独立的 `payload_sha256` 标识提交时冻结并实际下发的 dt 节点运行时，包括
launcher、wrapper、CUDA allocation probe、telemetry、phase 和 artifact
校验代码。项目代码相同但 dt 运行时不同的任务因此不会再被误当成完全相同；
旧作业缺失该字段时同样显示 `null`。
对带该身份的新任务，head 会在 SSH 启动命令中内联只读 verifier，在执行
launcher、环境同步、setup 或 GPU probe 前重新计算计算节点上的七文件哈希。
不一致时 launcher 内部码 17 映射为 `payload-integrity` failed-before-start，
同时保留 expected/observed 哈希且不占用 GPU；历史任务没有身份时保持兼容。
成功校验耗时记录在 `launch_phases_s.payload_attestation`，并在 `dt info`
的人类 `prepare phases` 中显示为 `payload`。
`dt info` 的人类视图会把超长或多行命令压成带行数/字节数的单行摘要，避免
命令正文淹没状态、耗时和资源结论；需要逐字复现时加 `--full-command`。
`info --json` 始终保留原始完整命令，不受人类视图压缩影响。
提交时刻来自 head，wrapper 的开始/结束时刻来自计算节点；两台机器的 wall
clock 可能存在亚秒级偏差。人类视图因此显式标为 `submitted (head)`、
`started (node)` 和 `finished (node)`，并提示跨时钟区间只能近似比较。
`info --json.timestamp_domains` 逐字段给出 `head/node/registry/mixed` 来源，
`cross_clock_intervals_approximate` 则让自动化无需从时间倒序猜测时钟偏差。
`duration_s` 在开始和结束均来自 wrapper 时只使用同一节点时钟。

每次提交还会把这棵代码树保存到主节点的内容寻址快照库。相邻快照只为变化
文件增加存储，未变化文件在快照库内部安全去重；真正运行的作业目录始终使用
独立 inode，训练脚本改源码或权限不会污染其他作业。`dt fork REF` 从该快照库
创建新任务，默认保持原 GPU 数和实际节点，`--` 后可替换命令，适合严格 A/B：
队列派发前还会核对本地 staged `code/`；若人工检查意外生成 `__pycache__`、
`.pytest_cache` 等额外文件，会先从精确快照库恢复。传到计算节点后只对远端
`code/` 做带删除的精确收敛，再校验树哈希；既不保留旧代码垃圾，也不删除同一
作业已产生的 `logs/` 或 `outputs/`。
快照捕获、旧作业回填、直接/排队派发中的代码与 launcher 支持文件传输，都在
链路类故障后内部重试 2 次；重试进度带节点和 snapshot phase 写入既有提交日志，
无需为每次实验再增加命令行选项。

```bash
dt fork baseline -n candidate -- python train.py --variant candidate
dt fork baseline -n long-candidate --max-hours 0.5 -- python train.py
dt compare baseline candidate
```

Fork 默认继承源任务的 runaway guard；新命令预计运行更久时，用
`--max-hours H` 只覆盖新 fork（含 `--repeat` 的每一项）。该值必须有限且大于
0，并会写入 JSON 提交回执，源任务记录不会改变。

成功结束的 exact-fork 源任务还可显式复用其 `outputs/` 下缓存目录：

```bash
dt fork baseline -n warm \
  --reuse-cache outputs/.cache/torchinductor \
  --cache-env TORCHINDUCTOR_CACHE_DIR \
  -- python train.py
```

`--reuse-cache` 是明确的共享可写模式，适合累计 warmup，但后续任务会继续改变
同一 source。受控重复优先使用 `--clone-cache`：launcher 在占用 GPU 前验证
source job/snapshot/uv 环境和路径，为每个任务复制出
`outputs/.cache/dt-clone`，并在复制前后比较 source 清单及副本清单。可用时
采用 reflink，失败则普通复制。TorchInductor/Triton 缓存可能嵌入 source 的
绝对路径，因此 runner 还会进入私有 mount namespace，把 clone 映射到旧路径；
宿主机和其他任务仍看到原 source。节点必须提供可用的 `unshare`/user
namespace，否则会在训练前明确拒绝：

```bash
dt fork baseline -n isolated-warm \
  --clone-cache .cache/torchinductor \
  --cache-env TORCHINDUCTOR_CACHE_DIR \
  --repeat 4
```

四个任务都从相同 source 路径独立克隆，运行时彼此不污染。回收的
`outputs/dt/cache-reuse.json` 会记录 `mode=clone`、私有 runtime path、
source metadata identity、`private_mount_namespace` 隔离、文件/字节数和
clone 耗时。

若 REF 本身已经是带缓存绑定的 exact fork，用 `--inherit-cache` 可显式保留
该绑定，不必手工回溯原始 source job、路径和环境变量；新任务沿用 REF 的命令
与资源约束，但缓存和精确快照仍从已验证的原始 source 取得。若 REF 原本采用
clone，inherit 会再次从原始 source 创建新私有副本，而不是继承前一任务已经
修改过的副本。提交 JSON、`dt info` 和 `dt/job.json` 中，
`forked_from` 指向用户实际传给 fork 的 REF，`cache_reuse.source_job_id`
则独立记录缓存/精确快照来源；即使多代 warm fork 回到同一个缓存源，实验父子
关系也不会被压平：

```bash
dt fork warm --inherit-cache -n warm-repeat
```

新实验 runner 已通过 `dt sync --artifact` 得到内容地址 manifest、但仍要复用
旧任务的 exact snapshot/verified cache 时，可在 fork 上显式覆盖 artifact
绑定：

```bash
dt fork cache-source --clone-cache outputs/.cache/torchinductor \
  --cache-env TORCHINDUCTOR_CACHE_DIR \
  --artifact-manifest NEW_MANIFEST -- python '$DT_ARTIFACT_ROOT/outputs/new-runner.py'
```

这只替换新任务的 artifact contract；代码 snapshot、cache source、环境和
fork lineage 不变。launcher 会在 setup/训练前验证新 manifest 的路径、类型、
权限、大小和内容；无效、缺失或漂移的 manifest 仍会 failed-before-start。
`--repeat` 的每一项获得同一覆盖值，laptop 转发保持逐字不变。

需要把同一个 exact/cold/warm 配置连续跑多次时，用一次调用提前装满 FIFO：

```bash
dt fork warm --inherit-cache --repeat 4 -n abba-warm | tee abba-warm.jobs
dt watch --file abba-warm.jobs
```

名称固定为 `abba-warm-001`…`004`。首项正常调度，后续项固定到同一节点并
强制排队，前项结束（包括运行失败）后由 agent 自动接续；所有项复用同一
snapshot；shared warm 模式复用同一已验证 cache source，clone warm 模式则让
每项从同一 source 获得独立副本。普通 cache-bound REF 的 repeat 为每项设置
独有的 job-local cold cache。`--repeat 1` 保持单任务输出
兼容；`--repeat N`（N > 1）的 `--json` 返回一个 `dt_fork_repeat_v1` 完整或
部分回执，stdout 人类模式只输出 job id。为保证预装队列语义，N > 1 与
`--no-queue` 互斥。

普通 `dt fork warm` 仍是 cold fork，并在 stderr 明确提示会丢掉缓存绑定；
dt 同时把 REF 记录的 cache 环境变量指向每个新任务独有的
`$DT_JOB_DIR/outputs/.cache/dt-cold`，避免节点 shell 或框架默认值把名义 cold
fork 静默变成共享 warm。
`--inherit-cache`、`--reuse-cache` 与 `--clone-cache` 三者互斥；inherit 只
适用于已有完整 provenance 且 source snapshot、uv 环境、项目和实际节点仍与
REF 一致的任务。

这不是隐式全局缓存。dt 只允许 source job 成功结束、目标保持同一精确快照与
实际节点、路径严格位于 source `outputs/` 内且 uv 环境 identity 一致时启动；
绝对路径、`..`、软链接逃逸和保留环境变量会被拒绝。launcher 在远端再次验证
source exit、snapshot、env 与 canonical path，wrapper 同时提供稳定的
`$DT_REUSED_CACHE_DIR`、设置 `--cache-env` 指定变量，并写入
`outputs/dt/cache-reuse.json`。`dt info` 与提交 JSON 保留完整 provenance。
活动中的 cache consumer 会阻止 `dt clean` 删除 source job；consumer 结束后
仍由显式 clean 生命周期管理。该契约验证来源、身份与路径边界，不对缓存字节
逐项哈希；缓存格式兼容性和条目锁仍由使用它的框架（如 TorchInductor）负责。

`dt rerun` 与它语义不同：rerun 使用当前工作区代码，适合“修复后重试”；fork
使用旧任务的精确代码，适合只改运行命令或配置覆盖的可复现实验。对绑定了
exact cache 的任务，rerun 会在提交前拒绝并打印对应的
`dt fork REF --inherit-cache`，避免当前代码在缺失缓存环境变量时占卡后秒退。
rerun 会冻结源任务的 snapshot SHA，并在得到新快照后明确显示
`code changed OLD → NEW` 或 `code unchanged SHA`；JSON 使用
`rerun_source_snapshot_sha256/rerun_snapshot_changed` 提供同一机器可读证据。
若源任务带 `after_success`，rerun 也会保留该依赖，从而继续注入同一个
`DT_PREDECESSOR_OUTPUTS`，不会把依赖产物任务降级为无前序的普通重跑。
这些字段会持久进入 `ps/info/compare` 与 pull 回的 `dt/job.json`，多人或多
agent 同时修改工作区时不再把“当前代码重试”误认成 exact repeat。
老任务若尚未进入快照库，fork 会尝试从原运行目录回填并重新验哈希；源码有任何
变化时会明确拒绝，绝不会把近似副本标成 exact。`dt clean` 会在最后一个引用
任务被清理后回收对应快照。fork 即使先因容量不足进入队列，agent 派发后也会保留
`forked_from`；`ps/info/compare` 与回收的 `dt/job.json` 因此始终能追溯父任务。

`dt compare REF...` 在下结论前一次审计两个或更多任务的 project、精确
snapshot、精确 dt payload、可选 artifact manifest、uv 环境、center/node、GPU 数与卡号、节点
boot ID 和 required path/disk。全部相同时显示 `MATCH`；任一控制漂移显示逐任务差异、建议改用
`dt fork`，并返回退出码 1。`--json` 输出稳定的 `dt_compare_v1` 对象，其中
`controls_match` 可直接作为实验门禁，`results_ready` 另行表示所有任务是否
都已成功结束。即使两个 dirty-worktree 提交共享 git SHA，snapshot 或环境变化
也不会被误判为有效 A/B。
也可用 `dt compare -F JOBS.txt` 读取 batch 保存的有序 ID；文件行序就是
`--groups` 标签和基线/候选的对齐顺序，laptop 会在本地解析后沿用现有 center
定位与转发契约。

加上 `--metric 'OUTPUT_GLOB::DOTTED_FIELD'` 后，dt 会直接从每个任务的
`outputs/` 读取唯一匹配的 JSON 数值，不需要先 pull 再手写 jq。`--groups ABBA`
或逗号分隔标签会按组计算均值、range/spread 和相对第一组的提升；
`--lower-is-better` 用于延迟、损失等越低越好的指标，`--unit` 只控制展示。
metric 模式输出 `dt_compare_v2`；控制变量漂移或任务未完成时不会读取结果，
缺失、多重匹配、非数值和非有限值都会明确失败。
完整任务时长无需依赖远端产物：`--metric '@job::duration_s'` 直接读取 head
注册表中的权威 `started_at/finished_at`，可与 `--lower-is-better` 和相同门禁组合。
缺失终态时间戳时明确失败，不回退到可能尚未最终归档的 `outputs/dt/job.json`。

性能门禁可直接放在同一条 compare 命令中：`--min-improvement PCT` 要求候选组
至少提升指定百分比；非劣实验用 `--max-regression PCT` 表示候选组最多允许
回退多少。两者互斥，避免同一条命令出现冲突判据。`--max-spread PCT` 要求
基线和候选组的组内 `(max-min)/mean` 波动都不超过阈值。门禁要求恰好两个
唯一组，第一组是基线、第二组是候选；spread 门禁还要求每组至少两次重复。
任务未完成、指标读取失败、提升不足、回退超限或波动超限都会在保留完整比较
结果的同时返回退出码 1；JSON 的 `metric.gate` 给出 `pass`、方向修正后的
`observed_improvement_pct`、`observed_regression_pct`、阈值和逐项失败原因。

编译实验常把多 GB 的 Inductor 缓存写入 `outputs/.cache/`。只需要报告和日志时
用 `dt pull REF --lite`，它会跳过 `checkpoints/`、`.cache/`、`cache/` 与 profiler
目录中的原始 trace；普通 `dt pull` 仍完整回收全部产物，`--lite` 也可以继续
叠加自定义 `--exclude`。pull 的同一次远端存在性预检也会尽力读取
`outputs/` 的 apparent bytes：普通回收超过 1 GiB 时会在 rsync 前显示总量并
提示 `--lite`，传输状态行也带总量；`--json` 在探测成功时增加
`remote_outputs_bytes`。旧节点不支持该体积探针时仍照常回收，不把可选提示
升级成失败；`du` 本身最多运行 5 秒，超时只省略体积字段。
`--to DIR` 会把远端 `outputs/` 的内容直接放入 DIR；省略时使用
`paths.results`（默认 `~/dt/results`）下的 `<job-id>/`，不会在自定义 DIR
下再自动套一层 job id。批次实验优先用 `--collection NAME`：单任务和多任务
都会稳定落入托管根目录的 `collections/NAME/<job-id>/`，NAME 只允许相对路径
且不能逃逸托管根。这样既能按实验分组，也不会在代码仓库制造大批
`results/` 文件。无论 full/lite，
dt 都会额外在保留目录 `DIR/dt/` 写入注册表快照 `job.json`，并把远端
`logs/` 整体断点续传到同一目录：除兼容路径 `stdout.log` 外，还会带回
`env.log`（uv/setup）与 `telemetry.log` 等运行记录；即使极短任务没有报告或
遥测，回收目录也包含命令、终态和完整诊断链。终态记录会从 head 的
`started_at/finished_at` 派生 `duration_s`；运行中或缺失终态时间戳时该字段
为 `null`，不会把 pull 时刻误当成最终完成时间。
多任务 `dt pull REF...` 把 `--to DIR` 解释为批次根目录，每个任务固定落到
`DIR/<job-id>/`，省略时则并行写入托管结果根的 `<job-id>/`。最多同时执行
4 个独立 rsync，每个子目录仍持有自己的归属锁；一个任务未就绪、离线或传输
失败不会阻止其他任务完成；输入中某个 REF 不存在时，它也只成为一个有序的
`not_found` 子结果，其他有效任务照常回收且不会为无效 REF 创建目录。单任务
不存在时仍沿用原有的单项错误契约。终端结果表按输入顺序汇总，整体退出码取
第一个非零结果；`--json` 只输出一个 `dt_pull_group_v1`，包含
`pulled/issues/aggregate_exit_code` 和每个任务的目标、records、partial 或
错误。Ctrl-C 会协作终止仍在运行的本地 rsync，保留已完成和 `--partial`
目录，并打印包含全部 refs/选项的精确续传命令；加 `--json` 时 stdout
恰好输出一个 `pull_interrupted` 对象，包含退出码 130 和同一恢复命令，
stderr 保持为空。该契约统一覆盖 head 单任务、head 多任务和 laptop。
laptop 多任务 pull 要求同一 center，以便整组重连和聚合；跨 center 请分别执行。
`stdout.log` 是历史兼容文件名，实际合并保存命令的 stdout 与 stderr；
`dt watch` 因此把对应面板标成 `output · stdout+stderr`，避免把远端错误误认
为标准输出。
`job.json` 会在 outputs 传输开始前原子写入，因此中断留下的部分目录也有明确
作业归属，可用同一命令安全续传。outputs rsync 内部始终排除保留路径
`dt/job.json` 和 `dt/*.log`，权威日志只从远端 `logs/` 单独回收；因此同名训练
产物在完整或中断传输中都不能覆盖作业身份或伪造 stdout/setup 证据。日志
rsync 自身也排除 `logs/job.json` 与 `logs/resources.jsonl`，关闭第二条身份和
遥测覆盖入口；内置 `dt/resources.jsonl` 始终只来自 outputs。目标目录已有同一
job id 时直接续传；若目录非空但没有记录，或记录属于另一作业，dt 会在任何
远端访问/传输前拒绝，避免静默混合实验结果。确实需要合并或覆盖其他同名文件
时显式加 `--force`。

laptop 模式仍把项目与结果归档在对应 center 的 head，不让笔记本直连计算节点；
但长时间 pull 的外层 laptop→head SSH 现在也会按 2/4/8/10 秒退避自动重连。
所有按 job id/name 操作的命令优先查询 `default_center`：150ms 内命中就直接
使用，不再为无关 center 建立 SSH；未命中会立即查询其余 center，仍在等待则于
150ms 后并发 hedge。只有每个可配置 center 都明确返回“没有该任务”时才报告
`not_found`；只要尚未找到任务且任一 head 不可达，结果就是 `unreachable`
（退出 5，并保留逐 center 原因），不会把未知状态伪装成任务不存在。head 返回
坏 JSON 或其他协议错误时则使用
`lookup_failed`（退出 1）。这套定位契约统一覆盖
`logs/info/watch/wait/metrics/pull/rerun/fork/kill`；`kill --json` 对未知状态
输出 `unverified`，绝不声称已安全删除。
断线前的半截 stdout 会被丢弃，因此 `--json` 始终只有最终完整对象。head 端按
规范化目标目录持有独立 flock：若第一次远端 pull 在链路断开后仍存活，重连后的
续传会等待它释放锁，绝不会并发写同一结果目录。Ctrl-C 只停止 laptop 本地等待，
head 已完成和 partial 数据都保留，并打印可直接重跑的 `dt pull REF`。
head 上的单任务 pull 也遵循同一契约：返回 130、不删除已落盘数据，并打印保留
原 refs 和选项的精确续传命令；`--json` 中断不会退化为空 stdout。
启动前失败通常没有 `outputs/`；此时 `dt pull ID` 自动进入记录回收模式，只
拉取 `dt/job.json` 和已有运行日志，成功 JSON 明确给出
`outputs_present:false`，不会把“没有训练产物”误报成回收失败。
自动化回收可加 `--json`：成功对象包含
`job_id/status/job_status/node/destination/lite/excludes/records`；其中
`status` 是回收操作状态（成功时为 `pulled`），`job_status` 是拉取开始时的
源任务生命周期（例如 `running` 或 `finished`），因此运行中快照与最终回收
无需读取本地文件即可区分。`application_outputs_recovered` 明确表示应用
`outputs/` 是否已成功传输；`records_scope:"dt_reserved"` 则说明 `records`
只枚举 `dt/` 保留诊断记录，而不是应用输出树的文件清单。预检或 rsync
失败仍保持原退出码，并输出
`error/message/exit_code`；只要 ref 已解析到任务，失败对象也包含同一
`job_status`，真正的 `not_found` 则不伪造生命周期。outputs 或运行日志传输
中断对象还会标记 `partial: true`、已回收 records 和目标目录，原命令可直接
续传。
链路类故障默认在首次尝试后再重试 2 次，退避 5 秒、10 秒；每次失败都会立即
在 stderr 显示 outputs/run logs phase、当前/总尝试数、退出码、简短原因和下次
等待，不再让 spinner 静默转满 15 秒。交互排障可用 `--retries 0` 快速失败，
或用 `--retries N` 明确限制额外尝试次数；确定性的权限/数据错误仍立即返回，
不会因该选项盲目重试。只要实际发生退避，最终单任务或多任务 JSON 会增加
`retry_events`，逐项记录 phase、失败/下次/总尝试数、等待秒数、退出码和原因；
没有重试时不输出空字段。非默认策略会完整转发到 laptop/head，并保留在
Ctrl-C 打印的精确续传命令中。
`records` 会如实枚举 `dt/` 顶层运行记录，包括 outputs 中的
`resources.jsonl`，不只枚举 `*.log`。
回收前会先检查远端 `outputs/`：目录确实不存在时返回 4；节点或 SSH 不可达时
保留原始连接错误并返回 5，不再误报“没有产物”，也不会提前创建本地目标目录。
传输中途发生明确的 socket、协议、超时或 SSH 链路错误也返回 5；数据和权限
错误仍返回通用失败并立即回传。中断后的部分文件会保留，节点恢复后原命令
即可续传。

每张已分派的 GPU 从 wrapper 启动到作业退出都会持有节点本地 advisory
lease；因此，即使训练仍在 CPU 数据预处理、尚未创建 CUDA context，`dt free`
和后续调度也不会误把该卡当成空闲。`dt free --json` 同时返回每张卡的实时
温度；租约占用时还返回完整 `lease_owner` job id，`dt free --who` 紧凑显示
`dt:<任务名>`，不再只写无法定位的 `dt-lease`。人类多任务视图由
`dt ps --watch` 显示分配卡的利用率、显存和温度。
`dt free [--who]` 在 80 列终端仍保留完整节点名、GPU/VRAM/CPU/RAM/disk/IO
与占用者；`GPU free` 使用 `空闲数/总数 [可用索引]`（例如 `1/2 [1]`），
`VRAM free` 明确使用 `剩余/总量`（例如繁忙 24 GiB 卡上的
`2.2/24G`），避免与 `watch` 中的 `已用/总量` 混淆；
`load` 使用节点逐卡最大 `利用率%/温度°`（例如 `89%/74°`）；已持有 dt
租约、尚未出现 CUDA 进程且只占用基线显存时显示 `init/温度°`；若已经保留
CUDA 显存但当前正处于 CPU 仿真、数据处理或短进程间隙，则显示
`pulse/温度°`。`dt ps --watch` 同样区分 `GPU:init/...` 与
`GPU:pulse/...`，不会把二者误解成普通 `0%` 空转。离线节点
用 `GPU free=offline` 和紧凑 issue 展示，完整错误仍在 JSON。
进程和 CUDA 显存已经出现、但训练日志只声明总步数而尚未出现第一步时，人类
`watch`/`ps --watch` 进一步显示 `pre-step · target N`；实时 `0%` 仍原样保留，
因此既能识别冷启动边界，也不会把真正的低利用伪装成健康训练。机器 JSON
保持原始 progress/resource 字段，不增加探测或推断原因。
磁盘低于 5% 可用空间，或绝对余量低于 20 GiB 时，`disk` 会追加 `!`，
`IO / issue` 同时显示精确可用百分比（例如 `85G! · disk 4.7%`）。这是一条
人类告警，不会在不知道任务预计写入量时擅自改变自动放置；公共 JSON 仍保留
原始 `disk_free_gib/disk_total_gib` 数值供自动化制定自己的阈值。预计会写入
checkpoint、数据集或 profiler trace 时，用 `--require-disk-gib N` 把估算变成
可执行的调度合约；`dt info` 和提交 JSON 会显示最终生效值。
人类模式还在资源表下合并同一 center 的调度解释，例如
`2/3 GPU free · 0 running · 0 queued · idle: no dt work queued`，并给出
`dt task <空闲节点> 'COMMAND' -n NAME`。有运行任务但没有 queued 后继时，
界面会在 GPU 真正空闲前标出 queue runway 已耗尽；仍有空卡就指向当前最佳空闲
节点；若它和唯一运行节点不同，还会单列 `keep busy` 命令。所有卡都忙则指向
唯一的运行节点，节点不唯一时安全回退为 `NODE` 占位符。
“最佳空闲节点”先比较空闲 GPU 数；并列时优先已知磁盘健康的节点，再选未知，
最后才选已经触发 `<20GiB` 或 `<5%` 告警的节点，并用绝对可用空间继续打破平局。
该规则只影响人类提示，不改变调度放置与公共 JSON。
有队列时会同时解释 center 总空卡与
队首真正可用的容量：固定节点任务不会把其他节点的空卡误报成 eligible；
非固定多卡任务会区分单节点容量不足造成的 fragmentation、配置保留卡造成的
reserve wait，以及确有合格容量但 agent 尚未完成派发的 dispatch pending。
任务自身的 `blocked:` 约束优先展示，不会被表面的空卡掩盖。队首 ID 和原始
reason 始终保留，便于直接进入 `dt info`。有空卡但 agent 停止时明确标为
`stalled` 并提示 `dt agent start`；registry 已空而租约仍存在时不会误说
“外部占用”，而是给出对应 `dt info OWNER` 检查命令。
公共 `dt free --json` 数组保持原 schema，不混入 scheduler 字段；laptop
人类视图从新 head 取得轻量上下文，遇到旧 head 不认识该内部能力时自动回退到
原资源表。自动化需要同时回答“GPU 为什么空”和“下一步做什么”时，显式使用
`dt free --json --explain`：它返回版本化的 `dt_free_explain_v1` 对象，
同时包含原资源行、跨 center 汇总、每个 center 的稳定 `state/message`、
原始 scheduler 上下文和 argv 形式的安全下一步。典型状态包括完全空闲、
残留 dt 租约、外部 GPU 占用、agent 停止、队首约束阻塞、正常排队，以及
运行任务没有 queued 后继。从多 center laptop 调用时，提交和启动 agent 的
argv 会自动附带所属 `-c CENTER`，不会误落到默认中心。遇到不支持 scheduler
上下文的旧 head 时，资源
仍正常返回，但 `running/queued` 为 null、状态为 `scheduler_unavailable`，
不会把未知误报为空闲。普通 `--json` 的数组契约保持不变。
`dt free --watch` 默认每 2 秒重新探测一次，`--poll S` 可调整间隔；从 laptop
经 head 查询时也会显式绕过短 TTL probe cache，因此每一帧都是真实新探测，
而不是只重绘旧缓存。配合 `--json` 时每次输出一行完整 JSON 数组，适合持续
采集；再加 `--explain` 时每行改为一个完整的版本化解释对象。probe cache
使用并发安全的唯一临时文件，缓存写失败只会失去加速，不会
丢掉刚获得的实时资源结果。多个并发探针用共享 flock 检查租约，彼此不会短暂
制造假占用；训练 wrapper 持有的是独占 flock，因此 CPU 初始化阶段的真实租约
仍会阻止所有探针和调度器把卡判为空闲。
一次性 `dt free` 在至少一个节点/center 返回可信结果时保留部分结果并成功；
若全部失败，纯链路故障返回 5，协议或工具故障返回 1。laptop 的离线 center
行保持与普通资源行一致的 `gpus/system/error/unreachable` 字段，机器消费者
无需为 head 故障维护第二套 schema。`--watch` 遇到全离线仍持续输出离线帧并
等待恢复，不会因瞬时网络抖动退出。

每个任务自动按 1 Hz 把分配 GPU 与主机资源写到
`$DT_JOB_DIR/outputs/dt/resources.jsonl`。运行时用 `dt watch` 看实时值；
原始记录的 `node` 使用配置中的稳定节点名（如 `psibot-hm`），不会因机器
hostname 与调度别名不同而破坏跨任务分析。
采样循环使用单调时钟的绝对 deadline，探针自身耗时不会再叠加到每个 1 秒
周期；若一次采样严重超时，会跳过已经错过的周期而不是连续突发补采。
sidecar 还会扫描 wrapper 每个 Linux 线程的 `/proc/.../children`，沿完整子树
归因该 job 自身的 CPU、内存、读写速率、进程数和线程数；因此由 uv 非主线程
创建的训练进程或调用 `setsid` 的子进程都不会丢失。内存原始记录保留 RSS 与
总 PSS，界面优先显示 `RAM(anon PSS)`：既不会把 fork/DataLoader 共享页重复
相加，也不会把 CUDA 设备文件映射误算成主机 RAM；旧内核或权限不足时依次回退
到总 PSS、RSS。`watch` 在现有日志 SSH 响应里复用最后一条样本，不增加远端
往返：单任务显示 `live job`，多任务把紧凑的 job CPU/RAM 放在进度列。终态
`info/watch/metrics` 汇总 job 均值与峰值，并继续把 host 总量单独显示，避免
把共享机器负载错算给当前实验。旧任务没有新增字段时仍按原格式读取。
1 Hz 利用率是离散采样：短 CUDA 脉冲可能全部落在采样间隙。若峰值为 0%，
`watch/info/metrics` 会明确写成“没有捕获到忙碌样本”，而不是把它解释为
GPU 从未使用；显存、功耗和原始 JSONL 仍保留为交叉判断依据。
为避免把初始化误读为训练供给不足，汇总同时显示当前汇总范围的 `window` 均值和
仅非零利用率样本的 `busy-only` 均值、非零样本比例、首次忙碌时间及结尾空档。
`busy-only` 是条件统计，不等同于训练阶段均值；训练中的零利用率仍会降低
非零样本比例。`metrics --json` 在每张 GPU 下提供对应的
`util_busy_mean_pct/util_busy_samples/util_samples/busy_fraction_pct` 及时间边界。
任务还可以在不安装任何 Python 包的情况下标记应用阶段：
`"$DT_PHASE" data_loading`。名称限 1–64 个字母、数字及 `_.:-`；dt 自动记录
`wrapper/runner/runner_returned`，自定义标记写入
`$DT_JOB_DIR/outputs/dt/phases.jsonl`。当前阶段会进入同一份 1 Hz 遥测，
因此 `dt watch` 不增加 SSH 往返就能显示 `live phase`；`dt info` 的
`phase_summary`/`phase timeline` 则给出相邻标记间的耗时。原始阶段 JSONL
也由普通 `dt pull` 一起回收。标记描述的是调用方定义的阶段边界，dt 不会把
`campaign_run` 等同于纯训练循环。资源汇总还会把连续同名遥测切成有序的
`phases` spans；`dt info` 显示紧凑的 `phase samples` GPU 均值/峰值，
`dt metrics` 展开阶段 GPU 与 job CPU 行，全部复用现有遥测而不新增探测。
`dt info` 默认内嵌最近 3600 条样本的均值/峰值摘要，结束后无需先 pull 就能看到峰值
显存、利用率、温度和采样错误；它并行读取状态、产物与实时资源，节点不可达时
快速返回 `node_unreachable=true` 和具体资源错误。运行时间超过 guard 时还会
标记 `max_hours_exceeded` 及超出秒数；节点离线时明确写成“completion
unconfirmed”。极短任务若尚未产生第一条遥测，摘要保持为空但不会误报节点离线。
可用 `--metrics-tail N` 调整窗口（`0` 表示全部），或用 `dt metrics` 查看独立的
详细汇总；两者复用同一读取与聚合实现。原始文件随普通 `dt pull`
一起回收，不依赖训练项目额外安装监控包。
`dt metrics` 会区分“遥测文件确实不存在”（退出 4）与“节点当前不可达”
（保留连接错误、退出 5），不会把网络故障误写成 sidecar 未启动。加
`--json` 后，参数、任务定位、未启动、连接和遥测读取错误都会输出稳定
`error/message/exit_code` 对象，成功时仍输出资源汇总对象。从 laptop 查询时，
外层 SSH 若中断会按 2/4/8/10 秒退避恢复并重新读取；失败尝试的半截 stdout
会被丢弃，所以 `--json` 始终只有一个完整对象。Ctrl-C 返回 130，并提供可直接
重跑的命令，不会改变远端任务或遥测记录。

失败时 `dt wait` 除了返回训练进程的退出码和 stdout 尾部，还会识别形如
`see outputs/...log` 的任务内日志引用，校验路径不越出 `outputs/` 后自动附上
被引用日志的尾部。因此外层 runner 包装异常时，通常一条 wait 就能看到内部
Triton/CUDA/训练根因；用 `--error-lines N` 调整行数，设为 0 可关闭。
若主日志或被引用日志读取时节点离线，wait 会显示原始 SSH 原因，但仍返回训练
进程自身退出码，不会用链路错误覆盖实验结果。
`dt wait --json` 的 stdout 保持单个终态对象；非零退出时会把同样的主日志与
引用日志证据放入结构化 `failure_log`（`path/tail/error/referenced`），日志
读取失败也作为字段返回且不覆盖任务退出码。`--error-lines 0` 会省略该字段。
`dt wait REF...` 并发等待同一 center 的整组任务，全部进入终态后输出紧凑结果
表，并按任务集中附上失败证据；它不会因第一个失败而漏掉其余任务。整体退出码
取输入顺序中第一个非零任务结果（训练码 0–125、killed 66、lost 67、
failed-before-start 68），全部成功才返回 0。多任务 `--json` 仍只输出一个
`dt_wait_group_v1` 对象，包含 `succeeded/issues/aggregate_exit_code` 汇总和按
输入顺序排列的完整 `jobs`。单任务输出契约保持不变；laptop 多任务等待要求
refs 属于同一 center，跨 center 总览使用 `dt ps --watch`。多任务的实时边缘
使用紧凑 `序号/总数 · action` 前缀，完整名称和 ID 留给 identity 行及终态表格，
所以长 batch 名称不会再次拆断 queue/start 动作。
单任务和多任务 wait 都会在运行阶段使用 completion 连接提前唤醒状态刷新；
排队状态、节点不可达或连接异常时仍按 `--poll` 可靠检查，不改变退出码和错误
证据契约。Ctrl-C 只停止本地等待，不取消任何排队或运行中的任务，返回 130
并打印保留 refs、poll、错误行数和隐藏跟随状态的精确恢复命令。加 `--json`
时 stdout 恰好输出一个 `wait_interrupted` 对象；正常进度仍留在 stderr。
该契约统一覆盖 head 单任务、head 多任务和 laptop。面向人的单任务状态边缘把
短动作、`job <id>` 和动态 reason 分成有意的独立行；因此长 job id 或详细 GPU
占用原因只会在自身行换行，不会把 `queued; waiting for dispatch`、
`started on NODE` 或 `finished · exit N` 的关键动作拆断。
一次性日志读取可用 `dt logs REF --json`，返回
`job/status/node/source/path/lines/text` 单对象；参数、定位、未启动、链路和读取
错误使用稳定错误对象。`logs -f` 保持面向人的实时文本流，机器持续监控请使用
`dt watch REF --json`，两种模式不会混用。
训练框架使用 seek/覆盖式进度输出时，日志可能出现 sparse NUL padding。有限
日志视图（`logs/watch/wait/info` 及其 JSON）会把每段连续 padding 替换为
`[dt: omitted N NUL bytes]`，既不污染终端，也不静默隐藏数量；`logs -f`
在 live tail 边界过滤 NUL。净化只作用于查看流，节点上的原始日志和
`dt pull` 回收记录保持逐字节不变。
head 到计算节点的状态探针失联时，`dt wait` 会立即输出一次具体连接错误并继续
重试，连续失败不刷屏；链路恢复时再输出一次恢复提示。注册表中的最后可信任务
状态始终保留，不会因为网络抖动提前结束等待；缓存的 `lost` 在离线帧中也不会
推进终态确认，必须等节点恢复并取得新的可达探针证据。等待期间超过
`--max-hours` 时会只提示一次逾期时长；若节点同时离线，会明确说明暂时无法
确认远端 timeout 的完成标记。自动化可加 `--json`：stdout 只输出一个终态
对象（含 placement、snapshot、reason 和退出码），进度与失败日志仍在 stderr，
进程退出码继续透传训练结果。
单次 `dt logs` 在 SSH 失联时同样把底层 255 规范化为 dt 的稳定退出码 5，
stderr 仍保留原始连接原因，便于脚本和用户同时判断。
`dt logs -f` 会同时跨越 laptop→head 与 head→计算节点两层 SSH 抖动，按
2/4/8/10 秒退避自动重连；故障与恢复各提示一次。恢复时重新输出最近的 tail，
因此断线期间产生的日志不会静默丢失，但少量最近行可能重复。Ctrl-C 只停止本地
跟随并打印恢复命令，不会取消任务。正常跟随使用 wrapper PGID 绑定
`tail --pid -s 0.2 -F`：wrapper 结束后先排空最后日志，再自动返回与
`dt wait` 相同的稳定退出码（训练码、killed 66、lost 67、setup 68）。
对已终止任务执行 `logs -f` 不再启动永久 tail，而是输出最后 N 行并立即返回。
对 queued 任务执行同一命令会先显示当前排队原因，每 0.5 秒只读 head registry；
dispatch 后显示目标节点并无缝进入同一个 follower，直到 terminal。排队原因仅在
变化时更新，不刷屏；排队阶段 Ctrl-C 仍只分离，直接 killed/failed 则分别返回
66/68。长 job id 的 queue/start 动作与身份分成有意的两行，在 80 列终端不会
把“waiting for logs”拆断。
远端 tail 不申请无用 PTY，因此管道、CI 和非交互调用不会出现
`Pseudo-terminal will not be allocated` 噪声。`dt attach` 保持交互式直连语义，正常
detach/退出码保持原样，链路断开统一返回 5，用户可显式重新 attach。

长任务的外层 stdout 若很安静，而训练框架把进度写到
`outputs/**/*.log`，`dt watch` 和非 follow 的 `dt logs` 会在每次读取时自动
选择 stdout 或最近更新的任务内日志，并明确显示 `log_source`。`dt logs -f`
启动时跟随当前活跃日志。选择范围严格限制在该 job 的 stdout 与
`outputs/**/*.log`，不会读取任务目录外的路径。
同一次日志读取还会返回被选中日志的更新时间；`dt watch --json` 提供
`log_updated_at` 与持续增长的 `log_age_s`，无需额外 SSH 探测。人类单任务
视图显示 `log age`，多任务视图在日志超过 60 秒未更新时追加 `log idle`。
它只陈述日志新鲜度，不把安静的数据加载、编译或评估阶段误判为任务失败；
日志再次写入后 age 会自然归零。

`dt watch` 还会从活跃日志里的明确字段提取结构化进度：step、总步数、
百分比、ETA、单步耗时与 samples/s；吞吐既支持独立的
`Throughput 668.7 samples/s`，也支持 step 行内的
`throughput=668.7 samples/s`。终端显示为一行紧凑摘要，`--json`
则放在稳定的 `progress` 对象中；首帧前若参数非法或任务不存在，也会输出可解析
的 `invalid_argument` / `not_found` 错误对象并分别返回 1 / 4。它不会根据
普通文本猜测进度，完成态也不会
残留最后一次运行中 ETA。最终帧会同时读取已经落盘的 `resource_summary`，
因此机器消费者无需再调用一次 `metrics` 就能得到 GPU/主机均值与峰值；旧任务
没有 telemetry 或节点不可达时保持 `null`，不会让 watch 失败。任务耗时以远端
wrapper 的亚秒级起止标记为准，运行态切换到终态时 elapsed 不会因整数秒取整而
倒退；任务结束后再打开 watch，elapsed 也不会继续增长。旧任务的整数时间标记
仍可直接读取。每帧的状态、资源和日志读取并行执行，
节点不可达时保留注册表中的最后可信状态，并在资源行和日志面板直接显示连接
错误；状态行同步显示 `running? offline`。超过运行 guard 时追加 `>max` 和
overdue 时长，JSON 帧提供与 `ps/info` 相同的结构化字段。后续刷新会持续重试，
不会把断网误判成任务失败。

`dt watch REF...` 可把同一 center 的排队、运行和已结束任务收敛到一个 Live
表格；每行显示状态、任务、节点/GPU、耗时和进度/根因，只为运行中或失败任务
展开日志尾部，成功任务不会淹没屏幕。它等待所有 refs 进入终态；watch 自身
完成仍返回 0，训练退出码与整组失败汇总由 `dt wait REF...` 负责。单 ref 的文本和 JSON
保持原契约；多 ref 的 `--json` 每次输出一个完整 `dt_watch_group_v1` 对象，
包含状态计数、`issues`、整体 `terminal` 和按输入顺序排列的 `jobs` 快照。
长时间接入自动化时可加 `--compact`：单任务帧使用
`dt_watch_compact_v1`，多任务帧使用 `dt_watch_group_compact_v1`；它保留状态、
节点/GPU、耗时、实时资源、结构化进度、日志新鲜度和当前 FIFO 上下文，但不输出
重复的原始日志尾部，也不读取或输出终态 `resource_summary`。排队任务会携带
`queue_position/depth/ahead_count/head/predecessor`；非队首的 `reason` 是实时
派生的 FIFO 等待原因，可能陈旧的最后一次容量探测仍保存在
`last_dispatch_reason`，因此不会把队尾误报为仍被旧任务占卡。文本 watch 同样显示
`queued #N/M`。默认 `--json` 仍是完整契约；需要详细
终态统计时继续使用默认 watch 或 `dt metrics REF --json`。`--compact` 仅与
`--json` 一起使用，laptop 转发、断线重连和 Ctrl-C 恢复命令会原样保留该选项。
从 laptop 发起的多 ref watch 要求这些任务属于同一 center，才能由一个远端
Live 会话稳定重连；跨 center 总览使用 `dt ps --watch`。

从 laptop 直接运行 `dt watch` 同样会在 head 链路断开后按 2/4/8/10 秒有界
退避重连；故障和恢复各提示一次，`--json` 的 stdout 仍只包含 JSON 帧。运行
任务的 completion 连接会让终态刷新提前发生；定期资源/日志帧仍由 `--poll`
控制。

同一机器契约也覆盖 `dt ps --watch --json` 的非法刷新间隔（退出 1）和
`dt info REF --json` 的未知任务引用（退出 4），因此监控脚本无需解析 Rich 文本。

训练脚本约定：从 `$DT_JOB_DIR` 拿作业目录，产物写 `$DT_JOB_DIR/outputs/`；
长阶段在边界调用 `"$DT_PHASE" safe_phase_name`。
不可变代码快照不包含 `.git`；需要 git SHA、dirty 状态或精确
`snapshot_sha256` 时，读取 `$DT_META_PATH` 指向的 `meta.json`。

## 开发

开发在 psibot-hm:`~/cw/project/dt` 进行（Codex 常驻主节点），git 历史在此；
远端 remote 待功能完备后配置再 push。

```bash
uv sync            # 含 dev 依赖
uv run pytest      # 纯逻辑测试（解析、配置、id、渲染）
```

代码结构：`config` 配置 → `probe` 探卡 → `dispatch` 提交编排（直派 + 排队）→
`agent` 队列 agent → `payload/launcher.sh` 节点侧原子启动 →
`jobs` 注册表与状态机 → `remote` 笔记本转发 → `cli` 命令面。

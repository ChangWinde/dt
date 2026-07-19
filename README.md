# dt — DistTrainer

在多人共享、无 root 的 GPU 服务器集群上派发实验：找空闲卡、同步代码快照、
uv 复现环境、tmux 挂后台、收结果，全部收敛成一条命令。设计文档见
`tasks/DistTrainer.md`（Tools 仓库）。

## 安装（便携性设计）

依赖只有 typer / rich / pyyaml，Python 解释器由 uv 托管，对系统零要求、无 root。
开发与 git 历史都在规范仓库 psibot-hm:`~/cw/project/dt`。

主节点（裸机一条命令）：

```bash
bash bootstrap.sh          # 装 uv → Python 3.11 → dt（editable）→ 配置骨架
```

笔记本（从规范仓库的 git 安装，本地不留开发副本）：

```bash
uv tool install git+ssh://psibot-hm/home/psibot/cw/project/dt
```

计算节点什么都不用装：launcher / wrapper 随作业快照下发。

## 升级（维护性设计）

- psibot-hm 上是 editable 安装：改完代码（或 `git pull`）下一次 `dt` 调用
  即是新代码，只有改依赖或入口时才需要 `uv tool install --force -e .`。
- 其他主节点，在笔记本上跑（笔记本只做中继，跨中心主节点互不可达；
  笔记本无需持有仓库副本，脚本自己会从源拉）：

```bash
ssh psibot-hm cat cw/project/dt/deploy.sh | bash -s -- zgca-r0 star-0
```

- 笔记本自身：`uv tool upgrade dt`（重新解析 git 源的 HEAD）。
- `dt --version` 带 git sha；`dt doctor`（笔记本模式）会报出每个主节点的
  dt 版本，升级漂移一眼可见。

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
default_project: vla
paths:
  root: ~/dt
  envs: ~/dt/envs        # home 在 NFS 时改指节点本地盘
queue:                   # 可选：排队与自我约束旋钮
  poll_s: 60             # agent 轮询间隔
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

配好先跑 `dt doctor`，它会逐项验证配置声明的连通性与依赖。

## 快速上手

```bash
dt free                                   # 哪里有空闲卡
dt run -g 2 -n exp42 -- python train.py   # 提交（stdout 末行是 job id）
dt ps                                     # 在跑什么（含排队中）
dt logs exp42 -f                          # 看日志
dt wait exp42                             # 等结束，透传退出码（覆盖排队阶段，断线自动重连）
dt rerun exp42                            # 原样重跑（当前代码 + 相同命令/资源）
dt pull exp42                             # outputs/ 拉回主节点（断点续传）
dt kill exp42 / dt clean --before ...     # 收尾（--envs 顺带清闲置环境）
```

无空闲卡时 `dt run` 默认进队列（快照在提交时落盘，排队期间改代码不影响
已提交作业），主节点 agent 每 60s 轮询、FIFO 派发；`--no-queue` 恢复
"无卡直接退出码 2"。agent 由 `dt run` 入队时自动拉起，`dt agent install`
写 crontab `@reboot` 保证主节点重启后队列不停摆。

训练脚本约定：从 `$DT_JOB_DIR` 拿作业目录，产物写 `$DT_JOB_DIR/outputs/`。

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

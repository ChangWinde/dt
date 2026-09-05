# dt 0.13.6 升级后复核：此前报告的问题哪些已修、哪些仍在

- 复核日期：2026-09-05 20:20–20:40
- 环境：psibot-hm 头节点 `dt 0.13.6 (git 74a5abe1bcda)`；本机 star-0 头节点同版本
- 方法：在真实队列上做非破坏性实测（假引用的 kill、管道下的 ps、-g 0 的 `true` 任务），并对照 d5bedd2e..74a5abe1 的提交记录

## 已修复（实测）

| 此前报告 | 结果 | 依据 |
| --- | --- | --- |
| `dt kill` 未加 `-y` 在非终端下读 stdin | 已修：立即输出 `non-interactive kill needs -y` 并退出，脚本后续命令正常执行 | 实测；提交 7f2026d / 34eeede |
| `dt ps` 管道输出截断 ref | 已修：管道下输出完整 job 身份（78–89 字符） | 实测；提交 e323dda |
| `dt ps` 只显示 "queued blocked"，原因要另查 `dt info` | 已修：阻塞原因随行显示 | 实测；提交 c96872d |

## 按提交记录应已修复，本次未能实测

| 此前报告 | 相关提交 |
| --- | --- |
| 带失败派发标识的排队 job 无法 kill | dfdb79a "dt kill dequeues a job that still carries a dispatch attempt" |
| compact 删除写进 `code/` 的产物 | 2df34e3 "compact keeps code copies the job wrote results into" |
| 容量缓存滞留 | dc9377c "retry blocked queue entries the moment a running job ends" |
| Linger 前置条件无文档 | b4d5ed8 "list systemd lingering as a GPU-node prerequisite"（`dt doctor` 的表格里仍看不到 Linger 列） |

## 仍存在或形态改变

1. **`dt run` 在 stdin 为管道时提交后不返回**（形态改变）。`printf 'MARKER\n' | dt run -g 0 -n probe -- true` 与 `ssh head 'bash -s' <<EOF ... dt run ... EOF` 两种情形下，任务已进入队列（`dt ps` 可见），但 `dt run` 进程停在 `hrtimer_nanosleep` 轮询，3 分钟以上不退出；`dt run ... </dev/null` 立即返回。旧版本是把后续 stdin 吞掉，新版本变成阻塞——脚本化调用仍必须加 `</dev/null`。建议：stdin 是否为终端不应改变 `dt run` 的返回语义；若这是"跟随模式"的隐式触发，请改为显式 `-f`。
2. **队列跨节点严格先进先出**（新报告，见 `2026-09-05-fifo-head-of-line-blocking-across-nodes.md`）：钉在空闲节点的任务被排在前面、等待另一节点的任务阻塞。
3. **`dt fork` 钉在源任务的节点**：19 个 fork 全排到同一张卡、1 个排到离线节点；没有 `--node` 覆盖项。
4. **有常驻 GPU 进程（rustdesk 424 MiB）的卡永远被判为忙**：`free = 无进程 ∧ 显存 < 阈值 ∧ 未租用`，阈值对此无效。
5. **compact apply 的参数长度问题（E2BIG）未见对应提交**，未复测（不敢在生产队列上 apply）。
6. `dt doctor` 报三节点 `slow(90–114KB/s)`，而 rsync 实测机房内网 1 GB 约 90 s（约 11 MB/s）、`dt sync` 1 GB 约 85 s；doctor 的带宽探测口径值得核对。

# dt bug：头节点容量缓存滞留，空闲节点被队列持续跳过

- 报告日期：2026-08-30（2026-09-05 整理归档）
- 来源：研究项目07 lrd，头节点 psibot-hm，受影响节点 psibot-yw
- dt 版本：0.13.x（早于 d5bedd2e）
- 严重程度：中（闲置一张卡，队列变慢）

## 现象

psibot-yw 的 GPU 完全空闲（`nvidia-smi` 无进程），但队列中的任务全部跳过它；`dt info` 显示陈旧的容量告警；删除 `gpu-0.lock` 无效。

## 机理（阅读 head/state 与 agent.log 推断）

任务在租约有效期内结束时，头节点缓存的该节点容量停留在 busy 状态，没有事件触发刷新，直到某个无关操作重新探测该节点。

## 期望

容量缓存加 TTL，或在任务终结事件里强制刷新对应节点的容量；`dt info` 标明容量读数的采样时间。

## 规避

提交一个 pin 到该节点的 0-GPU 任务（`dt run -g 0 --node psibot-yw -- true`），或 `dt seed psibot-yw`，触发一次探测。

## 处理结果（2026-09-05）

- 判断：节点探测缓存本身 TTL 只有 3 s；让空闲节点被持续跳过的是被阻塞任务的放置退避（5 s 翻倍至 300 s 上限），没有事件重置它。
- 修复：agent 每个 tick 比较 reconcile 前后的 running 集合，只要有任务离开节点就清空全部退避、下个 tick 立即重试。提交 dc9377c。
- `dt info` 的 placement failures 现在标注 "as of <时间> (last attempt)"，说明这是上次尝试的诊断而非实时读数。
- 回归测试：`tests/test_queue.py::test_blocked_backoff_resets_when_a_running_job_frees_capacity`。

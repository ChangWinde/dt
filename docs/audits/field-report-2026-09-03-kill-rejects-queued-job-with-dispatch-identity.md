# dt bug：经历过失败派发的排队 job 无法 `dt kill`（"invalid job registry record"）

- 报告日期：2026-09-03（2026-09-05 整理归档）
- 来源：研究项目07 lrd，头节点 psibot-hm
- dt 版本：`dt 0.13.4 (git d5bedd2e8813)`
- 严重程度：中（无法终止的排队任务会在下次派发时再次占用节点）

## 现象

一个 queued job 曾被派发到 psibot-yw 并在 launcher 阶段失败（yw 当时 node-unfit），回到队列后带有一次派发尝试标识。对它执行 `dt kill REF` 报：

```
invalid job registry record: only queued jobs may retain a dispatch attempt identity
```

job 状态明明是 queued，却因为携带 dispatch attempt identity 被 kill 路径的校验拒绝，只能等 agent 再次派发后失败。

## 期望

- kill 路径接受"queued + 有派发尝试标识"的记录（这是一个合法的中间状态），清理标识并出队。
- 或提供 `dt kill --force` 跳过该校验。

## 相关

同一台 yw 上还观察过 `.dt/state/launch-identity.sha256` 残留导致 dispatcher 重试撞 identity-conflict（exit 18）、job 永久排队的情况（2026-09-01），可能与本条共享同一状态机缺口。

## 处理结果（2026-09-05）

- 主问题：`dt kill` 对带派发尝试标识的排队任务，先用该 token 在节点上放置取消哨兵并核实无存活进程（`dispatch.cancel_queued_attempt`，与 failover 恢复同一原语），再清除标识出队；无法核实则保持排队并返回 `dispatch_attempt_unverified`。提交 dfdb79a。
- 附注问题（`launch-identity.sha256` 残留 → 重试撞 identity-conflict → 永久排队）：根因是 launcher 在发布身份标记之后才做 node-unfit 等预检，退出后标记留在节点而调度器丢弃了该次 token。修复两层：调度器在放弃已知 token 的可重试失败时先绑定其取消哨兵，让下次 launcher 合法取代旧标记；遇到 identity-conflict 时探测该标记，超过 6 小时且胶囊无任何运行期状态的孤儿标记就地退役。提交 1ad1edd。
- 回归测试：`tests/test_kill_queued_attempt.py`、`tests/test_launch_identity_retirement.py`、`tests/test_reliability.py::test_identity_conflict_*`。

# dt bug：`dt compact` apply 模式把整批清理脚本作为单个参数传递，超过 MAX_ARG_STRLEN 后 993 个 job 被误标为 unreachable

- 报告日期：2026-09-02（2026-09-05 整理归档）
- 来源：研究项目07 lrd，头节点 psibot-hm（工作节点 psibot-ds / ys / yw）
- dt 版本：0.13.4 早期构建（早于 d5bedd2e）
- 严重程度：高（apply 模式对大仓库不可用，且失败原因被错误归类）

## 现象

`dt compact --before 2026-09-03 -y` 对 1,030 个候选 job 执行时报：

```
[Errno 7] Argument list too long
```

993 个 job 失败，并在结果里被标为 `unreachable`（节点实际全部在线可达）。`dt compact --plan` 在同一批候选上正常。

## 机理（阅读执行路径推断）

每批 40 个候选的清理脚本被拼成一个字符串，作为单个参数交给 `bash -c` / `ssh`。apply 模式的脚本比 plan 模式长（含 `find -xdev -depth -delete` 与回执写入），总长度超过 Linux 单个参数上限 128 KiB（`MAX_ARG_STRLEN`），`execve` 返回 E2BIG。这个错误被当成节点不可达处理。

## 期望

- 脚本经 stdin 传给远端 `bash -s`，或按字节数而不是按候选个数分批。
- E2BIG 归类为"拆批重试"，不是 `unreachable`；结果摘要里区分两者。

## 规避

按 dt 同样的语义（快照校验、进程存活普查、`find -xdev -depth -delete`、`dt_workdir_prune_v1` 回执）自行生成每节点脚本并经 stdin 执行；事后 `dt compact --plan` 把全部 671 个 job 识别为 already_compact，说明回执格式兼容。

## 相关

同一批操作暴露了 `2026-09-02-compact-deletes-outputs-written-into-code.md` 描述的风险。

## 处理结果（2026-09-05）

- 已在 main 的 9b5e8e2（2026-09-02）修复：census 脚本经 stdin 交给远端 `bash -s`，不再进 argv；本地 spawn 失败（E2BIG/EMFILE/ENOMEM）归类为 head 侧失败（"head could not launch census"），不再记为 `unreachable`。
- psibot-hm 的构建早于该提交，需要升级。

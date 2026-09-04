# dt bug：`dt run` 读取并吞掉管道 stdin，批量提交脚本被静默截断

- 报告日期：2026-08-31（2026-09-05 整理归档）
- 来源：研究项目07 lrd，头节点 psibot-hm
- dt 版本：0.13.x（当时构建早于 d5bedd2e；2026-09-05 在 d5bedd2e 上仍需 `</dev/null` 规避）
- 严重程度：高（数据丢失级别的静默失效：提交数量少于预期而 exit 0）

## 现象

用 `ssh psibot-hm 'bash -s' < submit.sh` 提交一批判决格，脚本内第一条 `dt run …` 执行后，脚本余下的全部文本被 `dt run` 从 stdin 读走；结果 53 个格只提交了 6 个，脚本 exit 0，没有任何报错。

## 期望

`dt run` 默认不读 stdin；需要从 stdin 取命令或参数时用显式开关（例如 `--stdin`）。至少在 stdin 不是 TTY 时不应读取。

## 规避

每条 `dt run` 末尾加 `</dev/null`；或把脚本落盘后用 `bash file` 执行（stdin 为终端）。本项目所有提交模板已固定加 `</dev/null`。

## 备注

与 `2026-09-05-kill-noninteractive-reads-stdin.md` 同类；建议对全部子命令统一"非 TTY 不读 stdin"的策略并加测试。

## 处理结果（2026-09-05）

- 根因：`sshio._run_bounded_process` 启动 ssh/rsync 时 `stdin=None`，子进程继承 head 进程的 stdin，ssh 把脚本余下文本全部读走转发到节点。
- 修复：所有非交互子进程一律 `stdin=/dev/null`（有显式载荷时才喂入）；laptop→head 转发仅在 tty 且本地 stdin 确为终端时透传。提交 a9958f3、4898a36。
- 回归测试：`tests/test_stdin_isolation.py`（真实子进程：父进程 stdin 有内容时子进程读不到；显式载荷仍送达；tty 转发在管道下也不读）。
- 需要：head 升级到含此修复的构建后，脚本里的 `</dev/null` 规避可以去掉。

# dt bug：`dt kill` 未加 `-y` 时在非终端下仍读 stdin，吞掉调用脚本的余下内容

- 报告日期：2026-09-05
- 来源：研究项目07 lrd，头节点 psibot-hm
- dt 版本：`dt 0.13.4 (git d5bedd2e8813, install 59df2bdfa5f4, payload ed4c7b9d0de2)`；本机 `~/cw/software/dt` 的 HEAD（aa7529e）在该构建之后 112 个提交
- 严重程度：中（批量操作静默失效，无任何报错）

## 现象

用 `ssh psibot-hm 'bash -s' <<'EOS' … EOS` 执行如下脚本（stdin 是脚本文本，不是终端）：

```bash
refs=$(dt ps --recent 2>&1 | grep -E "orl-" | grep queued | awk '{print $2}')
n=0; f=0
for r in $refs; do
  if dt kill "$r" >/dev/null 2>&1; then n=$((n+1)); else f=$((f+1)); echo "kill failed: $r"; fi
done
echo "killed=$n failed=$f"
```

结果：脚本没有任何输出（连最后一行 `echo` 也没有执行），33 个排队任务一个都没被终止，`dt ps` 仍显示 33 个 queued。两次尝试（一次带 `set -e`，一次不带）现象相同。

改为 `dt kill -y -F /tmp/kill_refs.txt </dev/null` 后，33 个任务全部 `dequeued`。

## 判断

未加 `-y` 时，`dt kill` 的确认提示在 stdin 不是终端的情况下仍然从 stdin 读取，把脚本余下的文本当作提示的回答消费掉，于是 shell 再也读不到后续命令。与已报告的 `dt run` 吞管道 stdin 是同一类问题。

## 期望

stdin 不是 TTY 且未给 `-y` 时，立即以非零退出并输出一句明确提示（例如 `non-interactive kill needs -y`），不触碰 stdin。

## 已知线索

本机源码 `src/dt/cli/commands/kill.py` 第 465 行已有：

```python
if not yes and not sys.stdin.isatty():
    _fail_submission(kind="confirmation_required", message="non-interactive kill needs -y", ...)
```

该守卫随提交 `7f2026d` / `34eeede` 进入，均不在 psibot-hm 安装的构建（d5bedd2e）中。请确认：(1) 该守卫覆盖 head 与 laptop 两条路径；(2) `compact`、`clean` 等其他带确认提示的子命令有同样守卫；(3) 头节点应升级到含此修复的版本。

## 规避

所有脚本化调用一律 `dt kill -y`（或 `-F 文件`），并在命令末尾加 `</dev/null`。

## 处理结果（2026-09-05）

- 确认：(1) `dt kill` 的守卫在 laptop 分支之前，两条路径都覆盖；(2) `compact`、`clean` 有同样守卫（`confirmation_required`）；(3) 该守卫来自 #70，psibot-hm 的构建（d5bedd2e）早于它，需要升级。
- 另外修复了更深一层的同类问题：即便有 `-y`，dt 自己起的 ssh 也会吞 stdin（见 `2026-08-31-run-swallows-piped-stdin.md` 的处理结果）。
- 回归测试：`tests/test_error_contract.py::test_destructive_commands_refuse_without_yes_as_a_machine_error`、`tests/test_stdin_isolation.py`。

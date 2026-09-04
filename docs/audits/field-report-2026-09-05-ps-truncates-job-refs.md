# dt bug：`dt ps` 在非终端输出下仍按列宽截断 ref，截断后的 ref 不能再交给其他子命令

- 报告日期：2026-09-05
- 来源：研究项目07 lrd，头节点 psibot-hm
- dt 版本：`dt 0.13.4 (git d5bedd2e8813)`
- 严重程度：低–中（脚本化编排时拿不到可用的 ref）

## 现象

`dt ps 2>&1 | awk '{print $1}'`（stdout 是管道）输出的活动任务表仍按终端宽度排版，ref 列被截断：

```
20260905-0209_orl-scratch-a135-s4404_bb328622a19e01 -       wan… queued #1/… 02:
20260905-0147_pufu-cell-a135-s1103_218c25be75cb10a psibot… 0   running       01
```

完整 ref 是 `20260905-0209_orl-scratch-a135-s4404_bb328622a19e01f8`（末尾 2 个字符被截去，且没有省略号提示）。在窄一些的终端里 ref 会被截成 `…`。把截断后的 ref 交给 `dt kill` / `dt logs` / `dt info` 时无法解析。

`dt ps --recent` 第二列的 4 字符短引用（如 `01f8`）可以正常用于 `dt kill -F`，这是目前脚本化唯一可靠的取法，但它只出现在 `--recent` 视图里。

## 期望

- stdout 不是 TTY 时不做列宽截断（或提供 `--json` / `--no-trunc`）。
- ref 是任务的唯一可路由标识，任何视图都不应截断它；要截就截 name 列。
- 活动任务表也显示短引用列，与 `--recent` 一致。

## 已知线索

本机源码里 `ps` 已有 `display_ref` 压缩引用逻辑，`tests/test_ux.py` 也断言渲染结果不含 `…`，可能已修复；请确认覆盖"stdout 非 TTY"的情况，并升级头节点。

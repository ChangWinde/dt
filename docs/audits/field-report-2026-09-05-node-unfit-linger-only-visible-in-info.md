# dt 可用性：节点因 `loginctl Linger=no` 被判 node-unfit 时，`dt ps` 只显示 "queued blocked"，原因与修法只在 `dt info` 里

- 报告日期：2026-09-05
- 来源：研究项目07 lrd，节点 psibot-yw（用户 admin01）
- dt 版本：`dt 0.13.4 (git d5bedd2e8813)`
- 严重程度：低（原因是清楚的，但发现路径绕）

## 现象

psibot-yw 的 dt 用户没有开启 systemd linger。提交 12 个 GPU 任务后，`dt ps` 只显示：

```
skfu-cell-s1103   -   want:1  queued blocked #3/9
```

`dt info REF` 才能看到：

```
placement failures  psibot-yw: node-unfit: [launcher] node-unfit: GPU runtime requires loginctl Linger=yes
```

在 yw 上执行 `loginctl enable-linger admin01`（无需 sudo）后恢复，任务正常派发到 yw。

## 期望

- `dt ps` 的 blocked 行直接带上最近一次 placement failure 的一句话原因。
- `dt doctor` / `dt sync` / `dt seed` 对节点做 Linger 检查，未开启时打印修复命令 `loginctl enable-linger <user>`。
- 若 dt 有节点接入文档，把 Linger 列为前置条件。

## 附：同日另一个小问题

`dt sync {nodes} --artifact artifacts/loap_v1/X` 在 cwd 含该路径、但配置的项目路径是另一个目录时报 `artifact path does not exist: 'artifacts/loap_v1/X'`。报错里只给了相对路径，没有说"相对于项目路径 ~/cw/project/lrd 解析"。建议错误信息带上解析所用的项目根，或在项目路径与 cwd 不一致时提示。

## 处理结果（2026-09-05）

- `dt ps` 默认视图：只要可见行中有被阻塞/离线的排队任务就显示 issue 列，原因压缩为 "节点: 具体原因"（例如 `psibot-yw: GPU runtime requires loginctl Linger=yes`）。提交 c96872d。
- `dt doctor` 已有 Linger 检查，修复提示改为可直接执行的 `loginctl enable-linger "$(id -un)"`；入门文档把 Linger 列为 GPU 节点前置条件。
- `dt sync --artifact` 的路径报错现在带上解析所用的项目根。
- 未做：让 `sync`/`seed` 也检查 Linger——健康检查归 `doctor`，`ps`/`info` 已直接给出原因与修法。

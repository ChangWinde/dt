# dt 设计风险：`dt compact` 删除 `code/` 时不检测其中的非快照文件，写在 `code/` 里的产物无声消失

- 报告日期：2026-09-02（2026-09-05 整理归档）
- 来源：研究项目07 lrd
- dt 版本：0.13.4 早期构建
- 严重程度：高（不可逆数据丢失，无警告、无回执记录）

## 现象

本项目早期的 runner 把结果 bundle 写在 job 工作目录的 `code/artifacts/`（快照副本内），而不是 `$DT_OUTPUT_DIR`。`dt compact` 按设计删除终态 job 的 `code/`，于是四个未收割的训练束（每个约 5 GPU 小时）和两天的判决格原始 `result.json` 一起被删除。compact 过程没有任何警告，回执里也没有"删除了非快照文件"的记录。

责任在使用方（产物应写到 `$DT_OUTPUT_DIR`），但 dt 有足够信息在删除前发现异常：`code/` 是快照的副本，快照的 tree hash 已知，任何不属于快照的文件都是运行期新写入的。

## 期望

- compact 前把 `code/` 与快照 manifest 比对；存在非快照文件时默认拒绝该 job（或先移到 `outputs/`），并在摘要中列出。
- `dt run` 结束时若 `DT_OUTPUT_DIR` 为空而 `code/` 出现新文件，在 `dt info` 中给出警告（"outputs written into the disposable snapshot copy"）。
- 回执记录被删除的非快照文件清单，哪怕只是路径与大小。

## 规避（已在项目内落实）

所有 runner 经统一函数解析输出根：dt 下落到 `$DT_OUTPUT_DIR/artifacts/...`，本地运行落到仓库 `artifacts/`；另有收割脚本扫描 `<worker>/jobs/*/outputs/` 把产物拉回正典库。

## 处理结果（2026-09-05）

- 修复：节点侧 census 在删除前用 `find -newer <state>/started_at` 统计 `code/` 里运行期新写入的常规文件（只 stat，不读内容）；有则报告 `code_modified`（含文件数与字节数）并保留，`--json` 里有 `code_modified_jobs`；`dt compact --prune-modified` 是显式升级，删除时把被删清单（大小、路径，最多 1 万行）写到回执旁的 `code-pruned.modified.tsv`；agent 自动 sweep 永不删除这类树。提交 2df34e3、4898a36。
- `dt info` 现在对 `code/` 里有运行期新文件的任务给出警告（`code_modified_files`/`code_modified_bytes`），并明确 `dt pull` 不抓取 `code/`。
- 说明：`dt pull` 只恢复 `outputs/`，这类文件需从节点 `<job_dir>/code` 自行拷出；文档 operations.md "Safe compaction" 已写明。
- 回归测试：`tests/test_compact_modified_code.py`（真实 bash 执行 census：保留/显式删除/未改动三种情形）。

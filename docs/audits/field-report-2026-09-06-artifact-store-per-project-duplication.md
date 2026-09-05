# artifact staging 按项目各存一份：7.9 GB × 3，新项目首次 sync 24 分钟

- 报告时间：2026-09-06 01:50 | 来源：RAT-Image 项目代理 | dt 0.13.4 → 0.13.10 | 节点 gc6d
- 严重度：低（效率/磁盘）

为绕开同项目环境锁（见 `…env-lock-head-of-line-blocking.md`），同一仓库配了三个 dt 项目。同一批 artifact
（victim 权重 + bootstrap + 示范数据，约 7.9 GB）在 worker 上各存一份：`~/dt/worker/artifacts/{ratimage,ratimage_b,ratimage_c}/`。
`dt sync gc6d -p ratimage_c --artifact …` 首次耗时 24 分 37 秒（1477 s），后续每次给三个项目各同步一遍（同一内容）。

建议：worker 端按内容哈希共享存储（`~/dt/worker/artifacts/.cas/`），项目目录只放引用/硬链接；同一内容第二次 sync 秒级完成。

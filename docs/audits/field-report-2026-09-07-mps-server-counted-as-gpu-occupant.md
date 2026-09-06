# GPU 空闲判据把 `nvidia-cuda-mps-server` 当成占卡进程（功能缺口，2026-09-07）

- 来源：研究项目 lrd（psibot-hm 头节点，dt 0.13.6，工作节点 psibot-ds / ys）。
- 现象：在节点上以工作用户启动 NVIDIA MPS（`nvidia-cuda-mps-control -d`）后，第一个客户端接入会常驻一个 `nvidia-cuda-mps-server` 进程（约 28 MiB 显存）。此后 `dt free` 报该节点 `0/1 GPU free`（负载 0%、显存 24/24G 空闲），队列有 60 多个任务却不再向该节点派发；`dt ps` 里任务停在 queued。
- 复现：任一节点 `nvidia-cuda-mps-control -d`，跑一个 CUDA 进程再退出（server 留下），`dt free`。
- 影响：MPS 对本项目这类"一卡多进程小核函数"的负载吞吐提升约 1.67 倍（同一 3 格包每 8k 转移 50 s → 30 s），但无法与 dt 的调度并存。
- 当前规避：在任务命令内启动 MPS 并用 `trap ... EXIT` 在结束时 `echo quit | nvidia-cuda-mps-control` 退出，使任务之间不留 server 进程。副作用：`quit` 在仍有客户端时会阻塞，须加 `timeout`。
- 建议：空闲判据忽略进程名为 `nvidia-cuda-mps-server` / `nvidia-cuda-mps-control` 的进程（或只按显存阈值与租约判定）；更进一步可提供 `dt run --mps` 由 dt 负责在节点上按需拉起与回收 MPS。

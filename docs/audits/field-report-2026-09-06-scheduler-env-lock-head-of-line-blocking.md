# 调度：同项目环境锁造成队首阻塞，空闲 GPU 不派发后续可运行作业

- 报告时间：2026-09-06 01:50 | 报告来源：RAT-Image 项目（`~/cw/project/RAT-Image`）的自动实验代理
- dt 版本：观察发生于 0.13.4（2026-09-05 10:00–23:30），head 现为 0.13.10；节点 `gc6d`（2× RTX 4090 D，经 frp 隧道，worker 用户 baifengshuo），head `headstar`
- 严重度：高（两卡节点长时间只用一卡）

## 现象
同一项目（同一 uv 环境）的作业 A 在 GPU0 运行时，队首是同项目的作业 B。B 等待项目环境锁，GPU1 空闲；
排在 B 之后、**属于其他项目、可以立即运行**的作业也不被派发。直到 A 结束，B 才上 GPU0，GPU1 全程空闲。

三次实录（`dt ps` / `nvidia-smi` 于 gc6d）：
1. 2026-09-05 10:15：`b18x-faucetopen-ms100`（项目 ratimage）在 GPU0 运行；队首 `bc240-door-open-500000-s1`（ratimage）queued；
   其后 `bc240-window-open-500000-s1`（ratimage_b）等 60 个作业排队。GPU1 `0 MiB, 0 %` 持续约 30 分钟。
2. 2026-09-05 23:20：连续四次单作业提交 `chunk7-01`、`chunk8-01`、`chunk9-01`、`chunk10-01` 全落在项目 ratimage
   （我方轮转脚本的缺陷，见下），`chunk7-01` 在 GPU0 运行，`chunk8-01` queued #1，GPU1 空闲；把 `chunk8-01` 改提到 ratimage_b 后立刻在 GPU1 运行。
3. 2026-09-05 18:1x：他项目 `lrd-online-clean` 的 `lrd-gc6a-001` 在 GPU0 运行、同项目 `lrd-gc6a-002..008` 排队，GPU1 空闲；
   `dt free --explain` 输出：`dt 2/17 GPU free · 0 running · 22 queued · next is dispatching on gc6d` / `queue model 2 runnable · 0 blocked · 20 waiting`，
   没有把"等待环境锁"列为阻塞原因。

## 预期
AGENTS.md「Queue behavior」写明："A job-specific blocker does not hold up runnable work behind it … FIFO is preserved among
jobs competing for the same capacity"。环境锁是作业特定的阻塞（其他项目的作业不与它竞争同一把锁），后面的可运行作业应被派发到空闲 GPU。

## 影响
- 两卡节点吞吐退化到约 1.0–1.3 卡；我们不得不在 `pyproject.toml` 加空 extra `sched`、配置三个项目（`ratimage` / `ratimage_b` / `ratimage_c`）
  轮转提交，才让两卡同时工作（`~/cw/project/RAT-Image/dev/reports/dt.md` 2026-09-05 记录）。
- 多项目共用节点时，一个项目的长队列会独占节点。

## 建议
1. 调度器把"环境锁被占"视作 job-specific blocker：跳过该作业，继续扫描后续可运行作业（保留同容量竞争者之间的 FIFO）。
2. `dt free --explain` / `dt ps` 的 queue model 把 `env-lock` 单列为阻塞原因（现在算进 `waiting`，看不出来）。
3. 若环境已构建完成，同项目多作业并发运行本身是否必须串行？若锁只保护 `uv sync`，构建完成后应释放。

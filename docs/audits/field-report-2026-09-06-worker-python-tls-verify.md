# worker 上 dt 环境里的 Python 无法验证 TLS 证书（torchvision 权重下载 CERTIFICATE_VERIFY_FAILED），而 curl 正常

- 日期：2026-09-06 15:35；报告方：RAT-Image 项目（agent 分支 B）；dt 0.13.12；节点 gc6d（腾讯云，Ubuntu）
- 严重度：低中（任何在作业里"首次下载预训练权重/数据集"的脚本都会在 worker 上失败）

## 现象
作业 `chunk28-01`（`torchvision.models.resnet18(weights="IMAGENET1K_V1")`）在 worker 上 23 秒失败：
```
urllib.error.URLError: <urlopen error [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1016)>
```
同一节点 `curl -sI https://download.pytorch.org/models/resnet18-f37072fd.pth` 返回 `HTTP/2 200`。本机（head）同一脚本正常。
launcher 给 uv 设置了 `UV_SYSTEM_CERTS=1`，但作业进程里的 Python `ssl` 模块没有可用的 CA（uv 管理的 Python 不读系统证书目录，
且环境里未装/未指向 certifi）。

## 期望
- launcher 在作业环境里导出 `SSL_CERT_FILE`/`REQUESTS_CA_BUNDLE`（系统 CA 路径，如 `/etc/ssl/certs/ca-certificates.crt`），
  或在 `dt doctor` 里检查"作业 Python 能否验证 TLS"并给出提示。
- 文档里说明：需要在线下载的资源应先 `dt sync --artifact` 进去（我们的规避：把权重放进 `data/pretrained/torch/hub/checkpoints/`
  并在载荷里 `export TORCH_HOME`）。

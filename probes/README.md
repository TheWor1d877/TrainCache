# 可行性探针执行手册(阶段0 第一部分 / v4 第一步)

装置参数统一在 `config.py`。装置说明、结果与分析在 [可行性探针报告.md](可行性探针报告.md)(§2 装置、§3 结果、§4 分析)。探针C / D 轨的运行细节见 [probe_c/README.md](probe_c/README.md),判定口径与结果见报告 §3.3、§3.4。

## 0. 环境检查

```bash
docker version                      # 29.1.3,已配镜像加速
python3 -c "import torch, webdataset, numpy, PIL; print(torch.__version__, webdataset.__version__)"
                                    # torch 2.14.1+cpu, webdataset 1.0.2
```

探针A/B 与消费顺序探针用系统 python3;探针C / D 轨测量时固定 `/usr/bin/python3.12`(依赖同上,pillow 12.3.0 cp312 装在用户级 site-packages)。本机现已没有 `/usr/bin/python3.12`(系统 python3 是 3.14),重跑前先重建解释器与依赖。

## 步骤总览

| # | 命令 | root? | 预期耗时 |
|---|---|---|---|
| 1 | `python3 setup/01_synth_data.py` | 否 | ~6 分钟(合成 2GB tar) |
| 2 | `bash setup/02_source_up.sh` | 否 | 秒级(nginx 本地已有) |
| 3 | `sudo bash probe_a/tc_setup.sh` | **是** | 秒级 |
| 4 | `sudo bash probe_a/run_cold.sh` | **是** | ~25 分钟(7×2.7min) |
| 5 | `bash probe_a/run_hot.sh` | 否 | ~2 分钟 |
| 6 | `python3 probe_a/judge.py` | 否 | 秒级 |
| 7 | `bash probe_b/run.sh` | 否 | ~10 分钟(六组) |
| 8 | `python3 probe_b/judge.py` | 否 | 秒级 |
| 9 | `python3.12 probe_d/judge.py` | 否 | ~10 秒(verify_wds → verify_torch → reconstruct + 汇总) |
| 10 | `bash probe_c/run_c.sh` 然后 `python3.12 probe_c/judge_c.py` | 否(优先 `sudo -n`,不可用回退特权 docker) | ~35 分钟(冷 w4/w0/w8 各 3 epoch≈9min + 热 w4≈3.5min;判定秒级) |
| 11 | `bash probe_c/run_track_d.sh` 然后 `python3.12 probe_c/judge_track_d.py` | 否(同上回退) | ~40 分钟(10 epoch 连续跑 + 限内存/采样/恢复;判定秒级) |
| 12 | `sudo bash probe_a/tc_teardown.sh` | **是** | 秒级 |
| 13 | `bash setup/03_source_down.sh` | 否 | 秒级 |

sudo 清单(需要密码的只有三处):步骤 3、4、12,终端会提示输密码。步骤 10-11 在 sudo 不可用时自动回退带 NET_ADMIN / 特权的 docker 容器执行等价操作,无需密码。

## 步骤说明与关键点

步骤 1:确定性合成,固定种子 20261006;σ 自动校准到 ~160KB/张,重跑逐字节一致。

步骤 2:起 nginx 源站容器,静态服务,tar 目录只读挂载,无上传步骤。选型原因与读路径等价性见报告 §2.3、§4.4。

步骤 3:tc 挂源站容器 netns 内 eth0 的 egress(源站出站方向)。看到 `tbf rate 100mbit` 与 `netem delay 20ms` 即成功。docker0 与 veth host 侧挂点已被实证否决,勿回退(证据见报告 §2.4);qdisc 随容器重建消失,需重挂。

步骤 4:每轮清缓存 + drop_caches + 下载,7 轮约 25 分钟(2GB @ 100mbit ≈ 2.7min/轮)。期间可另开终端跑 `sudo nsenter -t $(docker inspect -f '{{.State.Pid}}' traincache-source) -n tc -s qdisc show dev eth0`,确认 tc 计数在涨。

步骤 5:热读 7 轮,每轮自动附带零网络验证(9000 端口连接峰值,期望 0)。

步骤 6/8:判定脚本输出中文报告与 `results/probe_*/verdict.json`,exit 0 绿 / 1 红。任一红灯即停止后续探针,按报告 §4.1 四项排查装置:tc 计数、热读零网络、盘速、假冷读。

步骤 9(消费顺序探针):单进程(num_workers=0)纯 CPU,依赖步骤 1 的合成数据。`judge.py` 按序跑 `verify_wds.py → verify_torch.py → reconstruct.py`,汇总 7 点写 `results/probe_d/verdict.json`(7/7 通过 exit 0)。

步骤 10/11(探针C / D 轨):运行细节、测量纪律与判定口径见 [probe_c/README.md](probe_c/README.md);结论见报告 §3.3、§3.4。冷组要求步骤 3 的 tc 已挂,脚本会断言并在容器重建后自动重挂。

步骤 12/13:收尾。`tc_teardown.sh` 摘除限速恢复网络;源站容器随用随停,tar 数据留在 `data/tars/`。

## 常见问题

- 源站用 nginx:MinIO 镜像在所有可达镜像源均拉取失败;探针只走 HTTP 字节流读路径,nginx 与 MinIO 公开读等价,报告 §2.3 有说明。
- 9000 端口冲突:改 `config.py` 的 `MINIO_PORT`,其他脚本全部自动跟随。
- 探针B 冷组必须挂好 tc 再跑:未挂 tc 时冷读数与热态无差异,`judge.py` 判冷态红灯;补挂 tc 后重跑步骤 7。
- 数据与结果都在 `data/`、`results/`(已 gitignore),种子固定,可完全复现。

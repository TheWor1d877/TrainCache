# 可行性探针执行手册(阶段0 第一部分 / v4 第一步)

装置设计见 [design.md](design.md)。**先读 design.md 第 4 节(stall 口径),再跑。**
探针C / D 轨的装置细节与判定口径见 [probe_c/README.md](probe_c/README.md)。

## 0. 环境检查(本机已确认 ✓)

```bash
docker version                      # ✓ 29.1.3,已配镜像加速
python3 -c "import torch, webdataset, numpy, PIL; print(torch.__version__, webdataset.__version__)"
                                    # ✓ torch 2.14.1+cpu, webdataset 1.0.2(阶段0 第一部分用)
/usr/bin/python3.12 -c "import torch, webdataset, numpy, PIL; print(torch.__version__, webdataset.__version__)"
                                    # ✓ torch 2.14.1+cpu, webdataset 1.0.2(探针C/D 与 D 轨用;pillow 12.3.0 cp312 已 pip --user 补装)
```

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
| 14 | 填 `reports/探针结果报告模板.md` | — | — |

**sudo 清单(需要密码的只有三处)**:步骤 3、4、12。终端会提示输密码。
探针C / D 轨(步骤 10-11)在 sudo 不可用时自动回退带 NET_ADMIN / 特权的 docker 容器执行等价操作,无需密码。

## 步骤说明与关键点

**步骤 1**:确定性合成,固定种子;σ 自动校准到 ~160KB/张。重跑结果逐字节一致。

**步骤 2**:起源站容器。原设计为 MinIO,因镜像在所有可达源拉取失败,降级为本地已有的
nginx 静态服务(读路径语义等价,理由见 design.md 装置图下方的降级说明)。
tar 目录只读挂载,无上传步骤;kind 现勘阶段仍用真 MinIO。

**步骤 3**:tc 挂源站容器 netns 内 eth0 的 egress(源站出站方向)。看到
`tbf rate 100mbit` 与 `netem delay 20ms` 即成功。挂点选择经过两次实证踩坑
(docker0、veth host 侧均无效),记录见 design.md 第 3 节,勿回退。

**步骤 4**:每轮清缓存 + drop_caches + 下载,7 轮约 25 分钟(2GB @ 100mbit ≈ 2.7min/轮)。
期间可另开终端跑 `sudo nsenter -t $(docker inspect -f '{{.State.Pid}}' traincache-source) -n tc -s qdisc show dev eth0` 看计数在涨(红灯排查第 1 条)。

**步骤 5**:热读 7 轮,每轮自动附带零网络验证(9000 端口连接峰值,期望 0)。

**步骤 6/8**:判定脚本输出中文报告 + `results/probe_*/verdict.json`,红灯时
按 design.md 第 6 节排查。**探针A 红灯就不要跑探针B**;探针B 红灯同理。

**步骤 9(消费顺序探针)**:单进程(num_workers=0)纯 CPU,依赖步骤 1 的合成数据。
`judge.py` 按序跑 `verify_wds.py → verify_torch.py → reconstruct.py`,汇总 7 点写
`results/probe_d/verdict.json`(7/7 通过 exit 0)。

**步骤 10(探针C)**:`run_c.sh` 跑四组 `cold_w4 → cold_w0 → cold_w8 → hot_w4`,
每组 3 epoch×400 步;随后 `judge_c.py` 判定(exit 0 绿 / 1 红)。**纪律:epoch 之间
绝不清缓存**(测量对象);每组开跑前做一次冷态并检查 kind 节点负载(与 kind 上
其他子代理互斥)。解释器必须用 `/usr/bin/python3.12`;冷组要求 tc 已挂
(步骤 3,脚本会断言/自动重挂)。

**步骤 11(D 轨)**:`run_track_d.sh` = tc 断言 → kind 负载检查 → `docker update`
限源站容器 1GiB(欠配 50%,memory-swap=memory)→ 后台内存采样(2s)→ 跑前一次冷态
→ 10 epoch 连续跑 → 恢复内存限制并记档(`--memory 0` 零值语义不生效时回退 128GiB
等效)。`judge_track_d.py` 判定:exit 0 绿 / 1 红 / 2 无效(前置门不过判无效)。

**步骤 12/13**:收尾。`tc_teardown.sh` 摘除限速恢复网络;源站容器随用随停,tar 数据
在 `data/tars/`(种子固定可复现)。

## 常见问题

- **源站为何不是 MinIO**:MinIO 镜像在所有可达镜像源(docker hub 直连/玄源/DaoCloud/1ms/
  quay/阿里云)均拉取失败,装置降级为本地已有的 nginx 静态服务,读路径语义等价,
  理由见 design.md 装置图下方降级说明;kind 现勘阶段仍用真 MinIO。
- **9000 端口冲突**:改 `config.py` 的 `MINIO_PORT`,其他脚本全部自动跟随。
- **探针B 冷态没挂 tc 就跑**:数字会和热态无差异,judege 会判冷态红灯 —— 补挂 tc 重跑步骤 7 即可。
- **跑完想复现**:数据与结果都在 `data/`、`results/`(已 gitignore),种子固定可完全复现。

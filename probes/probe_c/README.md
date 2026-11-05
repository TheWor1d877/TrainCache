# 探针C:多 epoch 训练循环全程 data stall

验证 stall 是训练全程持续存在的状态(对照:只在启动预热期出现的一次性瞬态)。绿灯 = epoch0 中段与 epoch1/2 切换后的窗口指标 ≥ 冷态基准的 30%。判定口径、实测结果与分析见 [../可行性探针报告.md](../可行性探针报告.md) §3.3、§4.5;D 轨见 §3.4、§4.6。

## 装置

`train_multi_epoch.py` 是本探针的核心装置(与 D 轨共用),从探针B(`probe_b/train.py`)移植,打点口径一致(见文件头注释):

- 每步:`t0`=上一步 compute 结束 → `next(batch)` t1 → 极小 matmul(1024²×8,单线程)t2;`wait=t1-t0`、`step=t2-t0`、`stall=wait/step`;compute 矩阵开头生成一次、跨 epoch 复用;
- 每 epoch 重建 loader(wds pipeline 在 worker 进程内的副本无法换 seed):分片级 = 父进程 `random.Random(seed+epoch)` 生成 0..15 置换后显式构造 URL 列表(`shardshuffle=False`,读取顺序=列表顺序);样本级 = `.shuffle(100, seed=seed+epoch)`;有效种子 = 传入值,同 seed 复跑逐位相同(probe_d 验证点3 实测);
- epoch 长默认 400 步(16 tar × 800 图 / batch 32 = 一遍完整数据);epoch 0 前 `PROBE_C_WARMUP=10` 步为 warmup(计入步号、标记排除),epoch 1/2 步 0 起全部计入;
- epoch 边界不重置时钟,epoch 1/2 首步 wait 含 loader 重建 + worker 重启,切换代价即测量对象(`loader_build_s` 单独留档)。

CLI 全部参数化(`--source {http,local} --workers N --epochs N --steps-per-epoch N --seed N --out PATH`),缺省取 `config.py` 的 `PROBE_C_*`;D 轨传 `TRACK_D_*` 对应值(10 epoch、seed 20261014 等)复用同一打点/留档结构。缓存命中、内存采样等 D 轨专用逻辑不在本装置内。

## 运行

```bash
bash probes/probe_c/run_c.sh          # 四组:cold_w4 → cold_w0 → cold_w8 → hot_w4,每组 3 epoch×400 步
python3.12 probes/probe_c/judge_c.py  # 判定,输出 verdict.json(exit 0 绿 / 1 红)
```

- 解释器:测量时固定 `/usr/bin/python3.12`(webdataset 1.0.2 / torch 2.14.1+cpu 装在用户级 site-packages;pillow 12.3.0 cp312 补装到 `~/.local/lib/python3.12/site-packages`)。本机现已没有该解释器(系统 python3 是 3.14),重跑前先重建解释器与依赖;
- 结果:`results/probe_c/c_<组名>.json`(per_step/per_epoch/environment)、`env_<组名>.json`(组前环境快照)、`verdict.json`(判定+中文报告全文);
- 冷组走 `http://127.0.0.1:9000`(tc:100mbit+20ms),热组读 `TARS_DIR` 本地路径;冷组要求 tc 已挂,脚本会断言,容器重建后自动重挂;
- 环境约束:本机 sudo 无 NOPASSWD 也无 tty,脚本优先 `sudo -n`,不可用时回退带 NET_ADMIN 的 docker 容器(在源站 netns 内执行同一条 tc 命令)与特权 docker 容器(drop_caches、清 root 属主缓存目录),回退路径记入每组 `environment.tc_check/cache_reset/drop_caches` 的 `via` 字段;tc 断言以出现 `tbf` 为准(容器默认的 `noqueue` 不算挂载);源站容器不在时 run_c.sh 自动执行 `setup/02_source_up.sh`。

## 测量纪律(勿违反)

- epoch 之间不清缓存、不重启容器、不 drop_caches:epoch 切换后 stall 是否回升正是测量对象。只在每组开跑前做一次组前冷态(`rm -rf probes/.cache/agent` + `sync` + `drop_caches`),组间只 sleep 5;
- 互斥:kind 集群上可能有其他任务(修复/安装)占用宿主资源。run_c.sh 在每组计时测量前用 `docker stats` 检查 `traincache-control-plane/worker*` 容器 CPU,>30% 则等 2 分钟再查(最多 15 轮),结论写入该组 environment JSON;持续繁忙则放弃本轮测量;
- hot_w4 是装置判别力哨兵:本地热读在四个窗口的尾占比应全部低于冷组阈值 6.5%。哨兵 FAIL 视为装置污染(宿主负载/tc 状态),本轮冷组结论不可用。

## 判定口径(judge_c.py)

完整口径与实测见报告 §3.3。要点:cold_w0 看窗口内 stall 中位数,cold_w4/cold_w8/hot_w4 看窗口内 `wait_s≥0.5` 的步数占比;阈值 = 0.30×冷态基准,基准从 `probes/results/probe_b/cold_w*.json` 的 per_step 动态计算;hot_w4 哨兵取与冷组同一标尺;窗口指标附 bootstrap 95% CI(seed=42)。

## D 轨:多 epoch 欠配下的 stall 重复形态

测量 stall 在缓存欠配(nginx 源站容器 cgroup 限 1GiB,即数据集 2GB 的 50%,源站侧只能缓存一半工作集)下的逐 epoch 重复形态,为 v4 全程收益提供场景数据。单 Job 连续 10 epoch、每 epoch 换 seed(`seed=20261014+e`),装置复用 `train_multi_epoch.py`,行为语义未改,仅走 CLI。

```bash
bash probes/probe_c/run_track_d.sh           # ~40min:tc 断言→kind 检查→限内存→采样→冷态→跑→恢复
python3.12 probes/probe_c/judge_track_d.py   # 判定(exit 0 绿 / 1 红 / 2 无效)
```

运行时序(run_track_d.sh):tc 断言 → kind 节点负载检查 → `docker update --memory=1073741824 --memory-swap=1073741824 traincache-source` 并核对 HostConfig 值 → 后台每 2s 采样源站容器 cgroup v2 的 `memory.current` 与 `memory.stat` 的 `file` 行(`mem_track_d_w4.jsonl`,meta/采样/final 三类行)→ 跑前一次冷态 → 10 epoch 连续跑(`track_d_w4.json`)→ 停采样、记 OOM 状态、恢复内存限制并验证。epoch 之间不清缓存、不重启容器:重复形态正是测量对象。

已知限制:本机 docker 29 把 `update --memory 0` 视为「不变更」(零值语义),0/0 请求不生效;脚本检出后回退为 128GiB(≈4×宿主内存)等效恢复,实际恢复值记入 final 行 `limit_restored`;容器不重建则无法回到 HostConfig=0,而本装置禁止重建容器。

判定口径(judge_track_d.py → `verdict_track_d.json` / `track_d_windows.json`):

- 前置门(欠配真实生效):`file` 曾 ≥ 0.9×1GiB,且 `memory.current` 全程 ≤ 上限、无 breach / OOM kill;不满足判「无效」(exit 2),数据不用于红绿判定;
- 绿灯 = 前置门 + 判据①(相邻 epoch 窗口曲线 ≥6/8 对 Pearson r≥0.5)+ 判据②(epoch2-9 中位 stall CV ≤0.30),verdict 记「重复形态确认」;否则红灯(exit 1),照实报,不作装置外解释;
- 报告项③(epoch0 与 epoch1..9 中位 stall 中位数的比值)只报告、不参与判定,原因见报告 §4.6;verdict 同时报告 epoch1 首窗切换峰值与每 epoch 前 20 步切换窗尾占比;
- 实测结果与移交数据见报告 §3.4:`results/probe_c/track_d_windows.json`、`track_d_w4.json`、`mem_track_d_w4.jsonl`。

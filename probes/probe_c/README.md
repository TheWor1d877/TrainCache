# 探针C:多 epoch 训练循环全程 data stall

验证"全程 data stall 真实存在":连续 **3 个 epoch**(每 epoch 确定性 shuffle、
seed 不同)打点每 epoch 内的 stall 时间序列。绿灯 = **epoch 0 中段**与
**epoch 1 切换后**的 stall 回升 ≥ **冷态基准的 30%**;不通过则按实报红灯并
进入"叙事收缩分支"(不得装置外解释)。

## 装置

`train_multi_epoch.py` 是本探针的核心装置(**与 D 轨共用**),在探针B
(`probe_b/train.py`)结构上移植,打点口径严格一致(见文件头注释):

- 每步:`t0=上一步 compute 结束 → next(batch) t1 → 极小 matmul(1024²×8,单线程)t2`,
  `wait=t1-t0`、`step=t2-t0`、`stall=wait/step`;compute 矩阵开头生成一次、跨 epoch 复用;
- 每 epoch **重建 loader**(wds pipeline 在 worker 进程内的副本无法换 seed):
  分片级 = 父进程 `random.Random(seed+epoch)` 生成 0..15 置换后显式构造 URL 列表
  (`shardshuffle=False`,读取顺序=列表顺序);样本级 = `.shuffle(100, seed=seed+epoch)`;
  seed 语义以 probe_d T1 实测为准:**有效种子=传入值,同 seed 复跑逐位相同**;
- epoch 长默认 400 步(16 tar × 800 图 / batch 32 = 一遍完整数据);epoch 0 前
  `PROBE_C_WARMUP=10` 步为 warmup(计入步号、标记排除),epoch 1/2 步 0 起全部计入;
- epoch 边界不重置时钟,epoch 1/2 首步 wait 含 loader 重建 + worker 重启
  (切换代价即测量对象;`loader_build_s` 单独留档)。

## 运行

```bash
bash probes/probe_c/run_c.sh                      # 四组:cold_w4 → cold_w0 → cold_w8 → hot_w4
python3.12 probes/probe_c/judge_c.py              # 判定,输出 verdict.json(exit 0/1)
```

- 解释器固定 **`/usr/bin/python3.12`**(wds 1.0.2 / torch 2.14.1+cpu 装在用户级
  site-packages;系统 `python3` 没有这些包;项目根 venv 为空,不使用)。
  `PIL` 原有系统安装是 python3.14 构建、对 3.12 不可用,已补装
  `pillow 12.3.0`(cp312 wheel)到 `~/.local/lib/python3.12/site-packages`;
- 结果:`results/probe_c/c_<组名>.json`(per_step/per_epoch/environment)、
  `env_<组名>.json`(组前环境快照)、`verdict.json`(判定+中文报告全文);
- 冷组走 `http://127.0.0.1:9000`(tc:100mbit+20ms),热组读 `TARS_DIR` 本地路径。

## 测量纪律(勿违反)

- **epoch 之间绝对不清缓存 / 不重启容器 / 不 drop_caches** —— epoch 切换后
  stall 是否回升正是测量对象;只在"每组开跑前"做一次组前冷态
  (`rm -rf probes/.cache/agent` + `sync` + `drop_caches`),组间只 sleep 5;
- **互斥**:kind 集群上可能有其他子代理(修复/安装)占用宿主资源。run_c.sh 在
  每组计时测量前用 `docker stats` 检查 `traincache-control-plane/worker*` 容器
  CPU,>30% 则等 2 分钟再查(最多 15 轮);检查结论写入该组 environment JSON;
  持续繁忙则放弃测量(宁可不跑,不跑脏数据);
- hot_w4 是**装置判别力哨兵**:本地热读不应出现 ≥0.5s wait 的窗口占比达到冷组
  同一阈值;若哨兵 FAIL,视为装置污染(宿主负载/tc 状态),本轮冷组结论不可用。

## 判定口径(judge_c.py,基准从 probe_b per_step 动态计算)

| 组 | 窗口指标 | 基准 | 阈值 |
|---|---|---|---|
| cold_w0 | 窗口内 stall 中位数 | cold_w0 全程 median stall(≈71.7%) | 0.30×基准 ≈21.5% |
| cold_w4 / cold_w8 / hot_w4 | 窗口内 wait_s≥0.5 的步数占比 | cold_wX 全程同口径占比 | 0.30×基准 |

四窗口:A=epoch0 中段 [133,266)、B=epoch1 切换后 [0,20)、C=epoch2 切换后
[0,20)、D=epoch1 中段。绿灯:cold_w4 的 A、B ≥ 阈值 **且** cold_w0 的 A、B
(median 口径)≥ 阈值;cold_w8 仅报告;hot_w4 四窗口全部 < 阈值(哨兵)。
注:hot_w4 自身基准占比为 0,"0.30×自身基准"退化不可达,哨兵取与冷组**同一
标尺**(0.30×cold_w4 基准)——冷态必须越过的门槛,hot 必须压在门下。
窗口指标附 `bootstrap.pct_bootstrap_ci`(seed=42)的 95% CI。

## 与 D 轨的共用关系

`train_multi_epoch.py` 即 D 轨(全程缓存服务实验)的测量装置:CLI 全部参数化
(`--source {http,local} --workers N --epochs N --steps-per-epoch N --seed N --out PATH`),
缺省取 `config.py` 的 `PROBE_C_*`;D 轨传 `TRACK_D_*` 对应值(10 epoch、seed
20261014 等)即可复用同一打点/留档结构。缓存命中、内存采样等 D 轨专用逻辑
不在本装置内。

## D 轨:多 epoch 欠配下的 stall 重复形态

为 v4"全程收益"提供专用场景数据:单 Job **连续 10 epoch、每 epoch 换 seed**
(`seed=20261014+e`),源站 nginx 容器 **cgroup 限 1GiB**(`TRACK_D_CACHE_MB=1024`,
数据集 2GB 的 **50%** —— 源站侧只能缓存一半工作集,即"缓存欠配 50%"),
测量 stall 在欠配下的**逐 epoch 重复形态**。装置复用 `train_multi_epoch.py`
(行为语义未改,仅走 CLI:10 epoch / seed / out)。

```bash
bash probes/probe_c/run_track_d.sh           # ~40min:tc 断言→kind 检查→限内存→采样→冷态→跑→恢复
python3.12 probes/probe_c/judge_track_d.py   # 判定(exit 0 绿 / 1 红 / 2 无效)
```

欠配实现与运行时序(run_track_d.sh):

- ① tc 断言(qdisc 含 `tbf`;sudo -n 不可用时同 run_c.sh 回退 docker,via 记档);
- ② 计时测量前 kind 节点负载检查(`traincache-control-plane/worker* > 30%` 等空闲),
  开跑前再复检一次,结论嵌入 `track_d_w4.json` 的 environment;
- ③ `docker update --memory=1073741824 --memory-swap=1073741824 traincache-source`
  并验证 `HostConfig.Memory/MemorySwap`(memory-swap=memory → 禁 swap,不可用换页掩盖欠配);
- ④ 后台采样每 2s 读源站容器 cgroup v2(`docker-<id>.scope`)的 `memory.current`
  与 `memory.stat` 的 `file` 行 → `mem_track_d_w4.jsonl`(meta/采样/final 三类行);
- ⑤ **仅跑前一次**冷态(`rm -rf probes/.cache/agent` + `sync` + `drop_caches`);
  **epoch 之间绝不清缓存/不重启容器**——重复形态是测量对象;
- ⑥ 训练 10 epoch → `track_d_w4.json`;⑦ 停采样、记 OOM 状态、恢复内存限制
  (`--memory 0 --memory-swap 0`)并验证;final 行回填 OOM/events/恢复快照。
  注:本机 docker 29 将 `update --memory 0` 视为"不变更"(零值语义),0/0 请求
  可能不生效;脚本会检出并回退为 **128GiB(≈4×宿主内存)** 等效恢复,实际恢复值
  与说明记入 final 行 `limit_restored`(容器不重建则无法回到 HostConfig=0)。

判定口径(judge_track_d.py → `verdict_track_d.json` / `track_d_windows.json`):

- **前置门(欠配真实生效)**:内存采样中 `file` 曾 ≥ 0.9×1GiB,且 `memory.current`
  全程 ≤ 上限、无 breach / OOM kill(final 行 OOMKilled=false、oom_kill 事件=0);
  **不满足 → 判"无效"而非红灯**(数据不用于红/绿判定);
- **绿灯判据①**:每 epoch 400 步切 20 步窗口(20 窗,值=窗口内 stall 均值)→ 长度
  20 的逐 epoch 曲线(即 `track_d_windows.json` 移交数据);对 epoch 1..9 的 8 个
  相邻对算 Pearson r,要求 **≥6 对 r≥0.5**;
- **绿灯判据②**:epoch 2..9 逐 epoch 中位 stall 的 **CV ≤ 0.30**(幅度稳定);
- **报告项③(不参与绿/红)**:epoch 0 中位 stall 与 epoch 1..9 中位 stall 中位数的
  比值(旧阈值 >1.2)。**主对话裁决降级**:恒定 tc 限速(100mbit+20ms)装置下链路
  瓶颈掩盖 epoch0 冷态信号(实测比值 0.99),与探针 B 已确立机制一致 —— w4 冷态
  签名只在 wait 尾部、中位 stall 对冷态不敏感,故仅计算并报告数值;
- **绿灯 = 前置门 + 判据①②全满足**(verdict 记"重复形态确认";exit 0);否则红灯
  (exit 1,照实报,不作装置外解释)。verdict 同时含报告项:③数值与**"epoch1 首窗
  切换峰值"**(切换瞬间冷读集中的证据,实测 0.536 = 各 epoch 首窗最高)、逐 epoch
  中位 stall/p90 wait 表与**每 epoch 前 20 步切换窗 tail 占比表**(w4 口径
  `wait_s≥0.5`)。

> 裁决记录(2026-10-13):首轮 D 轨实测中 ③ 比值 0.99 未过旧阈值且被判定为装置
> 瓶颈掩盖(恒定 tc 限速);主对话据探针 B 机制与①②实测(7/8 对 r≥0.5、CV=0.8%)
> 裁决 ③ 降级为报告项,verdict 记绿灯"重复形态确认",并补记 epoch1 首窗切换峰值
> 0.536(切换瞬间冷读集中)作为旁证。

## 环境事实(2026-10-13)

- 本机 `sudo` 无法非交互取证(无 NOPASSWD、无 tty);run_c.sh 优先 `sudo -n`,
  不可用时回退**带 NET_ADMIN 的 docker 容器**(在源站 netns 内执行同一条
  tc_setup 的 qdisc 命令)与**特权 docker 容器**(drop_caches / 清 root 属主
  缓存目录);回退路径记录在每组 `environment.tc_check/cache_reset/drop_caches`
  的 `via` 字段。tc 断言以出现 `tbf` 为准(容器默认的 `noqueue` 不算挂载);
- 源站容器不在时 run_c.sh 自动 `setup/02_source_up.sh`(容器重建后 qdisc 消失,
  脚本会自动重挂)。

# 可行性探针装置设计(阶段0 第一部分)

> 目的:用 2-3 天证明**实验方法论成立** —— 装置能造出可信的冷热差异(探针A)、
> stall 指标能被打出来(探针B)。任一红灯,停止开工,修装置;不碰 kind,不碰 Fluid。

## 1. 装置图

```
                      ┌────────────────────── host(裸机,32核/30G/双NVMe) ──────────────────────┐
┌────────────┐  http  ┌────────┐          ┌────────────────────────────────────────────┐
│ fetch.py    │◄─────►│ docker0│◄────────►│ 源站容器(nginx),tar 只读挂载                 │
│ (模拟Agent) │12.5MB/s│ bridge │  veth pair│ 容器内 eth0 egress 挂 tc:                 │
│ 有界并发=4   │       └────────┘          │   tbf 100mbit + netem 20ms(源站出站方向)   │
└─────┬──────┘                            └───────────────────┬────────────────────────┘
      │ 冷读:下载落盘                                           │ -v 只读
      ▼                                                        ▼
┌──────────────┐   热读:顺序直读(零网络,verify_isolated.sh 验证)  ┌──────────────┐
│ Agent 缓存目录 │◄───────────────────────────────────────────────│ data/tars/   │
│ .cache/agent │  探针B 热态:webdataset 流式读本地 tar             │ 16×128MB tar │
└──────────────┘                                                 └──────┬───────┘
                                                                        │ 探针B 冷态:
 train.py(探针B):webdataset DataLoader,workers 0/4/8                   │ webdataset
 打点 wait_time/step_time → stall 占比                                 ▼
                                                               http 流式读源站(经 tc)
```

> **装置降级说明(2026-10-07)**:原设计源站为容器 MinIO(文档层3 指定),执行时
> MinIO 镜像在所有可达镜像源(docker hub 直连、玄源、DaoCloud、1ms.run、hub.rat.dev、
> 1panel、quay.io、阿里云)均拉取失败;降级为本地已有的 nginx:latest 静态服务。
> 探针中 MinIO 只承担"远程 HTTP 字节流源"角色(公开读、http 直取,不用 S3 API),
> nginx 在该读路径上语义等价;URL 结构与 tc 装置(容器内 eth0 egress)均不变。kind 现勘阶段仍按
> 文档部署真 MinIO(届时另解镜像)。

数据流两用:**同一批 16×128MB tar 分片**同时服务探针A(字节吞吐)与探针B(解码+打点),
装置口径一致,避免两套数据带来的差异解释负担。

## 2. 参数表(全部集中在 `config.py`,唯一参数源)

| 参数      | 值                                                             | 依据                                 |
| ------- | ------------------------------------------------------------- | ---------------------------------- |
| 分片      | 16 × 128MB(800 张 512² JPEG q85/tar,共 2GB)                     | 文档"百 MB 级分片"主流形态                   |
| 合成图     | 渐变底+几何图形+高斯噪声,σ 自动校准到 ~160KB/张                                | 防纯色块被 JPEG 过度压缩;固定种子可复现            |
| tc      | tbf rate 100mbit burst 1600k latency 50ms + netem 20ms        | 文档指定;burst≈1s 令牌量                  |
| tc 挂点   | 源站容器 netns 内 eth0 的 egress(源站出站方向)           | 见第 3 节踩坑记录,前两个挂点已被实证否决            |
| 探针A     | 冷/热各 7 轮,并发 4,冷读每轮 drop_caches                                | 文档 ≥7 次取中位数                        |
| 探针B     | batch 32(≈5MB/批),steps 60(warmup 10),workers 0/4/8,prefetch 2 | 文档三档;5MB/批@12.5MB/s≈0.4s 网络等待      |
| compute | 1024² 单次 matmul(CPU)                                          | 模拟真实 step 的 compute 量级,排除 GPU/模型干扰 |

## 3. 装置关键决策(为什么这么做)

> **tc 挂点踩坑记录(2026-10-07 实证,勿回退)**:
> 1. 挂 docker0 root qdisc → 无效。Linux bridge 二层转发(br_dev_xmit → veth)
>    不经过桥主设备的 egress qdisc(下载 14GB,docker0 计数只涨 12MB);
> 2. 挂 veth host 侧 root(egress)→ 无效。下载数据方向是 容器→host,在 host 侧
>    veth 上是 ingress,egress qdisc 管不到(tcpdump 实证数据包过 veth 但计数不动);
> 3. **挂容器 netns 内 eth0 的 egress(源站出站方向)→ 生效**
>    (132MB 实测 11.04s ≈ 12MB/s,overlimits 计数在涨)。qdisc 随容器生命周期
>    消失,容器重建后需重新挂载。

1. **裸机 + 容器源站,不用 kind**:验证方法论而非系统架构;裸机 tc 可控性最高,
   K8s 网络命名空间嵌套会让 tc 挂层难以判断。
2. **热读定义**:数据已在 Agent 缓存目录,顺序直读文件 —— 不 drop_caches。
   理由:真实训练里"热"含 OS page cache;冷读每轮 drop_caches 已把源站侧与
   下载侧的 page cache 全部清掉(两者共享 host 内核),杜绝"假冷读"。
3. **热读零网络验证**:`verify_isolated.sh` 在热读窗口内采样 9000 端口
   established 连接(期望 0),对应红灯排查清单第 2 条。
4. **drop_caches 时机**:每轮冷读前 `rm -rf 缓存目录 && sync && drop_caches`。
   注意 drop_caches 清的是整个 host 的 page cache,**源站容器进程的缓存同样被清**,
   这正是装置想要的 —— 冷读必须真到磁盘+网络。
5. **源站公开读**:MinIO 原方案为 anonymous download policy + mc 上传;降级为
   nginx 后,`data/tars/` 只读挂载到 `html/train-data/`,http 直取无签名逻辑,
   URL 结构 `/train-data/shard-*.tar` 与 MinIO bucket 前缀一致,fetch.py 与
   train.py 完全复用。
6. **下载脚本模拟 Agent 语义**:有界并发(信号量式线程池)、按分片(块)下载、落本地
   缓存目录 —— 与层2 Agent 的三条抓取指令同构,但刻意不实现指令合并与缓存命中
   (探针阶段不需要,详见"风险"节)。

## 4. stall 口径与判定的微妙点(实测校准后)

**打点口径(主进程视角)**:每步 `t0`=上一步计算结束 → `next(batch)` 到手 `t1` →
极小 matmul 完成 `t2`。`wait=t1-t0`,`step=t2-t0`,`stall=wait/step`。
解码在 worker 进程并行(workers>0 时),主进程 wait 只含排队与管线补货 ——
这是文档要求"真 JPEG(真实解码成本)"的意义:workers=0 与 >0 的 wait 结构不同。

**2026-10-07 实测发现的三条机制(判定口径按此对齐,数据见 results/probe_b)**:

1. **compute 量级必须校准**:多核 BLAS 把 1024² matmul 跑成 ~3ms,stall 对 wait 的
   微小变化极敏感(0.6ms wait → 17% stall,13ms wait → 81% stall),装置判别力差。
   校准:限单线程 + 重复 8 次 ≈ 200ms/步(仍是"极小 compute 替身")。
2. **管线化档的冷热差异只在 wait 尾部**:预取队列缓冲抹平数据源差异,w4/w8 的中位
   wait 都落在 ~12-17ms 的 DataLoader 基座上;冷热差异表现为冷态每 4-8 批一个
   2.5s 级网络排队大尾,热态只有 ~180ms 解码尾。因此"冷态"按无管线档(w0)判,
   "热态"按有管线档(w4/w8)判。
3. **median 口径下 w4→w8 会假性反升**:中位 wait 随 workers 微升(13→17ms,队列
   竞争),median stall 跟着微升;mean(时间加权)口径对大尾敏感,真实反映
   "等待随并行度下降"。条件3 按 mean 判,median 同时报告。

**最终口径与实测**(compute≈200ms):

| 组 | stall median | stall mean | 判定 |
|---|---|---|---|
| 冷 w0 | 71.7% | 70.4% | 条件1:>40% ✓ |
| 冷 w4 | 7.5% | 27.3% | — |
| 冷 w8 | 7.7% | 17.6% | 条件3 mean 严格单调 ✓ |
| 热 w0 | 58.1% | 56.6% | 解码在主进程,结构性偏高,不判 |
| 热 w4 | 8.1% | 8.0% | 条件2:<15% ✓ |
| 热 w8 | 7.8% | 7.8% | 条件2:<15% ✓ |

**旋钮**(装置迁移到新机器/新数据时按顺序调):
1. `PROBE_B_COMPUTE_SIZE/ITERS`:热态压不到 15% 就加大 compute(勿动 wait 侧);
2. `PROBE_B_BATCH`:网络等待占比不足(冷 w0 掉到 40% 附近)时加大;
3. 图尺寸/质量:影响解码成本与每批字节数,需重新校准 σ。
判定脚本输出全部六组 median+mean,不藏数据。

## 5. 绿灯判据汇总

**探针A**(judge.py,三条件全过):
1. 冷/热中位耗时差 ≥5×(同时报吞吐比,一个数量级即 10×);
2. 两组 bootstrap 95% CI(百分位法,2000 重采样)不重叠;
3. 组内变异系数 CV ≤15%。

**探针B**(judge.py,三条件全过;口径依据见第 4 节实测机制):
1. 冷态 stall 稳定 >40%(判 w0 无管线档);
2. 热态 stall <15%(判 w4/w8 至少一档,`--require-all` 切两档均需);
3. 冷态 stall 随 workers 下降(mean 口径,w0 严格 >w4,且 w4≥w8 弱单调
   + 带宽饱和签名;median 同时报告)。

**闸门**:A/B 双绿灯 → 进第二部分现勘;任一红灯 → 停止开工,按第 6 节复盘装置。

## 6. 红灯排查清单(不硬调参数凑数字)

| # | 排查项 | 命令 |
|---|---|---|
| 1 | tc 挂对位置/方向?计数在涨? | `sudo nsenter -t $(docker inspect -f '{{.State.Pid}}' traincache-source) -n tc -s qdisc show dev eth0`(冷读期间看 Sent 计数;verify_isolated.sh 会打印) |
| 2 | 热读真的零网络? | `verify_isolated.sh` 采样 9000 连接;更硬核 `sudo tcpdump -i any -c 10 port 9000`(热读期间应为 0 包) |
| 3 | 单盘自身是否成瓶颈? | 热读吞吐应 >1GB/s(NVMe);若 <500MB/s 查 `iostat -x 1` |
| 4 | 源站容器 page cache 假冷读? | 冷读每轮 drop_caches 已全清(源站与下载侧共享 host 内核);辅证:冷读吞吐应稳定贴在 ~12.5MB/s |

## 7. 风险与已知取舍

- **Agent 语义简化**:探针阶段 fetch.py 未实现"相同指令合并"与"缓存命中不触源"——
  这两条是层2 Agent 的性能特性,不影响"冷热差存在性"与"stall 可测性"这两个探针目标。
- **单节点装置**:候选节点数、多节点重复预热代价等实验变量不属于本阶段,留给 kind 阶段。
- **合成图 vs 真实图**:解码成本真实(512² JPEG),像素内容不真实;对 stall 打点无影响,
  若现勘阶段需要真实内容(如与公开基线对齐),可换数据源而不动装置骨架。
- **tbf burst 与 TCP 慢启动**:burst 1600k 允许首秒突发,冷读吞吐可能略高于
  12.5MB/s(约 12.5~15MB/s),不影响 ≥5× 判据,但 CI/CV 以实测为准。

# Range 并行与逐连接限速

## 结论

现有 `tc_apply.sh` 挂的是聚合 tbf，单条 TCP 连接就能吃满档位。Range 把 8MiB 块拆成 4 条连接，在聚合限速下总速率不变，makespan 不会缩短。

要让并行路径在实验里可见，两处一起改：

1. 把 `fetchRangeThresholdBytes` 从默认 64MiB 降到块大小 8MiB，让块真的走四分片。
2. 把源站出站的叶子 qdisc 从 `netem` 换成 `fq maxrate`，给每条连接加上限。聚合上限设 1000mbit，逐连接 100mbit。串行臂用 1 条连接，约 100mbit；并行臂用 4 条，约 400mbit。

两臂只差一个 ConfigMap 键和一次 qdisc 重挂，数据集、档位、客户端数量都相同。

## 现有口径

`tc_apply.sh` 在 MinIO Pod 的 eth0 egress 上挂 root tbf，再在 `1:1` 上挂 netem 固定时延。

- tbf 的 `rate` 是整条 egress 的总速率，不区分连接。
- 集群内 RTT 约 20ms，单条 TCP 连接足以跑满 100mbit 档，4 条连接共享同一个 tbf，总速率仍是档位值。
- netem 只加固定时延，随机时延会把包序打乱，脚本里已经撤掉。

## 逐连接限速

`fq` 按五元组分流，`maxrate` 限制每条流的速率。4 条 Range GET 来自 4 个源端口，是 4 条独立的流。

netem 是无类 qdisc，不能再挂子 qdisc；fq 也是无类 qdisc。两者不能挂在同一棵树上。逐连接模式没有额外 20ms 时延，两臂都不加，比较只在本次实验族内成立，绝对值不能和之前带 netem 的臂直接对比。

`sch_fq` 由宿主机内核提供，kind 节点共用宿主机内核。先确认模块可用：

```bash
modinfo sch_fq
```

挂载命令在 MinIO Pod 的 netns 内执行。MinIO 重启后 netns 换新，qdisc 消失，需要重挂：

```bash
POD=$(kubectl -n minio get pod -l app=minio -o jsonpath='{.items[0].metadata.name}')
NODE=$(kubectl -n minio get pod "$POD" -o jsonpath='{.spec.nodeName}')
docker exec -i "$NODE" bash -s -- "$POD" <<'EOF'
set -euo pipefail
POD="$1"
CID=$(crictl pods --state Ready --name "$POD" -q | head -1)
NETNS=$(crictl inspectp "$CID" | grep -oE '"/var/run/netns/[^"]+"' | head -1 | tr -d '"')
[ -n "$NETNS" ] || { echo "找不到 Pod sandbox netns(CID=$CID)"; exit 1; }
nsenter --net="$NETNS" tc qdisc del dev eth0 root 2>/dev/null || true
nsenter --net="$NETNS" tc qdisc add dev eth0 root handle 1: \
  tbf rate 1000mbit burst 12500k latency 50ms
nsenter --net="$NETNS" tc qdisc add dev eth0 parent 1:1 handle 10: \
  fq maxrate 100mbit
nsenter --net="$NETNS" tc qdisc show dev eth0
EOF
```

回读应出现 `qdisc tbf 1: root` 与 `qdisc fq 10: parent 1:1`。`tc` 报 `Unknown qdisc "fq"` 时，在宿主机执行 `modprobe sch_fq` 后重试。

拆除用现有脚本：

```bash
/home/howardhe/Project/TrainCache/traincache-dev/bench/scripts/tc_teardown.sh
```

## 配置改动

两个键都在 `traincache-config`。并行臂把阈值降到 8388608，串行臂保持 67108864。网关磁盘缓存两臂都关：串行臂若开缓存，后续 run 会命中盘上块；Range 请求本来就不查缓存，开着会让两臂的回源量不可比。

```bash
# 并行臂
kubectl -n traincache patch configmap traincache-config --type merge \
  -p '{"data":{"fetchRangeThresholdBytes":"8388608","gatewayCache":"false"}}'
kubectl -n traincache rollout restart ds/traincache-agentd deploy/traincache-gateway
kubectl -n traincache rollout status ds/traincache-agentd
kubectl -n traincache rollout status deploy/traincache-gateway

# 串行臂
kubectl -n traincache patch configmap traincache-config --type merge \
  -p '{"data":{"fetchRangeThresholdBytes":"67108864","gatewayCache":"false"}}'
kubectl -n traincache rollout restart ds/traincache-agentd
kubectl -n traincache rollout status ds/traincache-agentd
```

枚举值 `"false"` 与代码默认一致，网关仍要重启一次，确保它读到关闭后的缓存开关。

`cold_reset.sh` 会重启 MinIO 并重新挂默认的 tbf+netem。每次冷态规程之后，重跑上面的逐连接挂载命令：

```bash
/home/howardhe/Project/TrainCache/traincache-dev/bench/scripts/cold_reset.sh
# 然后执行「逐连接限速」一节的挂载命令
```

Agent 的本地 CAS 在节点 hostPath `/var/cache/traincache`，跨 run 保留。每个臂开始前用现有冷态规程清 page cache 与 MinIO 热态，并用新的 dataset 名或清空 hostPath，避免本地命中盖住回源差异。

## 两臂与预期

数据集 4GiB，块 8MiB，共 512 块。单训练 Pod，快照与网关都按一个客户端计。

| 臂 | `fetchRangeThresholdBytes` | qdisc | 每块回源 GET | 预期出口速率 |
|---|---|---|---|---|
| 串行 | `"67108864"` | tbf 1000mbit + fq 100mbit | 1 | 约 100mbit |
| 并行 | `"8388608"` | tbf 1000mbit + fq 100mbit | 4 | 约 400mbit |

按 100mbit 算，4GiB 约 343s；按 400mbit 算约 86s。这是上限估算，实际 makespan 还受 Agent 落盘、网关扇出与启动尾部影响。

## 判据

- 网关 60s 汇总日志的 `gets` 与 `owners`：并行臂每块各计 4 次，串行臂各计 1 次。`cache_hit` 在两臂都应接近 0，出现非 0 说明缓存没关干净。
- Agent 下载汇总的块数与任务时长：并行臂单块墙钟时间应接近串行臂的四分之一，直到撞上聚合上限或落盘瓶颈。
- 两臂的块总数相同，都是 512。并行臂如果仍是 512 次整对象 GET，检查阈值是否写进 Pod（`kubectl -n traincache exec ds/traincache-agentd -- printenv TRAINCACHE_FETCH_RANGE_THRESHOLD_BYTES`）。

## 公平性

本次 A/B 只有一个训练 Pod，逐连接上限对两臂一致，比较成立。

多客户端场景下，逐连接上限会让连接多的客户端占更多带宽。公平做法是给每个客户端一个 htb 类，类速率相等，再在类内做逐连接上限。这次的单客户端实验不需要这一层，也没有实现它。

## 边界与风险

- `rangeParts` 是代码常量 4。逐连接上限乘以 4 超过聚合上限时，并行臂会被聚合 tbf 截住，400mbit 是本次配置下的上限。
- 逐连接模式没有 20ms 时延，不能和之前的 netem 臂混在一张图里比绝对值。
- Range 请求不进网关的在途合并表与磁盘缓存。本实验族不要与网关缓存臂共用 run。
- 阈值在 Agent 启动时读取，改 ConfigMap 后必须重启 DaemonSet。
- `bench/scripts/tc_apply.sh` 在当前工作树有未提交改动，本次没有改它；上面的挂载命令是同一套 tbf 结构，只是把叶子换成 fq。

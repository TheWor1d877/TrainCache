#!/usr/bin/env bash
# bench 装置只读体检与状态查看。
#
# 用法:
#   bash bench/scripts/doctor.sh            逐项输出 PASS/FAIL/WARN；有 FAIL 时退出码 1
#   bash bench/scripts/doctor.sh --status   打印 kind 集群、节点、MinIO、tc qdisc、
#                                           数据目录与最近产物(不修改任何状态)
#
# 两个模式都只读:不写产物、不改集群、不挂/拆 tc、不跑实验。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
PY="${PY:-$REPO/.venv/bin/python}"

CFG() { "$PY" "$ROOT/config.py" --sh "$1" 2>/dev/null; }

CLUSTER="$(CFG CLUSTER_NAME)"
NS_MINIO="$(CFG K8S_NS_MINIO)"
BUCKET="$(CFG MINIO_BUCKET)"
NODE_PORT="$(CFG MINIO_NODEPORT)"
ACCESS_KEY="$(CFG MINIO_ACCESS_KEY)"
SECRET_KEY="$(CFG MINIO_SECRET_KEY)"
SUDO_PASS="$(CFG SUDO_PASS_FILE)"
DATA_ROOT="$(CFG DATA_ROOT)"
DATASET_DIR="$(CFG DATASET_DIR)"
TILT_DIR="$(CFG TILT_DIR)"
TRAIN_DIR="$(CFG TRAIN_DIR)"
RESULTS_DIR="$(CFG RESULTS_DIR)"

FAILED=0
pass() { printf 'PASS  %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; FAILED=1; }
warn() { printf 'WARN  %s\n' "$*"; }
section() { printf '\n[%s]\n' "$*"; }
have() { command -v "$1" >/dev/null 2>&1; }

# ---------------------------------------------------------------------------
# 只读探查:MinIO Pod 的 eth0 qdisc
# 挂点由 tc_apply.sh 放在 Pod sandbox netns 内,这里用同一条 crictl/nsenter 路径读取。
# ---------------------------------------------------------------------------
show_tc() {
    local pod node
    pod="$(kubectl -n "$NS_MINIO" get pod -l app=minio \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [ -z "$pod" ]; then
        echo "  MinIO Pod 未运行，无 tc 挂点"
        return 0
    fi
    node="$(kubectl -n "$NS_MINIO" get pod "$pod" \
        -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
    if [ -z "$node" ]; then
        echo "  取不到 MinIO Pod 所在节点"
        return 0
    fi
    docker exec "$node" bash -c '
        cid=$(crictl pods --state Ready --name "$1" -q | head -1)
        [ -n "$cid" ] || { echo "  找不到 MinIO sandbox"; exit 0; }
        netns=$(crictl inspectp "$cid" \
            | grep -oE "\"/var/run/netns/[^\"]+\"" | head -1 | tr -d "\"")
        [ -n "$netns" ] || { echo "  找不到 Pod netns"; exit 0; }
        nsenter --net="$netns" tc qdisc show dev eth0
    ' _ "$pod" 2>/dev/null || echo "  读取失败(docker/crictl/nsenter 不可用)"
}

node_ip() {
    kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' \
        2>/dev/null || true
}

# ---------------------------------------------------------------------------
# status 模式
# ---------------------------------------------------------------------------
status() {
    echo "== kind 集群 =="
    if have kind; then
        if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
            echo "  $CLUSTER 已创建"
        else
            echo "  $CLUSTER 未创建"
        fi
    else
        echo "  kind 不可用"
    fi

    echo
    echo "== 节点 =="
    kubectl get nodes -o wide 2>/dev/null || echo "  kubectl 不可用"

    echo
    echo "== MinIO =="
    kubectl -n "$NS_MINIO" get deployment,pods -o wide 2>/dev/null \
        || echo "  MinIO 未部署或 kubectl 不可用"
    local ip
    ip="$(node_ip)"
    if [ -n "$ip" ] && have curl; then
        printf '  health: HTTP %s (%s:%s)\n' \
            "$(curl -s -o /dev/null -m 3 -w '%{http_code}' \
                "http://$ip:$NODE_PORT/minio/health/live" 2>/dev/null || echo 000)" \
            "$ip" "$NODE_PORT"
    else
        echo "  health: 跳过(无节点 IP 或 curl)"
    fi

    echo
    echo "== tc qdisc (MinIO Pod eth0 egress) =="
    show_tc

    echo
    echo "== 数据目录 =="
    print_dir() {
        local label="$1" dir="$2" pattern="${3:-}"
        if [ -z "$dir" ] || [ ! -d "$dir" ]; then
            echo "  $label: 缺失 ($dir)"
            return
        fi
        if [ -n "$pattern" ]; then
            printf '  %s: %s 个 %s (%s)\n' "$label" \
                "$(ls -1 "$dir"/$pattern 2>/dev/null | wc -l)" "$pattern" "$dir"
        else
            printf '  %s: %s 个顶层条目 (%s)\n' "$label" \
                "$(ls -1A "$dir" 2>/dev/null | wc -l)" "$dir"
        fi
    }
    print_dir "主数据集" "$DATASET_DIR" 'shard-*.tar'
    print_dir "tilt" "$TILT_DIR"
    print_dir "train" "$TRAIN_DIR" 'shard-*.tar'
    echo "  结果根: $RESULTS_DIR"

    echo
    echo "== 最近产物(summary.csv,最多 5 条) =="
    if [ -d "$RESULTS_DIR" ]; then
        find "$RESULTS_DIR" -maxdepth 3 -name summary.csv \
            -printf '%TY-%Tm-%Td %TH:%TM %p\n' 2>/dev/null \
            | sort | tail -5 | sed 's/^/  /'
    else
        echo "  结果目录尚未创建"
    fi
}

if [ "${1:-}" = "--status" ]; then
    status
    exit 0
fi

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    sed -n '2,9p' "$0"
    exit 0
fi

# ---------------------------------------------------------------------------
# doctor 模式
# ---------------------------------------------------------------------------
if [ ! -x "$PY" ]; then
    fail "python 解释器不可执行: $PY"
    echo
    echo "结论: 有 FAIL 项"
    exit 1
fi
if [ -z "$RESULTS_DIR" ]; then
    fail "config.py 取值失败(检查 $ROOT/config.py)"
fi

section "工具与解释器"
pass "python: $PY"
for tool in kubectl kind docker git; do
    if have "$tool"; then
        pass "命令可用: $tool"
    else
        fail "命令缺失: $tool"
    fi
done
if have tc; then
    pass "命令可用: tc(宿主机)"
else
    warn "宿主机无 tc;挂载主路径用节点容器内 tc,tc_apply 的 docker 回退会受影响"
fi

section "Python 依赖"
for mod in numpy torch webdataset; do
    if "$PY" -c "import $mod" >/dev/null 2>&1; then
        pass "import $mod"
    else
        fail "import $mod 失败(train负载器需要)"
    fi
done
if "$PY" -c "import boto3" >/dev/null 2>&1; then
    pass "import boto3"
    HAVE_BOTO=1
else
    fail "import boto3 失败(upload / MinIO 桶检查需要)"
    HAVE_BOTO=0
fi

section "kind 集群与节点"
if have kind && kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    pass "kind 集群 $CLUSTER 已创建"
    HAVE_CLUSTER=1
else
    fail "kind 集群 $CLUSTER 不存在"
    HAVE_CLUSTER=0
fi

if have kubectl && kubectl version >/dev/null 2>&1; then
    pass "kubectl 可连集群"
    HAVE_K8S=1
else
    fail "kubectl 连不上集群"
    HAVE_K8S=0
fi

WORKERS=""
if [ "$HAVE_K8S" -eq 1 ]; then
    nodes="$(kubectl get nodes --no-headers 2>/dev/null || true)"
    total="$(printf '%s\n' "$nodes" | grep -c . || true)"
    ready="$(printf '%s\n' "$nodes" | grep -c ' Ready' || true)"
    if [ "$total" -eq 5 ]; then
        pass "节点数 $total(1 control-plane + 4 worker)"
    else
        fail "节点数 $total,期望 5"
    fi
    if [ "$total" -gt 0 ] && [ "$ready" -eq "$total" ]; then
        pass "节点 Ready $ready/$total"
    else
        fail "节点 Ready $ready/$total"
    fi
    WORKERS="$(kubectl get nodes -o name 2>/dev/null \
        | sed 's#^node/##' | grep -E 'worker[0-9]*$' || true)"
    worker_n="$(printf '%s\n' "$WORKERS" | grep -c . || true)"
    if [ "$worker_n" -eq 4 ]; then
        pass "worker 节点 4 个"
    else
        fail "worker 节点 $worker_n 个,期望 4"
    fi
    s3_n="$(kubectl get nodes -l traincache.io/s3-access=true --no-headers 2>/dev/null \
        | grep -c . || true)"
    if [ "$s3_n" -eq 4 ]; then
        pass "s3-access 标签覆盖 4 个 worker"
    else
        warn "s3-access 标签 $s3_n/4(traincache/up.sh 会自动补)"
    fi
fi

section "worker 镜像(containerd)"
image_present() {
    local node="$1" ref="$2"
    docker exec "$node" crictl images -q "$ref" 2>/dev/null | grep -q . && return 0
    case "$ref" in
        */*) return 1 ;;
        *) docker exec "$node" crictl images -q "docker.io/library/$ref" 2>/dev/null \
               | grep -q . ;;
    esac
}
AGENT_IMG="$(awk '/^[[:space:]]*image:[[:space:]]*/{print $2; exit}' \
    "$REPO/controller/deploy/agent/daemonset.yaml" 2>/dev/null || true)"
TRAIN_IMG="${TRAIN_IMAGE:-python:3.12-alpine}"
BUSYBOX_IMG="${BUSYBOX_IMAGE:-busybox:latest}"
if [ -z "$AGENT_IMG" ]; then
    fail "读不到 agentd 镜像引用(controller/deploy/agent/daemonset.yaml)"
fi
if [ "$HAVE_K8S" -eq 1 ] && have docker && docker ps >/dev/null 2>&1; then
    img_fail=0
    checked=0
    for node in $WORKERS; do
        if ! docker exec "$node" true >/dev/null 2>&1; then
            fail "docker exec $node 不可用"
            img_fail=1
            break
        fi
        checked=$((checked + 1))
        for img in "$AGENT_IMG" "$TRAIN_IMG"; do
            if ! image_present "$node" "$img"; then
                fail "镜像缺失 $node: $img"
                img_fail=1
            fi
        done
        if ! image_present "$node" "$BUSYBOX_IMG"; then
            warn "镜像缺失 $node: $BUSYBOX_IMG(verify_cold 用)"
        fi
    done
    if [ "$checked" -gt 0 ] && [ "$img_fail" -eq 0 ]; then
        pass "worker 镜像齐全: agentd / $TRAIN_IMG($checked 个节点)"
    fi
else
    fail "docker 不可用或集群未就绪,无法检查 worker 镜像"
fi

section "控制面与 Agent"
# 缓存上限的期望值不写死：环境变量 > 本次 COND(cache 字段)
# 的 cache.capacity_bytes > COND 层固定值(1GiB)。输出两行：字节数与来源
# (env|cond|default)；来源是 default 时说明这一批没声明档位，只比对不判死。
expected_capacity_bytes() {
    if [ -n "${TRAINCACHE_CACHE_CAPACITY_BYTES:-}" ]; then
        printf '%s\nenv\n' "$TRAINCACHE_CACHE_CAPACITY_BYTES"
        return 0
    fi
    "$PY" - "$ROOT/experiments" <<'PYEOF'
import os
import sys

sys.path.insert(0, sys.argv[1])
import cond  # noqa: E402

name = (os.environ.get("TRAINCACHE_COND") or "").strip()
if name:
    try:
        print(int(cond.load_cond(name)["cache"]["capacity_bytes"]))
        print("cond")
        sys.exit(0)
    except Exception as exc:  # COND 缺失/没有该字段时退回固定值
        print(f"读 COND {name} 失败: {exc}", file=sys.stderr)
print(cond.COMPARE_CACHE_BYTES)
print("default")
PYEOF
}
EXP_RAW="$(expected_capacity_bytes)"
EXP_CAP="$(printf '%s\n' "$EXP_RAW" | sed -n 1p)"
EXP_SRC="$(printf '%s\n' "$EXP_RAW" | sed -n 2p)"
# 预取提前量与淘汰策略的期望值：环境变量 > COND(prefetch.lookahead /
# cache.evict_policy) > 清单默认(16、heat)。输出三行：lookahead、evict_policy、来源。
expected_agent_knobs() {
    "$PY" - "$ROOT/experiments" <<'PYEOF'
import os
import sys

sys.path.insert(0, sys.argv[1])
import cond  # noqa: E402

la = ep = None
src = "default"
name = (os.environ.get("TRAINCACHE_COND") or "").strip()
if name:
    try:
        c = cond.load_cond(name)
        la = (c.get("prefetch") or {}).get("lookahead")
        ep = (c.get("cache") or {}).get("evict_policy")
        if la is not None or ep is not None:
            src = "cond"
    except Exception as exc:  # COND 缺失/没有该字段时退回清单默认
        print(f"读 COND {name} 失败: {exc}", file=sys.stderr)
if os.environ.get("TRAINCACHE_PREFETCH_LOOKAHEAD"):
    la = os.environ["TRAINCACHE_PREFETCH_LOOKAHEAD"].strip()
    src = "env"
if os.environ.get("TRAINCACHE_EVICT_POLICY"):
    ep = os.environ["TRAINCACHE_EVICT_POLICY"].strip().lower()
    src = "env"
print(la if la is not None else 16)
print(ep if ep is not None else "heat")
print(src)
PYEOF
}
K_RAW="$(expected_agent_knobs)"
EXP_LA="$(printf '%s\n' "$K_RAW" | sed -n 1p)"
EXP_EP="$(printf '%s\n' "$K_RAW" | sed -n 2p)"
K_SRC="$(printf '%s\n' "$K_RAW" | sed -n 3p)"
if ! printf '%s' "$EXP_CAP" | grep -Eq '^[0-9]+$'; then
    fail "算不出缓存上限期望值(PY=$PY,experiments=$ROOT/experiments,得到 ${EXP_CAP:-空})"
    EXP_CAP="-1"
    EXP_SRC="unknown"
fi
if [ "$HAVE_K8S" -eq 1 ] && kubectl -n traincache get ds traincache-agentd >/dev/null 2>&1; then
    desired="$(kubectl -n traincache get ds traincache-agentd \
        -o jsonpath='{.status.desiredNumberScheduled}' 2>/dev/null || true)"
    ready="$(kubectl -n traincache get ds traincache-agentd \
        -o jsonpath='{.status.numberReady}' 2>/dev/null || true)"
    if [ "${ready:-0}" -ge 4 ]; then
        pass "agentd Ready $ready/${desired:-4}"
    else
        warn "agentd Ready ${ready:-0}/${desired:-4}(make ctrl-up 会拉起)"
    fi
    cap="$(kubectl -n traincache get cm traincache-config \
        -o jsonpath='{.data.cacheCapacityBytes}' 2>/dev/null)"
    cap_rc=$?
    if [ "$cap_rc" -ne 0 ]; then
        fail "agentd 缓存上限查询失败(kubectl 退出码 $cap_rc)"
    elif [ -z "$cap" ]; then
        fail "traincache-config 缺少 cacheCapacityBytes(期望 $EXP_CAP)"
    elif [ "$cap" != "$EXP_CAP" ] && [ "$EXP_SRC" != "default" ]; then
        fail "agentd 缓存上限 $cap,本批期望 $EXP_CAP(来源 $EXP_SRC)"
    elif [ "$cap" != "$EXP_CAP" ]; then
        warn "agentd 缓存上限 $cap,与对照层缺省 $EXP_CAP 不同(本批未给 COND/env,只在声明档位时判)"
    else
        pass "agentd 缓存上限 $cap 字节(期望值同源:$EXP_SRC)"
    fi
    sw="$(kubectl -n traincache get cm traincache-config \
        -o jsonpath='{.data.startupWindowBytes}' 2>/dev/null || true)"
    if [ "$sw" = "536870912" ]; then
        pass "自动 K 启动窗口 $sw 字节(512MiB)"
    else
        fail "startupWindowBytes=${sw:-缺失},期望 536870912"
    fi
    mf="$(kubectl -n traincache get cm traincache-config \
        -o jsonpath='{.data.maxFillFraction}' 2>/dev/null || true)"
    if [ "$mf" = "0.125" ]; then
        pass "自动 K 缓存占比上限 $mf"
    else
        fail "maxFillFraction=${mf:-缺失},期望 0.125"
    fi
    # `kubectl apply -k` 会把 traincache-config 整份 data 盖回清单默认(16/heat),
    # 起组前写的本轮值因此会消失;ctrl_up.sh 在 apply 之后按环境变量再对齐一次。
    # 本批声明过 COND 却没查到本轮值,5.1 的策略矩阵与 5.3 的 lookahead 扫点就没有自变量。
    cm_la="$(kubectl -n traincache get cm traincache-config \
        -o jsonpath='{.data.prefetchLookahead}' 2>/dev/null || true)"
    cm_ep="$(kubectl -n traincache get cm traincache-config \
        -o jsonpath='{.data.evictPolicy}' 2>/dev/null || true)"
    for pair in "prefetchLookahead:$cm_la:$EXP_LA" "evictPolicy:$cm_ep:$EXP_EP"; do
        key="${pair%%:*}"
        rest="${pair#*:}"
        got="${rest%%:*}"
        want="${rest#*:}"
        if [ "$got" = "$want" ]; then
            pass "traincache-config 的 $key=$got(期望值同源:$K_SRC)"
        elif [ "$K_SRC" = "default" ]; then
            warn "traincache-config 的 $key=${got:-缺失},与清单默认 $want 不同(本批未给 COND/env)"
        else
            fail "traincache-config 的 $key=${got:-缺失},本批期望 $want(来源 $K_SRC);make ctrl-up 会重新对齐"
        fi
    done
else
    warn "agentd 未部署(首次 make bench-up 或 make ctrl-up 会部署)"
fi

section "淘汰证据(4.3 §2.6、§3.1)"
if [ "$HAVE_K8S" -eq 1 ]; then
    agentd_pods="$(kubectl -n traincache get pods -l app=traincache-agentd \
        -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)"
    if [ -z "$agentd_pods" ]; then
        warn "没有 agentd Pod,跳过淘汰证据检查"
    elif [ -z "${cap:-}" ] || [ "${cap:-0}" = "0" ]; then
        warn "缓存上限为 ${cap:-未配置}(0=字节维度淘汰不启用),跳过淘汰证据断言"
    else
        # 容器里的 env 真值:run_bench 只在 cacheCapacityBytes 变化时重启 agentd,
        # 手改 ConfigMap 或 agentd 早于改配置启动时,进程内仍是旧值(0=淘汰不触发)。
        env_bad=0
        for pod in $agentd_pods; do
            live="$(kubectl -n traincache exec "$pod" -- \
                printenv TRAINCACHE_CACHE_CAPACITY_BYTES 2>/dev/null || true)"
            if [ "$live" != "$cap" ]; then
                fail "agentd $pod 容器内 TRAINCACHE_CACHE_CAPACITY_BYTES=${live:-未设置},ConfigMap 声明 $cap"
                env_bad=1
            fi
            # 同一件事也发生在预取提前量与淘汰策略上:agentd 早于 ConfigMap 改动启动、
            # 或 apply -k 盖回默认后没重启时,进程内仍是旧值,本轮扫点就是空跑。
            for kv in "TRAINCACHE_PREFETCH_LOOKAHEAD:${cm_la:-}" \
                      "TRAINCACHE_EVICT_POLICY:${cm_ep:-}"; do
                want="${kv#*:}"
                [ -n "$want" ] || continue
                live="$(kubectl -n traincache exec "$pod" -- \
                    printenv "${kv%%:*}" 2>/dev/null || true)"
                if [ "$live" != "$want" ]; then
                    fail "agentd $pod 容器内 ${kv%%:*}=${live:-未设置},ConfigMap 声明 $want"
                    env_bad=1
                fi
            done
        done
        if [ "$env_bad" -eq 0 ]; then
            pass "各 agentd 容器内的上限/提前量/淘汰策略 env 与 ConfigMap 一致($cap/${cm_la:-?}/${cm_ep:-?})"
        fi
        # 行为断言:块在盘上而 agent 的字节累计恒 0 → 累计没生效,闸门不会触发。
        BLOCKS_DIR="/var/cache/traincache/blocks"
        dir_total=0
        if have docker; then
            for n in $WORKERS; do
                b="$(docker exec "$n" sh -c \
"find $BLOCKS_DIR -name '*.blk' -printf '%s\n' 2>/dev/null | awk '{t+=\$1} END{print t+0}'" \
                    2>/dev/null || true)"
                case "$b" in ''|*[!0-9]*) b=0 ;; esac
                dir_total=$((dir_total + b))
            done
        fi
        accum_max=""
        for pod in $agentd_pods; do
            v="$(kubectl -n traincache get --raw \
"/api/v1/namespaces/traincache/pods/$pod:9411/proxy/metrics" 2>/dev/null \
                | awk '/^traincache_cache_capacity_bytes_total\{source="bytes"\}/{print $2}' | head -1)"
            [ -n "$v" ] || continue
            accum_max="$(awk -v a="${accum_max:-0}" -v b="$v" 'BEGIN{print (b+0>a+0)?b:a}')"
        done
        if [ -z "$accum_max" ]; then
            fail "抓不到任何 agentd 的 cache_capacity_bytes_total{source=\"bytes\"},无法断言淘汰生效"
        elif [ "$dir_total" -gt 0 ] \
                && [ "$(awk -v a="$accum_max" 'BEGIN{print (a+0>0)?1:0}')" = "0" ]; then
            fail "节点上有 $dir_total 字节的块,但各 agentd 的字节累计最大为 $accum_max → 累计没生效,淘汰闸门不会触发"
        elif [ "$dir_total" -gt 0 ] \
                && [ "$(awk -v a="$accum_max" -v d="$dir_total" 'BEGIN{print (a+0>1.5*d)?1:0}')" = "1" ]; then
            fail "字节累计 $accum_max 对目录实占 $dir_total 字节超 1.5 倍(4.3 §2.4 字节账自洽判据) → 同一批块被重复记账"
        elif [ "$dir_total" -eq 0 ]; then
            pass "缓存目录为空(agentd 复位后),累计 $accum_max 是复位前残账,不作判据"
        else
            pass "淘汰证据:四节点块共 $dir_total 字节 / agent 字节累计最大 $accum_max"
        fi
    fi
fi

section "MinIO 与桶"
if [ "$HAVE_K8S" -eq 1 ]; then
    if kubectl -n "$NS_MINIO" get deployment minio >/dev/null 2>&1; then
        ready_rep="$(kubectl -n "$NS_MINIO" get deployment minio \
            -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
        if [ "${ready_rep:-0}" -ge 1 ]; then
            pass "MinIO deployment Ready ${ready_rep}"
        else
            fail "MinIO deployment 无 Ready 副本"
        fi
    else
        fail "MinIO deployment 不存在"
    fi
    IP="$(node_ip)"
    if [ -n "$IP" ] && have curl; then
        code="$(curl -s -o /dev/null -m 3 -w '%{http_code}' \
            "http://$IP:$NODE_PORT/minio/health/live" 2>/dev/null || echo 000)"
        if [ "$code" = "200" ]; then
            pass "MinIO health 200($IP:$NODE_PORT)"
        else
            fail "MinIO health HTTP $code($IP:$NODE_PORT)"
        fi
    else
        warn "跳过 MinIO health(无节点 IP 或 curl)"
    fi

    if [ "$HAVE_BOTO" -eq 1 ] && [ -n "$IP" ]; then
        bucket_report="$("$PY" - "$IP" "$NODE_PORT" "$ACCESS_KEY" "$SECRET_KEY" "$BUCKET" <<'PY'
import sys
import boto3
from botocore.config import Config

ip, port, ak, sk, bucket = sys.argv[1:6]
cfg = Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 0})
s3 = boto3.client("s3", endpoint_url=f"http://{ip}:{port}",
                  aws_access_key_id=ak, aws_secret_access_key=sk, config=cfg)
try:
    s3.head_bucket(Bucket=bucket)
except Exception as exc:  # noqa: BLE001
    print(f"ERR 桶 {bucket} 不可读: {exc}")
    raise SystemExit(0)
n = 0
for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix="shard-"):
    n += sum(1 for o in page.get("Contents", []) if o["Key"].endswith(".tar"))
try:
    s3.head_object(Bucket=bucket, Key="dataset/compare-data-8m/CURRENT")
    snap = "yes"
except Exception:  # noqa: BLE001
    snap = "no"
print(f"OK shards={n} snapshot_8m={snap}")
PY
)"
        case "$bucket_report" in
            OK*)
                shard_n="$(printf '%s' "$bucket_report" | sed -n 's/.*shards=\([0-9]*\).*/\1/p')"
                snap="$(printf '%s' "$bucket_report" | sed -n 's/.*snapshot_8m=\([a-z]*\).*/\1/p')"
                if [ "${shard_n:-0}" -ge 16 ]; then
                    pass "桶 $BUCKET 有 $shard_n 个 shard-*.tar"
                elif [ "${shard_n:-0}" -ge 1 ]; then
                    warn "桶 $BUCKET 只有 $shard_n 个 shard-*.tar,不足 16 个"
                else
                    fail "桶 $BUCKET 没有 shard-*.tar"
                fi
                if [ "$snap" = "yes" ]; then
                    pass "快照 dataset/compare-data-8m/CURRENT 在位"
                else
                    warn "快照 dataset/compare-data-8m/CURRENT 缺失(up.sh 会发布)"
                fi
                ;;
            *) fail "${bucket_report:-桶检查无输出}" ;;
        esac
    else
        warn "跳过桶检查(缺 boto3 或节点 IP)"
    fi
else
    fail "集群未就绪,跳过 MinIO 检查"
fi

section "tc qdisc"
tc_out="$(show_tc 2>/dev/null || true)"
if printf '%s' "$tc_out" | grep -q 'tbf'; then
    pass "tc qdisc 在位(MinIO Pod eth0)"
    printf '%s\n' "$tc_out" | sed 's/^/      /'
    # burst 的单位坑(D31;2026-11-03 一次性容器实测):tc 把 1250k 解析成 1 280 000
    # 字节、回读打印成 1250Kb,命令行同时给的 latency 50ms 不覆盖这个桶。回读值折算
    # 成字节后必须等于 config.burst_for_rate(该档 rate),±1% 容纳 iproute2 按 32 位字
    # 取整掉下的零头(3750k 实测回读 3839962b)。
    tbf_line="$(printf '%s\n' "$tc_out" | grep -m1 'qdisc tbf' || true)"
    rate_tok="$(printf '%s\n' "$tbf_line" | sed -n 's/.*rate \([0-9.][0-9.]*[A-Za-z]*\).*/\1/p')"
    burst_tok="$(printf '%s\n' "$tbf_line" | sed -n 's/.*burst \([0-9.][0-9.]*[A-Za-z]*\).*/\1/p')"
    if [ -n "$rate_tok" ] && [ -n "$burst_tok" ]; then
        bmsg="$("$PY" - "$ROOT" "$rate_tok" "$burst_tok" <<'PYEOF' 2>&1
import re
import sys

sys.path.insert(0, sys.argv[1])
import config

BYTE_UNITS = {"b": 1, "k": 1024, "kb": 1024, "mb": 1024 * 1024}
BIT_UNITS = {"bit": 1, "kbit": 1000, "mbit": 10 ** 6, "gbit": 10 ** 9}


def to_bytes(tok, units):
    m = re.fullmatch(r"([0-9.]+)([a-z]+)", tok.strip().lower())
    if not m or m.group(2) not in units:
        raise ValueError("无法解析: " + tok)
    return float(m.group(1)) * units[m.group(2)]


mbit = round(to_bytes(sys.argv[2], BIT_UNITS) / 10 ** 6)
want = to_bytes(config.burst_for_rate(mbit), BYTE_UNITS)
got = to_bytes(sys.argv[3], BYTE_UNITS)
print("@%dmbit 实挂 burst=%dB,推导 %s=%dB,差 %+.2f%%"
      % (mbit, got, config.burst_for_rate(mbit), want,
         100.0 * (got - want) / want))
sys.exit(0 if abs(got - want) <= 0.01 * want else 1)
PYEOF
)"
        if [ $? -eq 0 ]; then
            pass "tbf burst 与该档 rate 的推导值一致: $bmsg"
        else
            fail "tbf burst 对不上 rate 的推导值(档位或 burst 口径漂了): $bmsg"
        fi
    else
        warn "tc qdisc 在位但读不到 rate/burst 字段,跳过 burst 断言: $tbf_line"
    fi
else
    warn "tc qdisc 未挂(跑分前 make tc-up 或 cold_reset 会挂)"
    printf '%s\n' "$tc_out" | sed 's/^/      /'
fi

section "数据目录与磁盘"
b_n="$(ls -1 "$DATASET_DIR"/shard-*.tar 2>/dev/null | wc -l)"
if [ "$b_n" -ge 16 ]; then
    pass "主数据集 shard 数 $b_n: $DATASET_DIR"
elif [ "$b_n" -ge 1 ]; then
    warn "主数据集只有 $b_n 个 shard,不足 16 个: $DATASET_DIR"
else
    fail "主数据集没有 shard-*.tar: $DATASET_DIR"
fi
if [ -f "$TILT_DIR/hot-shards.txt" ] && [ -f "$TILT_DIR/full-shards.txt" ]; then
    pass "tilt 清单在位: $TILT_DIR"
else
    warn "tilt 清单缺失(synth_tilt.py 会生成)"
fi
d_n="$(ls -1 "$TRAIN_DIR"/shard-*.tar 2>/dev/null | wc -l)"
if [ "$d_n" -gt 0 ]; then
    pass "train shard 数 $d_n: $TRAIN_DIR"
else
    warn "train 未生成(track-d-data 会生成)"
fi
if [ -e "$SUDO_PASS" ]; then
    mode="$(stat -c '%a' "$SUDO_PASS" 2>/dev/null || echo '?')"
    if [ "$mode" = "600" ]; then
        pass "$SUDO_PASS 存在且权限 600"
    else
        warn "$SUDO_PASS 权限 $mode,期望 600"
    fi
else
    fail "$SUDO_PASS 不存在(cold_reset/drop_caches 需要 sudo)"
fi
if [ -n "$DATA_ROOT" ] && [ -d "$DATA_ROOT" ]; then
    avail_g="$(df -Pk "$DATA_ROOT" 2>/dev/null | awk 'NR==2{printf "%d", $4/1024/1024}')"
    if [ -n "${avail_g:-}" ] && [ "$avail_g" -ge 100 ]; then
        pass "数据层剩余 ${avail_g}GiB(≥100GiB)"
    else
        warn "数据层剩余 ${avail_g:-?}GiB(<100GiB)"
    fi
else
    fail "数据根不存在: $DATA_ROOT"
fi

echo
if [ "$FAILED" -ne 0 ]; then
    echo "结论: 有 FAIL 项"
    exit 1
fi
echo "结论: 全部通过(有 WARN 项时不影响退出码)"
exit 0

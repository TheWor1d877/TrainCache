#!/usr/bin/env bash
# 冷态规程:每组实验开始前执行,保证每次冷读一样冷。
# 1) sync + drop_caches 清宿主机内核 page cache(kind 节点容器与宿主机共享内核,
#    节点内所有进程的页缓存一并清掉);
# 2) 重启 MinIO(清其进程内热态);
# 3) 重挂 tc(MinIO 重建后 Pod 换新 netns,qdisc 随之消失)。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG() { python3 "$ROOT/config.py" --sh "$1"; }
CLUSTER_NAME="${CLUSTER_NAME:-$(CFG CLUSTER_NAME)}"
CLUSTER_NAME="${CLUSTER_NAME:-traincache}"

# 清页缓存的通道,按可用性取第一条:root 直跑 > sudo -n > kind 节点容器 >
# sudo -S 配 .sudo-pass。kind 节点是特权容器、与宿主机共用内核,容器内写的
# /proc/sys/vm/drop_caches 清的是宿主机同一份页缓存,所以常规装置不需要密码。
# 返回 0 时把通道名打到 stdout,失败返回 1。
drop_caches() {
    local node
    if [ "$(id -u)" = "0" ]; then
        sync
        echo 3 >/proc/sys/vm/drop_caches
        echo "root 直跑"
        return 0
    fi
    if sudo -n true 2>/dev/null; then
        sudo -n sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
        echo "sudo -n"
        return 0
    fi
    if command -v kind >/dev/null 2>&1; then
        for node in $(kind get nodes --name "$CLUSTER_NAME" 2>/dev/null); do
            if docker exec "$node" sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null; then
                echo "kind 节点容器 $node"
                return 0
            fi
        done
    fi
    if [ -r "$ROOT/.sudo-pass" ]; then
        if printf '%s\n' "$(cat "$ROOT/.sudo-pass")" \
            | sudo -S -p '' sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null; then
            echo "sudo -S(.sudo-pass)"
            return 0
        fi
    fi
    return 1
}

echo "== 1/3 drop_caches(宿主机内核,覆盖全部 kind 节点)=="
if ! via="$(drop_caches)"; then
    echo "    错误: 没有可用的清页缓存通道" >&2
    echo "    root 直跑 / sudo -n / kind 节点容器 / sudo -S 配 bench/.sudo-pass 至少要有一条通" >&2
    exit 1
fi
echo "    page cache 已清(通道:$via)"

echo "== 2/3 重启 MinIO =="
kubectl -n minio rollout restart deployment/minio
kubectl -n minio rollout status deployment/minio --timeout=180s
sleep 5
echo "== 3/3 重新挂 tc(MinIO 重启后 Pod 换新 netns,qdisc 随之消失)=="
bash "$ROOT/scripts/tc_apply.sh" 2>&1 | tail -3
echo "冷态就绪(MinIO 已重启、内核缓存已清、tc 已重挂)"

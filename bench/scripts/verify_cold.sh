#!/usr/bin/env bash
# 过关标准验证:两次冷读结果对得上。
# 冷读 = 集群内 busybox Pod 经 MinIO Service 全量下载 B 轨 tar(受 tc 限速),
# 每轮前执行冷态规程;两次冷读相对差 ≤10% 判"对上"。
# 用法:verify_cold.sh [--quick](quick 只读 1 个 tar,秒级;默认全量 8GB,约 11 分钟/轮)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG() { python3 "$ROOT/config.py" --sh "$1"; }
QUICK="${1:-}"
BUCKET="$(CFG MINIO_BUCKET)"
OUT="$ROOT/results/cold-verify.txt"
mkdir -p "$ROOT/results"

cold_read() {
  local shards pod
  if [ "$QUICK" = "--quick" ]; then
    shards="shard-00000.tar"
  else
    shards="$(seq -f 'shard-%05g.tar' 0 $(( $(CFG TRACK_B_N_TARS) - 1 )) | tr '\n' ' ')"
  fi
  pod="cold-reader-$(date +%s)"
  # 冷读 8GB 需 ~11 分钟,远超 kubectl run --rm 的默认 60s 超时,
  # 改为:创建 → 轮询 Succeeded → 取日志 → 删除
  # busybox 的 date 不支持 %N(毫秒),计时用秒级;冷读 8GB≈640s,秒级精度 0.2% 足够
  kubectl run "$pod" --image="$(CFG COLD_READ_IMAGE)" --restart=Never \
    --image-pull-policy=IfNotPresent \
    --command -- sh -c \
    "t0=\$(date +%s); for f in $shards; do wget -q -O /dev/null http://minio.minio.svc.cluster.local:9000/$BUCKET/\$f; done; t1=\$(date +%s); echo \"cold_s=\$((t1-t0))\""
  for _ in $(seq 1 150); do
    phase=$(kubectl get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || echo Gone)
    [ "$phase" = "Succeeded" ] && break
    [ "$phase" = "Failed" ] && { kubectl logs "$pod" 2>&1 | tail -3; break; }
    sleep 10
  done
  kubectl logs "$pod" 2>/dev/null | grep cold_s
  kubectl delete pod "$pod" --ignore-not-found >/dev/null
}

echo "冷读验证(两轮,每轮前冷态规程)$([ "$QUICK" = "--quick" ] && echo ' [quick 模式]')" | tee "$OUT"
for i in 1 2; do
  echo "== 第 $i 轮:冷态规程 ==" | tee -a "$OUT"
  bash "$ROOT/scripts/cold_reset.sh" | tail -2 | tee -a "$OUT"
  echo "== 第 $i 轮:冷读 ==" | tee -a "$OUT"
  cold_read | tee -a "$OUT"
done

python3 - "$OUT" <<'EOF'
import re, sys
path = sys.argv[1]
vals = [int(m) for m in re.findall(r'cold_s=(\d+)', open(path).read())]
if len(vals) < 2:
    print("冷读次数不足,验证失败"); sys.exit(1)
diff = abs(vals[0] - vals[1]) / max(vals[0], 1)
print(f"两轮冷读: {vals[0]} s vs {vals[1]} s,相对差 {diff:.1%}")
print("过关:两次冷读结果对得上" if diff <= 0.10 else "不过关:冷读不可复现,查装置")
sys.exit(0 if diff <= 0.10 else 1)
EOF

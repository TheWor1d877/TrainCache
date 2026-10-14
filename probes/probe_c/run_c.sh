#!/usr/bin/env bash
# 探针C 四组:冷 w4 → 冷 w0 → 冷 w8 → 热 w4,每组连续 3 epoch 全程打点。
#
# 测量纪律(重要,勿改):
#   * epoch 之间绝对不做任何清缓存 / 容器重启 / drop_caches —— epoch 切换后
#     stall 是否回升正是本探针的测量对象,中间清缓存会污染测量;
#   * 仅"每组开跑前"做一次组前冷态(清 Agent 缓存目录 + drop_caches);
#   * 组间只 sleep 5;
#   * 开跑计时测量前做 kind 节点负载检查(与 kind 上其他子代理互斥)。
#
# sudo 说明:本机 sudo 无法非交互取证(无 NOPASSWD、无 tty)。脚本优先尝试
# `sudo -n`,不可用时回退为特权/带 NET_ADMIN 的 docker 容器(在源站 netns 内
# 或宿主机内核上)执行完全相同的操作:tc qdisc、drop_caches、rm 缓存目录。
# 回退路径与结果记入每组 environment JSON(env_<组名>.json)并随结果留档。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PY:-/usr/bin/python3.12}"
CFG() { "$PY" "$ROOT/config.py" --sh "$1"; }

OUT_DIR="$ROOT/results/probe_c"
mkdir -p "$OUT_DIR" 2>/dev/null || {
  echo "results 目录权限不足: sudo chown -R $(id -un) \"$ROOT/results\""
  exit 1
}

SOURCE_NAME="$(CFG SOURCE_CONTAINER)"
TC_IMAGE="${TC_IMAGE:-kindest/node:v1.28.15}"   # 本地已有镜像,含 /usr/sbin/tc
TC_RATE="$(CFG TC_RATE)"; TC_BURST="$(CFG TC_BURST)"
TC_LATENCY="$(CFG TC_LATENCY)"; TC_NETEM_DELAY="$(CFG TC_NETEM_DELAY)"
KIND_THRESHOLD=30          # kind 节点容器 CPU 上限(%)
KIND_POLL_MAX=15           # 最多轮询 15 次(30min),仍忙则放弃测量
KIND_POLL_WAIT=120         # 轮询间隔 2min

echo "== 0) 前置:python3.12 依赖与源站 =="
"$PY" -c "import webdataset, torch, PIL, numpy" || {
  echo "python3.12 缺依赖(webdataset/torch/PIL/numpy)"; exit 1; }
if [ "$(docker inspect -f '{{.State.Running}}' "$SOURCE_NAME" 2>/dev/null || echo false)" != "true" ]; then
  echo "  源站 $SOURCE_NAME 不在跑 → setup/02_source_up.sh"
  bash "$ROOT/setup/02_source_up.sh"
fi
SRC_PID="$(docker inspect -f '{{.State.Pid}}' "$SOURCE_NAME")"

# ---- tc:断言/挂载(sudo 优先,docker 回退;以出现 tbf 为准,默认 noqueue 不算)----
TC_OK=0; TC_VIA=none; TC_QDISC=""
tc_show_sudo() {
  timeout 15 sudo -n nsenter -t "$SRC_PID" -n tc qdisc show dev eth0 </dev/null 2>/dev/null
}
tc_show_docker() {
  timeout 60 docker run --rm --net="container:$SOURCE_NAME" --cap-add=NET_ADMIN \
    --entrypoint /bin/sh "$TC_IMAGE" -c 'tc qdisc show dev eth0' 2>/dev/null
}
tc_ensure() {
  local out; out="$(tc_show_sudo || true)"
  if [ -n "$out" ]; then TC_VIA=sudo
  else out="$(tc_show_docker || true)"; TC_VIA=docker; fi
  if printf '%s' "$out" | grep -q 'tbf'; then
    TC_OK=1; TC_QDISC="$(printf '%s' "$out" | tr '\n' ';')"
    echo "  tc 已挂载(via $TC_VIA): $TC_QDISC"; return 0
  fi
  echo "  tc 未挂载(当前: ${out:-空}),尝试挂载 ..."
  if timeout 30 sudo -n bash "$ROOT/probe_a/tc_setup.sh" </dev/null >/dev/null 2>&1; then
    TC_VIA=sudo
  else
    timeout 60 docker run --rm --net="container:$SOURCE_NAME" --cap-add=NET_ADMIN \
      --entrypoint /bin/sh "$TC_IMAGE" -c "
        tc qdisc del dev eth0 root 2>/dev/null || true
        tc qdisc add dev eth0 root handle 1: tbf rate $TC_RATE burst $TC_BURST latency $TC_LATENCY &&
        tc qdisc add dev eth0 parent 1:1 handle 10: netem delay $TC_NETEM_DELAY" \
      >/dev/null 2>&1 || { echo "tc 挂载失败"; return 1; }
    TC_VIA=docker
  fi
  out="$(tc_show_sudo || true)"
  if [ -z "$out" ]; then out="$(tc_show_docker || true)"; fi
  printf '%s' "$out" | grep -q 'tbf' || { echo "tc 挂载后断言失败: $out"; return 1; }
  TC_OK=1; TC_QDISC="$(printf '%s' "$out" | tr '\n' ';')"
  echo "  tc 挂载成功(via $TC_VIA): $TC_QDISC"
}

# ---- 组前冷态:清 Agent 缓存目录 + drop_caches ----
CACHE_RM_OK=0; CACHE_RM_VIA=none
cache_reset() {
  if timeout 20 sudo -n rm -rf "$ROOT/.cache/agent" </dev/null >/dev/null 2>&1; then
    CACHE_RM_VIA=sudo
  elif timeout 60 docker run --rm -v "$ROOT/.cache:/mntcache" --entrypoint /bin/sh \
       "$TC_IMAGE" -c 'rm -rf /mntcache/agent' >/dev/null 2>&1; then
    CACHE_RM_VIA=docker
  else
    echo "  Agent 缓存目录清理失败"; CACHE_RM_OK=0; return 1
  fi
  if [ -e "$ROOT/.cache/agent" ]; then
    echo "  Agent 缓存目录仍存在"; CACHE_RM_OK=0; return 1
  fi
  CACHE_RM_OK=1; echo "  Agent 缓存目录已清(via $CACHE_RM_VIA)"
}
DROP_OK=0; DROP_VIA=none
drop_caches() {
  if timeout 15 sudo -n sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' </dev/null >/dev/null 2>&1; then
    DROP_OK=1; DROP_VIA=sudo
  elif timeout 60 docker run --rm --privileged --entrypoint /bin/sh "$TC_IMAGE" \
       -c 'sync; echo 3 > /proc/sys/vm/drop_caches' >/dev/null 2>&1; then
    DROP_OK=1; DROP_VIA=docker
  else
    DROP_OK=0; DROP_VIA=none; echo "  drop_caches 失败"; return 1
  fi
  echo "  drop_caches 完成(via $DROP_VIA)"
}

# ---- kind 节点负载检查(traincache-control-plane/worker* > 30% 则等 2min 再查)----
KIND_IDLE=0; KIND_ATTEMPTS=0; KIND_WORST_NAME=""; KIND_WORST_PCT=0; KIND_STATS=""
kind_check() {
  local attempt=0 line name cpu v
  while :; do
    attempt=$((attempt + 1)); KIND_ATTEMPTS=$attempt
    KIND_STATS="$(docker stats --no-stream --format "{{.Name}} {{.CPUPerc}}" 2>/dev/null || true)"
    KIND_WORST_PCT=0; KIND_WORST_NAME=""
    while read -r name cpu; do
      [ -n "$name" ] || continue
      case "$name" in
        traincache-control-plane|traincache-worker*) ;;
        *) continue ;;
      esac
      v="$(printf '%s' "${cpu%\%}" | tr -d ' ')"
      if awk -v a="$v" -v b="$KIND_WORST_PCT" 'BEGIN{exit !(a>b)}'; then
        KIND_WORST_PCT="$v"; KIND_WORST_NAME="$name"
      fi
    done <<< "$KIND_STATS"
    if awk -v a="$KIND_WORST_PCT" -v b="$KIND_THRESHOLD" 'BEGIN{exit !(a>b)}'; then
      echo "  负载检查#$attempt: $KIND_WORST_NAME=${KIND_WORST_PCT}% > ${KIND_THRESHOLD}%,等 ${KIND_POLL_WAIT}s"
      if [ "$attempt" -ge "$KIND_POLL_MAX" ]; then return 1; fi
      sleep "$KIND_POLL_WAIT"
    else
      KIND_IDLE=1
      echo "  负载检查#$attempt: 最高 ${KIND_WORST_NAME:-无}=${KIND_WORST_PCT}% ≤ ${KIND_THRESHOLD}%,空闲"
      return 0
    fi
  done
}

write_env_json() {  # $1=tag $2=file $3=src $4=workers
  "$PY" - "$2" "$1" "$3" "$4" "$TC_OK" "$TC_VIA" "$TC_QDISC" \
      "$CACHE_RM_OK" "$CACHE_RM_VIA" "$DROP_OK" "$DROP_VIA" \
      "$KIND_IDLE" "$KIND_ATTEMPTS" "$KIND_WORST_NAME" "$KIND_WORST_PCT" \
      "$KIND_STATS" <<'PYEOF'
import json, sys
from datetime import datetime
(path, tag, src, workers, tc_ok, tc_via, tc_qdisc, cache_ok, cache_via,
 drop_ok, drop_via, kind_idle, kind_attempts, kind_worst_name, kind_worst_pct,
 kind_stats) = sys.argv[1:17]
stats = {}
for sline in kind_stats.splitlines():
    parts = sline.split()
    if len(parts) == 2:
        stats[parts[0]] = parts[1]
env = {
    "group": tag, "source": src, "workers": int(workers),
    "date": datetime.now().isoformat(timespec="seconds"),
    "tc_check": {"ok": tc_ok == "1", "via": tc_via, "qdisc": tc_qdisc},
    "cache_reset": {"agent_cache_rm": cache_ok == "1", "via": cache_via},
    "drop_caches": {"ok": drop_ok == "1", "via": drop_via},
    "kind_load_check": {
        "idle": kind_idle == "1", "attempts": int(kind_attempts),
        "threshold_pct": 30,
        "max_cpu_pct": float(kind_worst_pct) if kind_worst_name else None,
        "busiest": kind_worst_name or None, "stats": stats,
        "note": "仅统计 traincache-* kind 节点容器(debug-control-plane 仅留档)"},
    "notes": ("epoch 之间不清缓存、不重启容器(测量对象);组间 sleep 5;"
              "tc=100mbit+20ms;热组读 TARS_DIR 本地路径;"
              "sudo 不可用时回退特权 docker 执行同操作(见 via 字段)"),
}
open(path, "w").write(json.dumps(env, ensure_ascii=False, indent=2) + "\n")
PYEOF
}

# ---- 主流程:四组顺序执行 ----
# 注意:变量名不能用 GROUPS —— bash 内置的只读特殊数组(当前用户的组 ID 列表),
# 赋值会被静默忽略,曾导致此处解析出 tag=1000 的事故。
GROUP_SPECS=("cold_w4 http 4" "cold_w0 http 0" "cold_w8 http 8" "hot_w4 local 4")
echo "== 1) 开跑前 tc 断言 =="
tc_ensure || exit 1

for group in "${GROUP_SPECS[@]}"; do
  read -r tag src w <<< "$group"
  if [ -z "$tag" ] || [ -z "$src" ] || [ -z "$w" ]; then
    echo "组参数解析失败: '$group'"; exit 1
  fi
  echo ""
  echo "===== 组 $tag(source=$src workers=$w)====="
  echo "-- 组前冷态(仅此一次;此后 epoch 之间不清任何缓存)--"
  cache_reset || exit 1
  sync
  drop_caches || exit 1
  if [ "$src" = http ]; then
    echo "-- tc 断言(冷组)--"
    tc_ensure || exit 1
  fi
  echo "-- kind 节点负载检查(计时测量前)--"
  kind_check || { echo "kind 节点持续繁忙,放弃测量(结果未产出)"; exit 1; }
  write_env_json "$tag" "$OUT_DIR/env_${tag}.json" "$src" "$w"
  echo "-- 开跑:3 epoch,epoch 之间不清缓存 --"
  "$PY" "$ROOT/probe_c/train_multi_epoch.py" \
    --source "$src" --workers "$w" \
    --epochs "$(CFG PROBE_C_EPOCHS)" \
    --seed "$(CFG PROBE_C_SEED)" \
    --out "$OUT_DIR/c_${tag}.json" \
    --env-file "$OUT_DIR/env_${tag}.json"
  sleep 5
done

echo ""
echo "完成 → results/probe_c/c_*.json;判定:python3.12 probes/probe_c/judge_c.py"

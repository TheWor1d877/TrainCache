#!/usr/bin/env bash
# ============================================================================
# D 轨勘测:单 Job 连续 10 epoch、每 epoch 换 seed、缓存欠配 50% 的 stall 形态
# ----------------------------------------------------------------------------
# 场景:v4"全程收益"专用场景数据 —— 多 epoch 欠配下 stall 逐 epoch 的重复形态。
# 欠配实现:nginx 源站容器 cgroup 限 1GiB(= config TRACK_D_CACHE_MB,数据集
# 2GB 的 50%)—— 源站侧只能缓存一半工作集,每 epoch 换 seed 重排读取顺序后,
# "冷/热混合"的 miss 形态仍应逐 epoch 重复出现,即本轨的测量对象。
#
# 时序(勿改):
#   ① tc 断言(qdisc 含 tbf)                              ② kind 节点负载检查
#   ③ docker update 内存限制并验证(HostConfig.Memory/MemorySwap)==
#   ④ 起后台内存采样(每 TRACK_D_MEM_SAMPLE_S=2s 读 cgroup)
#   ⑤ 跑前冷态一次(rm 源码 Agent 缓存目录 + sync + drop_caches) ← 仅此一次!
#   ⑥ 10 epoch 连续跑(epoch 之间绝不清缓存 / 不重启容器)
#   ⑦ 杀采样、记 OOM 状态、恢复内存限制(--memory 0 --memory-swap 0)并验证
#
# sudo 说明:同 run_c.sh —— sudo -n 不可用(本机无 NOPASSWD、无 tty)时回退
# 特权/带 NET_ADMIN 的 docker 容器执行等价操作(tc qdisc、drop_caches、清缓存
# 目录),via 记入 environment(嵌入 results/probe_c/track_d_w4.json)。
# ============================================================================
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
MEM_LIMIT=$(( $(CFG TRACK_D_CACHE_MB) * 1024 * 1024 ))   # 1024MB → 1073741824
MEM_INTERVAL="$(CFG TRACK_D_MEM_SAMPLE_S)"
EPOCHS="$(CFG TRACK_D_EPOCHS)"; SEED="$(CFG TRACK_D_SEED)"
WORKERS="$(CFG TRACK_D_WORKERS)"        # config 列表 → 空格连接,单档为 "4"
KIND_THRESHOLD=30; KIND_POLL_MAX=15; KIND_POLL_WAIT=120

MEM_JSONL="$OUT_DIR/mem_track_d_w4.jsonl"
RESULT_JSON="$OUT_DIR/track_d_w4.json"
ENV_TMP="$(mktemp /tmp/track_d_env.XXXXXX.json)"

SRC_CID=""; CGROUP_DIR=""; SAMPLER_PID=""; LIMIT_APPLIED=0

cleanup() {
  local rc=$?
  if [ -n "$SAMPLER_PID" ] && kill -0 "$SAMPLER_PID" 2>/dev/null; then
    kill "$SAMPLER_PID" 2>/dev/null || true
    wait "$SAMPLER_PID" 2>/dev/null || true
  fi
  if [ "$LIMIT_APPLIED" = 1 ]; then
    echo "  [cleanup] 异常退出,恢复源站内存限制 ..."
    docker update --memory 0 --memory-swap 0 "$SOURCE_NAME" >/dev/null 2>&1 || true
  fi
  rm -f "$ENV_TMP" || true
  return $rc
}
trap cleanup EXIT

echo "== 0) 前置:python3.12 依赖与源站 =="
"$PY" -c "import webdataset, torch, PIL, numpy" || {
  echo "python3.12 缺依赖(webdataset/torch/PIL/numpy)"; exit 1; }
if [ "$(docker inspect -f '{{.State.Running}}' "$SOURCE_NAME" 2>/dev/null || echo false)" != "true" ]; then
  echo "  源站 $SOURCE_NAME 不在跑 → setup/02_source_up.sh"
  bash "$ROOT/setup/02_source_up.sh"
fi
SRC_CID="$(docker inspect -f '{{.Id}}' "$SOURCE_NAME")"
SRC_PID="$(docker inspect -f '{{.State.Pid}}' "$SOURCE_NAME")"
CGROUP_DIR="/sys/fs/cgroup/system.slice/docker-${SRC_CID}.scope"

# ---- ① tc:断言/挂载(sudo 优先,docker 回退;以出现 tbf 为准)----
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

# ---- ② kind 节点负载检查(traincache-* > 30% 则等 2min 再查)----
KIND_IDLE=0; KIND_ATTEMPTS=0; KIND_WORST_NAME=""; KIND_WORST_PCT=0; KIND_STATS=""
kind_check() {
  local attempt=0 line name cpu v
  while :; do
    attempt=$((attempt + 1)); KIND_ATTEMPTS=$((KIND_ATTEMPTS + 1))
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
      echo "  负载检查: $KIND_WORST_NAME=${KIND_WORST_PCT}% > ${KIND_THRESHOLD}%,等 ${KIND_POLL_WAIT}s"
      if [ "$attempt" -ge "$KIND_POLL_MAX" ]; then return 1; fi
      sleep "$KIND_POLL_WAIT"
    else
      KIND_IDLE=1
      echo "  负载检查: 最高 ${KIND_WORST_NAME:-无}=${KIND_WORST_PCT}% ≤ ${KIND_THRESHOLD}%,空闲"
      return 0
    fi
  done
}

# ---- ⑤ 跑前冷态:sudo -n 优先,回退特权/带挂载 docker ----
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

write_env_json() {  # $1=out 路径;$2=started(采样起始);$3=check_rounds
  "$PY" - "$1" "$2" "$3" "$SOURCE_NAME" "$SRC_CID" "$MEM_LIMIT" "$MEM_INTERVAL" \
      "$TC_OK" "$TC_VIA" "$TC_QDISC" "$CACHE_RM_OK" "$CACHE_RM_VIA" \
      "$DROP_OK" "$DROP_VIA" "$KIND_IDLE" "$KIND_ATTEMPTS" "$KIND_WORST_NAME" \
      "$KIND_WORST_PCT" "$KIND_STATS" "$EPOCHS" "$SEED" "$WORKERS" "$MEM_JSONL" <<'PYEOF'
import json, sys
from datetime import datetime
(path, started, rounds, src_name, src_cid, mem_limit, mem_interval, tc_ok, tc_via,
 tc_qdisc, cache_ok, cache_via, drop_ok, drop_via, kind_idle, kind_attempts,
 kind_worst_name, kind_worst_pct, kind_stats, epochs, seed, workers,
 mem_jsonl) = sys.argv[1:24]
stats = {}
for sline in kind_stats.splitlines():
    parts = sline.split()
    if len(parts) == 2:
        stats[parts[0]] = parts[1]
env = {
    "group": "track_d_w4", "source": "http", "workers": int(workers),
    "date": datetime.now().isoformat(timespec="seconds"),
    "track": "D(多 epoch 欠配 stall 重复形态)",
    "epochs": int(epochs), "seed": int(seed),
    "tc_check": {"ok": tc_ok == "1", "via": tc_via, "qdisc": tc_qdisc},
    "kind_load_check": {
        "idle": kind_idle == "1", "attempts": int(kind_attempts),
        "rounds": int(rounds), "threshold_pct": 30,
        "max_cpu_pct": float(kind_worst_pct) if kind_worst_name else None,
        "busiest": kind_worst_name or None, "stats": stats,
        "note": "仅统计 traincache-* kind 节点容器;跑前共查 rounds 轮"
                "(步骤② + 开跑前复检),任一 >30% 会等到空闲"},
    "mem_limit": {
        "bytes": int(mem_limit), "mb": int(mem_limit) // (1024 * 1024),
        "container": src_name, "container_id": src_cid,
        "apply_via": "docker update", "restore": "--memory 0 --memory-swap 0",
        "restore_caveat": ("docker 将 update --memory 0 视为不变更(零值语义),"
                           "若 0/0 后验证不变,脚本回退 128GiB 等效恢复,"
                           "实际值见 mem_*.jsonl 的 final 行 limit_restored"),
        "note": "数据集 2GB 的 50%;源站侧只能缓存一半工作集(欠配实现)"},
    "sampler": {"path": mem_jsonl, "interval_s": float(mem_interval),
                "started": started,
                "fields": "cgroup v2 memory.current + memory.stat 的 file 行"},
    "cache_reset": {"agent_cache_rm": cache_ok == "1", "via": cache_via},
    "drop_caches": {"ok": drop_ok == "1", "via": drop_via},
    "notes": ("仅跑前一次冷态;epoch 之间绝不清缓存/不重启容器(测量对象);"
              "tc=100mbit+20ms;欠配=源站容器 cgroup 限 1GiB(=数据集 50%);"
              "sudo 不可用时回退特权 docker 执行同操作(见 via 字段);"
              "JOB 内连续 10 epoch,每 epoch seed=基种子+epoch"),
}
open(path, "w").write(json.dumps(env, ensure_ascii=False, indent=2) + "\n")
PYEOF
}

echo "== ①) tc 断言 =="
tc_ensure || exit 1

echo ""
echo "== ②) kind 节点负载检查(计时测量前互斥检查)#1 =="
kind_check || { echo "kind 节点持续繁忙,放弃测量(结果未产出)"; exit 1; }
KIND_ROUNDS=1

echo ""
echo "== ③) 源站容器内存限制:${MEM_LIMIT} 字节(1GiB,数据集 2GB 的 50%) =="
docker update --memory="${MEM_LIMIT}" --memory-swap="${MEM_LIMIT}" "$SOURCE_NAME"
MEM_VERIFY="$(docker inspect -f '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}' "$SOURCE_NAME")"
echo "  验证 HostConfig.Memory/MemorySwap = $MEM_VERIFY"
[ "$MEM_VERIFY" = "$MEM_LIMIT $MEM_LIMIT" ] || { echo "内存限制验证失败,放弃"; exit 1; }
LIMIT_APPLIED=1

echo ""
echo "== ④) 起后台内存采样(每 ${MEM_INTERVAL}s → $MEM_JSONL)=="
: > "$MEM_JSONL"
"$PY" - "$MEM_JSONL" "$CGROUP_DIR" "$MEM_LIMIT" "$MEM_INTERVAL" "$SOURCE_NAME" "$SRC_CID" <<'PYEOF' &
import json, os, sys, time
from datetime import datetime
path, cgroup, limit, interval, name, cid = sys.argv[1:7]
limit = int(limit); interval = float(interval)
f = open(path, "a")
def emit(obj):
    f.write(json.dumps(obj, ensure_ascii=False) + "\n"); f.flush()
started = datetime.now().isoformat(timespec="seconds")
emit({"type": "meta", "container": name, "container_id": cid,
      "cgroup": cgroup, "mem_limit_bytes": limit, "sample_interval_s": interval,
      "started": started,
      "note": "每采样点读 cgroup v2 memory.current 与 memory.stat 的 file 行"})
n = 0
while True:
    rec = {"n": n, "t": datetime.now().isoformat(timespec="seconds"),
           "ts": round(time.time(), 3)}
    try:
        with open(os.path.join(cgroup, "memory.current")) as fh:
            rec["memory_current"] = int(fh.read().strip())
        with open(os.path.join(cgroup, "memory.stat")) as fh:
            for line in fh:
                if line.startswith("file "):
                    rec["file"] = int(line.split()[1]); break
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {e}"
    n += 1
    emit(rec)
    time.sleep(interval)
PYEOF
SAMPLER_PID=$!
sleep 1
kill -0 "$SAMPLER_PID" 2>/dev/null || { echo "采样进程启动失败"; exit 1; }
echo "  采样进程 pid=$SAMPLER_PID"
STARTED=""
for _ in 1 2 3 4 5 6 7 8 9 10; do
  STARTED="$("$PY" -c 'import json,sys
try:
    print(json.loads(open(sys.argv[1]).readline())["started"])
except Exception:
    pass' "$MEM_JSONL" 2>/dev/null || true)"
  [ -n "$STARTED" ] && break
  sleep 0.5
done
STARTED="${STARTED:-unknown}"
echo "  采样起始: $STARTED"

echo ""
echo "== ⑤) 跑前冷态(仅此一次;此后 epoch 之间不清任何缓存)=="
cache_reset || exit 1
sync
drop_caches || exit 1

echo ""
echo "== ②复检) 开跑前最后负载检查 =="
kind_check || { echo "kind 节点持续繁忙,放弃测量(结果未产出)"; exit 1; }
KIND_ROUNDS=$((KIND_ROUNDS + 1))

write_env_json "$ENV_TMP" "$STARTED" "$KIND_ROUNDS"
echo "  环境快照 → $ENV_TMP(随结果嵌入 track_d_w4.json)"

echo ""
echo "== ⑥) 开跑:${EPOCHS} epoch(seed=${SEED}+e,w${WORKERS},http 冷态,epoch 之间不清缓存)=="
"$PY" "$ROOT/probe_c/train_multi_epoch.py" \
  --source http --workers "$WORKERS" --epochs "$EPOCHS" --seed "$SEED" \
  --out "$RESULT_JSON" --env-file "$ENV_TMP"

echo ""
echo "== ⑦) 收尾:停采样、记 OOM、恢复内存限制 =="
kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true
SAMPLER_PID=""

OOM_INFO="$(docker inspect -f '{{.State.OOMKilled}} {{.RestartCount}}' "$SOURCE_NAME")"
OOM_AFTER="${OOM_INFO%% *}"; RESTART_AFTER="${OOM_INFO##* }"
MEM_EVENTS_RAW="$(cat "$CGROUP_DIR/memory.events" 2>/dev/null | tr '\n' ';' || true)"

RESTORE_NOTE=""
docker update --memory 0 --memory-swap 0 "$SOURCE_NAME"
RESTORE_VERIFY="$(docker inspect -f '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}' "$SOURCE_NAME")"
echo "  恢复验证(请求 0/0 后)HostConfig.Memory/MemorySwap = $RESTORE_VERIFY"
if [ "$RESTORE_VERIFY" != "0 0" ]; then
  # 实测(本机 docker 29.1.3):update --memory 0 被当作"不变更"(0 是零值),
  # 容器不重建时无法还原 HostConfig.Memory=0;回退为远高于物理内存的大值,
  # 对运行中的容器等效无限(本机内存 32GiB、无 swap)。
  RESTORE_FALLBACK=$(( 4 * 34359738368 ))   # 128GiB
  echo "  0/0 未生效 → 回退等效恢复:--memory ${RESTORE_FALLBACK} --memory-swap ${RESTORE_FALLBACK}"
  docker update --memory="$RESTORE_FALLBACK" --memory-swap="$RESTORE_FALLBACK" "$SOURCE_NAME"
  RESTORE_VERIFY="$(docker inspect -f '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}' "$SOURCE_NAME")"
  echo "  恢复验证(回退后) = $RESTORE_VERIFY"
  RESTORE_NOTE="请求 --memory 0/--memory-swap 0 未生效(docker 将 0 视为不变更,需重建容器才能归零,本装置禁止);回退为 128GiB(≈4×宿主内存,等效无限)"
fi
RESTORE_MEM="${RESTORE_VERIFY%% *}"; RESTORE_SWAP="${RESTORE_VERIFY##* }"
LIMIT_APPLIED=0

"$PY" - "$MEM_JSONL" "$OOM_AFTER" "$RESTART_AFTER" "$MEM_EVENTS_RAW" \
    "$RESTORE_MEM" "$RESTORE_SWAP" "$RESTORE_NOTE" <<'PYEOF'
import json, sys
from datetime import datetime
path, oom, restart, events_raw, rmem, rswap, rnote = sys.argv[1:8]
samples = []; meta = None
with open(path) as f:
    for line in f:
        line = line.strip()
        if not line: continue
        rec = json.loads(line)
        if rec.get("type") == "meta": meta = rec
        elif "memory_current" in rec: samples.append(rec)
events = {}
for kv in events_raw.split(";"):
    parts = kv.split()
    if len(parts) == 2:
        try: events[parts[0]] = int(parts[1])
        except ValueError: pass
file_max = max((s.get("file") or 0) for s in samples) if samples else None
cur_max = max((s["memory_current"] for s in samples), default=None)
rec = {"type": "final", "stop": datetime.now().isoformat(timespec="seconds"),
       "samples": len(samples), "file_max": file_max,
       "memory_current_max": cur_max,
       "mem_limit_bytes": (meta or {}).get("mem_limit_bytes"),
       "oom_killed": oom == "true", "restart_count": int(restart),
       "memory_events": events,
       "limit_restored": {"requested": "0 0", "memory": int(rmem),
                          "memory_swap": int(rswap),
                          "note": rnote or "0/0 生效"}
       }
with open(path, "a") as f:
    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
print(f"  采样 {len(samples)} 点;file_max={file_max} "
      f"current_max={cur_max};oom_killed={rec['oom_killed']} "
      f"restart={rec['restart_count']} oom_kill事件={events.get('oom_kill')}")
PYEOF

echo ""
echo "完成 → $RESULT_JSON / $MEM_JSONL"
echo "判定:python3.12 probes/probe_c/judge_track_d.py"

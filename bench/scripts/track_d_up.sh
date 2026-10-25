#!/usr/bin/env bash
# ============================================================================
# D 轨装置编排(up):源站容器 → 欠配 50% → tc → 冷态 → 连续 10 epoch → 恢复 → 足迹
# ----------------------------------------------------------------------------
# 零参数;参数全部自读 bench/config.py 的 TRACK_D_*(bash 侧经 config.py --sh)。
# 可选开关:--dry-run 只打印将执行的命令(不触碰 docker/tc/端口/数据);
#           -h/--help 打印用法。
#
# 步骤(与任务约定逐字一致):
#   ① 起源站容器 traincache-source-d(不存在则 docker run -d --name ... -p 9100:80
#      -v <TRACK_D_DIR>:/usr/share/nginx/html/track-d:ro nginx:latest;已存在则跳过
#      并校验挂载/确保在跑)
#   ② 欠配:docker update --memory/--memory-swap = TRACK_D_CACHE_MB(=1GiB,
#      数据集 2GiB 的 50%),并以 HostConfig 验证生效
#   ③ tc_apply.sh <100mbit> <20ms> --target source-d
#   ④ 冷态:sync + drop_caches(优先 echo .sudo-pass | sudo -S;无则回退特权 docker,
#      via 留痕)→ docker restart traincache-source-d
#      ⚠ 实测(probes/probe_a/tc_setup.sh 与 bench/scripts/cold_reset.sh 同因):
#        docker restart 重建容器 netns → eth0 qdisc 随之丢失,故重启后复挂 tc 并
#        断言(以出现 tbf 为准);否则训练会跑在无限速链路上,测量无效。
#   ⑤ $PY(缺省 /usr/bin/python3) scripts/bench_track_d.py 全 TRACK_D_EPOCHS epoch
#      → TRACK_D_RESULTS_DIR
#   ⑥ 后置:欠配等效恢复 docker update --memory=128GiB --memory-swap=128GiB
#      (本机 docker 将 update --memory 0 视为"不变更"(零值语义),见
#       probes/probe_c/run_track_d.sh 实证,故用 128GiB 等效恢复)
#   ⑦ 磁盘足迹:跑前/跑后 df -h / 与 du -sh <TRACK_D_DIR> <TRACK_D_RESULTS_DIR>
#      追加写 TRACK_D_RESULTS_DIR/disk_usage.txt(含时间戳)
# 运行环境留痕:TRACK_D_RESULTS_DIR/env_track_d_up.json(欠配值、tc/冷态 via 等)。
# 异常退出保护:欠配已应用但未恢复时,EXIT trap 自动回退 128GiB。
# ============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PY:-/usr/bin/python3}"
CFG() { "$PY" "$ROOT/config.py" --sh "$1"; }

NAME="$(CFG TRACK_D_SOURCE_CONTAINER)"
PORT="$(CFG TRACK_D_SOURCE_PORT)"
DATA_DIR="$(CFG TRACK_D_SOURCE_MOUNT)"
HTML_SUB="$(CFG TRACK_D_SOURCE_HTML)"
OUT_DIR="$(CFG TRACK_D_RESULTS_DIR)"
RATE="$(CFG TRACK_D_TC_RATE)"
DELAY="$(CFG TRACK_D_TC_DELAY)"
CACHE_MB="$(CFG TRACK_D_CACHE_MB)"
EPOCHS="$(CFG TRACK_D_EPOCHS)"
SEED="$(CFG TRACK_D_SEED)"
WORKERS="$(CFG TRACK_D_WORKERS)"
MEM_LIMIT=$(( CACHE_MB * 1024 * 1024 ))          # 1024MB → 1073741824(1GiB)
RESTORE_MEM="128GiB"                             # --memory 0 不生效,用等效大值
TC_IMAGE="${TC_IMAGE:-kindest/node:v1.28.15}"    # 本地已有镜像,含 /usr/sbin/tc
IMAGE="${SOURCE_IMAGE_D:-nginx:latest}"          # 本地已有镜像,无需拉取
TARGET="source-d"                                # tc_apply/tc_teardown 的 --target 别名
URL0="$(printf "$(CFG TRACK_D_URL)" 0)"          # 第 0 个分片 URL(HEAD 探活用)

usage() {
  cat <<EOF
用法: $(basename "$0") [--dry-run] [-h|--help]

D 轨装置编排(零参数即全流程):源站容器 $NAME(nginx:latest,端口 $PORT,挂
$DATA_DIR)→ 欠配 ${CACHE_MB}MB(--memory/--memory-swap,数据集 2GiB 的 50%)→
tc($RATE+$DELAY,--target $TARGET)→ 冷态(sync+drop_caches+docker restart,重启后
复挂 tc)→ 训练 $EPOCHS epoch($PY scripts/bench_track_d.py)→ 内存限制恢复
$RESTORE_MEM → 磁盘足迹追加写 $OUT_DIR/disk_usage.txt。

参数自读 bench/config.py(TRACK_D_*),无需命令行参数。
  --dry-run   只打印将执行的命令,不触碰 docker/tc/端口/数据
EOF
}

DRY=0
for arg in "$@"; do
  case "$arg" in
    -h|--help) usage; exit 0 ;;
    --dry-run) DRY=1 ;;
    *) echo "未知参数: $arg" >&2; usage >&2; exit 2 ;;
  esac
done

LIMIT_APPLIED=0
DROP_CACHES_VIA="(未执行)"
TC_SHOW_VIA=""
TC_QDISC=""
SRC_CID=""
RESTORE_VERIFY="(未执行)"

cleanup() {
  local rc=$?
  if [ "$LIMIT_APPLIED" = 1 ]; then
    echo "  [cleanup] 异常退出 → 恢复内存限制 --memory/--memory-swap=$RESTORE_MEM"
    docker update --memory="$RESTORE_MEM" --memory-swap="$RESTORE_MEM" "$NAME" \
      >/dev/null 2>&1 || true
  fi
  return $rc
}
trap cleanup EXIT

say() { echo "[dry-run] $*"; }

# ---- tc 只读断言:显示源站容器 eth0 qdisc(sudo -S 优先,特权/带 NET_ADMIN 的
#      docker 共享 netns 回退;以出现 tbf 为准,口径同 probes/probe_c/run_c.sh)----
tc_show() {
  local pid out
  pid="$(docker inspect -f '{{.State.Pid}}' "$NAME" 2>/dev/null || true)"
  if [ -n "$pid" ] && [ "$pid" != "0" ] && [ -f "$ROOT/.sudo-pass" ]; then
    out="$(echo "$(cat "$ROOT/.sudo-pass")" \
      | timeout 20 sudo -S -p '' nsenter -t "$pid" -n tc qdisc show dev eth0 \
        2>/dev/null || true)"
    if [ -n "$out" ]; then TC_SHOW_VIA="sudo -S nsenter"; printf '%s\n' "$out"; return 0; fi
  fi
  out="$(timeout 60 docker run --rm --net="container:$NAME" --cap-add=NET_ADMIN \
    --entrypoint /bin/sh "$TC_IMAGE" -c 'tc qdisc show dev eth0' 2>/dev/null || true)"
  if [ -n "$out" ]; then TC_SHOW_VIA="docker netns($TC_IMAGE)"; printf '%s\n' "$out"; return 0; fi
  return 1
}

tc_assert() {
  local out
  if [ "$DRY" = 1 ]; then
    say "断言 $NAME eth0 qdisc 含 tbf(sudo -S nsenter 优先,docker netns 回退)"
    TC_QDISC="(dry-run)"; TC_SHOW_VIA="(dry-run)"
    return 0
  fi
  out="$(tc_show)" || { echo "  tc 断言失败:无法读取 $NAME 的 eth0 qdisc" >&2; return 1; }
  if printf '%s' "$out" | grep -q 'tbf'; then
    TC_QDISC="$(printf '%s' "$out" | tr '\n' ';')"
    echo "  tc 已在位(via $TC_SHOW_VIA):$TC_QDISC"
    return 0
  fi
  echo "  tc 不在位(qdisc 无 tbf):$(printf '%s' "$out" | tr '\n' ';')" >&2
  return 1
}

# ---- ③ tc 挂载(幂等;也可用于 restart 后的复挂)----
tc_tier_apply() {
  if [ "$DRY" = 1 ]; then
    say "bash $ROOT/scripts/tc_apply.sh $RATE $DELAY --target $TARGET"
    return 0
  fi
  if ! grep -q -- '--target' "$ROOT/scripts/tc_apply.sh"; then
    echo "  BLOCKED:scripts/tc_apply.sh 尚不支持 --target(需 T2b 交付);" >&2
    echo "  接口约定:tc_apply.sh <rate> <delay> --target $TARGET(=容器 $NAME)" >&2
    return 2
  fi
  bash "$ROOT/scripts/tc_apply.sh" "$RATE" "$DELAY" --target "$TARGET"
}

# ---- ④ 冷态:sync + drop_caches(sudo -S 优先,特权 docker 回退)+ docker restart ----
cold_reset_d() {
  echo "-- 冷态 ④:sync + drop_caches + docker restart $NAME"
  if [ "$DRY" = 1 ]; then
    say "sync; 优先: echo \$(cat $ROOT/.sudo-pass) | sudo -S -p '' sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
    say "     回退: docker run --rm --privileged --entrypoint /bin/sh $TC_IMAGE -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
    say "docker restart $NAME"
    DROP_CACHES_VIA="(dry-run)"
    return 0
  fi
  sync
  if [ -f "$ROOT/.sudo-pass" ] && echo "$(cat "$ROOT/.sudo-pass")" \
       | timeout 20 sudo -S -p '' sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' \
         >/dev/null 2>&1; then
    DROP_CACHES_VIA="sudo -S(bench/.sudo-pass)"
  elif timeout 60 docker run --rm --privileged --entrypoint /bin/sh "$TC_IMAGE" \
       -c 'sync; echo 3 > /proc/sys/vm/drop_caches' >/dev/null 2>&1; then
    DROP_CACHES_VIA="privileged-docker($TC_IMAGE)"
  else
    echo "  drop_caches 失败(sudo -S 与特权 docker 均不可用)" >&2
    return 1
  fi
  echo "  drop_caches 完成(via $DROP_CACHES_VIA)"
  docker restart "$NAME" >/dev/null
  echo "  $NAME 已重启(nginx 进程内热态清零;仅此一次,epoch 之间不再清)"
}

# ---- 源站探活:HEAD 只取响应头,不下载正文(不动 page cache 冷读口径)----
wait_source_ready() {
  local i
  if [ "$DRY" = 1 ]; then
    say "curl -sfI $URL0(HEAD 探活,不下载正文;最多 30 次×1s)"
    return 0
  fi
  for i in $(seq 1 30); do
    if curl -sfI "$URL0" -o /dev/null 2>/dev/null; then
      echo "  源站就绪:$URL0(HEAD 探活,未下载正文)"
      return 0
    fi
    sleep 1
  done
  echo "  源站未就绪:$URL0 30s 内不可达" >&2
  return 1
}

# ---- ⑦ 磁盘足迹(追加写,含时间戳)----
disk_footprint() {  # $1=阶段标记(跑前/跑后)
  if [ "$DRY" = 1 ]; then
    say "追加写 $OUT_DIR/disk_usage.txt: [$1 \$(date -Is)] + df -h / + du -sh $DATA_DIR $OUT_DIR"
    return 0
  fi
  {
    echo ""
    echo "== [$1] $(date -Is) =="
    df -h /
    du -sh "$DATA_DIR" "$OUT_DIR" 2>/dev/null || true
  } >> "$OUT_DIR/disk_usage.txt"
  echo "  磁盘足迹($1)已追加 → $OUT_DIR/disk_usage.txt"
}

write_env() {
  "$PY" - "$OUT_DIR/env_track_d_up.json" "$NAME" "$SRC_CID" "$IMAGE" "$PORT" \
    "$DATA_DIR" "$HTML_SUB" "$OUT_DIR" "$MEM_LIMIT" "$CACHE_MB" "$RESTORE_MEM" \
    "$RESTORE_VERIFY" "$RATE" "$DELAY" "$TC_QDISC" "$TC_SHOW_VIA" \
    "$DROP_CACHES_VIA" "$EPOCHS" "$SEED" "$WORKERS" "$URL0" "$PY" <<'PYEOF'
import json, sys
from datetime import datetime
(path, name, cid, image, port, data_dir, html_sub, out_dir, mem_limit, cache_mb,
 restore_mem, restore_verify, rate, delay, qdisc, tc_via, drop_via, epochs,
 seed, workers, url0, py) = sys.argv[1:23]
env = {
    "track": "D", "kind": "up",
    "date": datetime.now().isoformat(timespec="seconds"),
    "source_container": {
        "name": name, "id": cid or None, "image": image, "port": int(port),
        "mount": f"{data_dir}:/usr/share/nginx/html/{html_sub}:ro",
        "url": f"http://127.0.0.1:{port}/{html_sub}/shard-%06d.tar",
        "ready_probe": url0,
    },
    "undersize": {
        "mem_limit_bytes": int(mem_limit), "mb": int(cache_mb),
        "apply": "docker update --memory/--memory-swap",
        "note": "数据集 2GiB 的 50%(源站侧只能缓存一半工作集)",
        "restore_requested": restore_mem, "restore_verified": restore_verify,
        "restore_caveat": "--memory 0 被 docker 视为不变更(probe_c 实证),"
                          "故以 128GiB 等效恢复",
    },
    "tc": {"rate": rate, "delay": delay, "target": "source-d",
           "assert_via": tc_via, "qdisc": qdisc,
           "note": "docker restart 重建 netns → qdisc 丢失,冷态后已复挂并断言 tbf"},
    "cold_state": {"drop_caches_via": drop_via, "container_restart": True,
                   "note": "sync + drop_caches + docker restart;仅跑前一次,"
                           "epoch 之间不清缓存(测量对象)"},
    "loader": {
        "cmd": f"{py} scripts/bench_track_d.py --epochs {epochs} "
               f"--seed {seed} --workers {workers} --out-dir {out_dir}",
        "epochs": int(epochs), "seed": int(seed), "workers": int(workers),
    },
    "notes": "D 轨 up:欠配 50% + tc(100mbit+20ms)+ 跑前冷态一次 + 连续 epochs;"
             "sudo 不可用时回退特权 docker 执行 drop_caches(见 via 字段)",
}
open(path, "w").write(json.dumps(env, ensure_ascii=False, indent=2) + "\n")
print(f"  运行环境留痕 → {path}")
PYEOF
}

echo "== D 轨 up:源站 → 欠配 ${CACHE_MB}MB → tc $RATE+$DELAY → 冷态 → ${EPOCHS} epoch =="
echo "  容器=$NAME 端口=$PORT 数据=$DATA_DIR 结果=$OUT_DIR"

if [ "$DRY" = 0 ]; then
  if ! ls "$DATA_DIR"/shard-*.tar >/dev/null 2>&1; then
    echo "未找到 $DATA_DIR/shard-*.tar → 先 make track-d-data" >&2
    exit 1
  fi
  mkdir -p "$OUT_DIR"
else
  say "检查 $DATA_DIR/shard-*.tar 存在(缺失则提示先 make track-d-data)"
  say "mkdir -p $OUT_DIR"
fi

echo ""
echo "== ①) 源站容器(nginx,挂 $DATA_DIR → /usr/share/nginx/html/$HTML_SUB:ro)=="
if [ "$DRY" = 1 ]; then
  say "docker inspect $NAME(存在则校验挂载并确保在跑;不存在则↓)"
  say "docker run -d --name $NAME -p $PORT:80 -v $DATA_DIR:/usr/share/nginx/html/$HTML_SUB:ro $IMAGE"
  say "curl -sfI $URL0(等源站就绪,HEAD 不下载正文)"
else
  if docker inspect "$NAME" >/dev/null 2>&1; then
    MOUNTS="$(docker inspect -f '{{range .Mounts}}{{.Source}}=>{{.Destination}} {{end}}' "$NAME")"
    EXPECT="$DATA_DIR=>/usr/share/nginx/html/$HTML_SUB"
    case "$MOUNTS" in
      *"$EXPECT"*) echo "  容器已存在,挂载校验通过:$EXPECT" ;;
      *) echo "  容器已存在但挂载不符: ${MOUNTS:-无挂载}" >&2
         echo "  期望:$EXPECT → 先 bash scripts/track_d_down.sh 再重跑本脚本" >&2
         exit 1 ;;
    esac
    if [ "$(docker inspect -f '{{.State.Running}}' "$NAME")" != "true" ]; then
      echo "  容器已停止 → docker start $NAME"
      docker start "$NAME" >/dev/null
    fi
  else
    docker run -d --name "$NAME" -p "$PORT:80" \
      -v "$DATA_DIR:/usr/share/nginx/html/$HTML_SUB:ro" "$IMAGE"
  fi
  SRC_CID="$(docker inspect -f '{{.Id}}' "$NAME")"
  wait_source_ready
fi

echo ""
echo "== ②) 欠配:--memory/--memory-swap = ${MEM_LIMIT} 字节(${CACHE_MB}MB,数据集 2GiB 的 50%) =="
if [ "$DRY" = 1 ]; then
  say "docker update --memory=$MEM_LIMIT --memory-swap=$MEM_LIMIT $NAME"
  say "docker inspect -f '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}' $NAME(验证 = $MEM_LIMIT)"
else
  docker update --memory="$MEM_LIMIT" --memory-swap="$MEM_LIMIT" "$NAME" >/dev/null
  MEM_VERIFY="$(docker inspect -f '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}' "$NAME")"
  if [ "$MEM_VERIFY" != "$MEM_LIMIT $MEM_LIMIT" ]; then
    echo "  内存限制验证失败:HostConfig.Memory/MemorySwap = $MEM_VERIFY" >&2
    exit 1
  fi
  LIMIT_APPLIED=1
  echo "  欠配生效:HostConfig.Memory/MemorySwap = $MEM_VERIFY"
fi

echo ""
echo "== ③) tc:$RATE+$DELAY,挂到 --target $TARGET =="
tc_tier_apply
tc_assert

echo ""
echo "== ④) 冷态(仅此一次;epoch 之间不清缓存/不重启)=="
cold_reset_d
echo "-- docker restart 重建 netns → qdisc 丢失(probe_a/cold_reset 实证),复挂 tc --"
tc_tier_apply
tc_assert
wait_source_ready

echo ""
echo "== ⑤) 训练:$EPOCHS epoch(seed=$SEED,w$WORKERS)→ $OUT_DIR =="
disk_footprint "跑前"
if [ "$DRY" = 1 ]; then
  say "$PY $ROOT/scripts/bench_track_d.py --epochs $EPOCHS --seed $SEED --workers $WORKERS --out-dir $OUT_DIR"
else
  "$PY" "$ROOT/scripts/bench_track_d.py" --epochs "$EPOCHS" --seed "$SEED" \
    --workers "$WORKERS" --out-dir "$OUT_DIR"
fi

echo ""
echo "== ⑥) 后置:恢复内存限制(请求 $RESTORE_MEM;--memory 0 不生效,见 probe_c) =="
if [ "$DRY" = 1 ]; then
  say "docker update --memory=$RESTORE_MEM --memory-swap=$RESTORE_MEM $NAME"
  say "docker inspect -f '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}' $NAME(验证恢复)"
  RESTORE_VERIFY="(dry-run)"
else
  docker update --memory="$RESTORE_MEM" --memory-swap="$RESTORE_MEM" "$NAME" >/dev/null
  RESTORE_VERIFY="$(docker inspect -f '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}' "$NAME")"
  echo "  恢复验证:HostConfig.Memory/MemorySwap = $RESTORE_VERIFY"
  LIMIT_APPLIED=0
fi

echo ""
echo "== ⑦) 磁盘足迹(追加写 disk_usage.txt)=="
disk_footprint "跑后"
if [ "$DRY" = 0 ]; then write_env; fi

echo ""
echo "D 轨 up 完成:结果 → $OUT_DIR(per_step_<seed>_w<workers>.jsonl / epochs_meta.json)"
echo "判定入口:bash scripts/track_d_verify.sh(两轮冷跑对账)"

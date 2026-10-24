#!/usr/bin/env bash
# ============================================================================
# D 轨装置编排(verify):两轮冷跑对账(过关标准:epoch 级聚合 median stall 相对差 ≤10%)
# ----------------------------------------------------------------------------
# 零参数;参数自读 bench/config.py(TRACK_D_* 与 COLD_VERIFY_TOLERANCE)。
# 开关:--dry-run(只打印将执行的命令)/ --selftest(判据函数级自测)/ -h|--help。
#
# 流程:
#   ⓪ 前置门:同档位 tc 已在位断言(含 tbf);缺则 BLOCKED,提示先 make track-d-up
#      或手工重挂(tc_apply.sh <rate> <delay> --target source-d),退出码 2。
#   ①② 每轮:欠配前置(docker update --memory/--memory-swap=1GiB,同 up ②)→ 完整
#      冷态(sync + drop_caches + docker restart,口径同 up ④)→ docker restart
#      重建 netns 会丢 qdisc(probe_a/cold_reset 实证),故重启后复挂 tc 并断言 →
#      跑 bench_track_d.py --epochs 3,轮输出
#      TRACK_D_RESULTS_DIR/verify_<轮>/(per_step_*.jsonl + epochs_meta.json)。
#      两轮结束后恢复内存限制 128GiB(EXIT trap 兜底)。
#   ③ 判据(epoch 级聚合;主对话 2026-10-13 裁决):两轮 per-epoch median_stall
#      逐 epoch 相对差 |a-b|/max(a,b) ≤ COLD_VERIFY_TOLERANCE(10%)且两轮全轮
#      median_stall 相对差 ≤ 10% 判"对上";判据函数在
#      bench_track_d.compare_epoch_medians(= --selftest)。
#   ④ 报告项(不参与 verdict):两轮 per-epoch 窗口 stall 均值曲线(窗口=20 步,
#      epoch0 前 warmup 步不计,口径同第一步)逐点相对差继续计算并写入 json
#      (bench_track_d.compare_window_curves)—— 主对话裁决:窗口均值被 wait 分布
#      重尾毛刺主导,同一冷启内相邻 epoch 逐点最大相对差已达 37%~56%,低于指标
#      噪声底、无判别力(实测两轮聚合口径仅差 0.6%,与第一步判据③降级同模式)。
#   ⑤ 输出 TRACK_D_RESULTS_DIR/verify_<ts>.json(epoch 级聚合对账 + 窗口曲线
#      报告项 + verdict)。
# 退出码:0 对上(PASS);1 对不上(FAIL);2 BLOCKED(前置门不满足)。
# ============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PY:-/usr/bin/python3}"
CFG() { "$PY" "$ROOT/config.py" --sh "$1"; }

NAME="$(CFG TRACK_D_SOURCE_CONTAINER)"
PORT="$(CFG TRACK_D_SOURCE_PORT)"
HTML_SUB="$(CFG TRACK_D_SOURCE_HTML)"
OUT_DIR="$(CFG TRACK_D_RESULTS_DIR)"
RATE="$(CFG TRACK_D_TC_RATE)"
DELAY="$(CFG TRACK_D_TC_DELAY)"
TOL="$(CFG COLD_VERIFY_TOLERANCE)"               # 0.10(过关标准)
WINDOW=20                                        # 窗口步数(报告项口径,同第一步)
VERIFY_EPOCHS=3                                  # 每轮 epoch 数(任务约定)
SEED="$(CFG TRACK_D_SEED)"
WORKERS="$(CFG TRACK_D_WORKERS)"
CACHE_MB="$(CFG TRACK_D_CACHE_MB)"               # 欠配:数据集 2GiB 的 50%
MEM_LIMIT=$(( CACHE_MB * 1024 * 1024 ))          # 1024MB → 1073741824(1GiB,同 up ②)
RESTORE_MEM="128GiB"                             # --memory 0 不生效,用等效大值(同 up ⑥)
TC_IMAGE="${TC_IMAGE:-kindest/node:v1.28.15}"    # 本地已有镜像,含 /usr/sbin/tc
TARGET="source-d"                                # tc_apply/tc_teardown 的 --target 别名
URL0="$(printf "$(CFG TRACK_D_URL)" 0)"          # 第 0 个分片 URL(HEAD 探活用)

usage() {
  cat <<EOF
用法: $(basename "$0") [--dry-run | --selftest] [-h|--help]

D 轨两轮冷跑对账(零参数即全流程):每轮完整冷态(sync+drop_caches+docker
restart+复挂 tc)后跑 $VERIFY_EPOCHS epoch(seed=$SEED,w$WORKERS,结果到
$OUT_DIR/verify_<轮>/);判据(epoch 级聚合)= 两轮 per-epoch median_stall 逐
epoch 相对差与全轮 median_stall 相对差最大值 ≤ $TOL 判"对上";窗口(=$WINDOW
步)stall 均值曲线逐点对账降级为报告项(低于噪声底,不参与 verdict),仍写入
json;结果写 $OUT_DIR/verify_<ts>.json。

参数自读 bench/config.py,无需命令行参数。
  --dry-run   只打印将执行的命令,不触碰 docker/tc/端口/数据
  --selftest  判据函数级自测(构造两轮 per-epoch median:5% → pass、15% → fail,
              缺轮/缺 epoch → fail,阈值 $TOL),不碰装置
EOF
}

DRY=0
SELFTEST=0
for arg in "$@"; do
  case "$arg" in
    -h|--help) usage; exit 0 ;;
    --dry-run) DRY=1 ;;
    --selftest) SELFTEST=1 ;;
    *) echo "未知参数: $arg" >&2; usage >&2; exit 2 ;;
  esac
done

say() { echo "[dry-run] $*"; }

# ---- 欠配前置:两轮结束后恢复大内存限制;异常退出时 EXIT trap 兜底(同 up)----
LIMIT_APPLIED=0
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

# ---- 判据函数级自测(bench_track_d.compare_epoch_medians;不碰装置)----
if [ "$SELFTEST" = 1 ]; then
  echo "== 判据函数级自测:bench_track_d.compare_epoch_medians(阈值 $TOL)=="
  if "$PY" - "$ROOT/scripts" "$TOL" "$WINDOW" <<'PYEOF'
import sys
sys.path.insert(0, sys.argv[1])
import bench_track_d as bt

tol = float(sys.argv[2])
window = int(sys.argv[3])
# 假的两轮 per-epoch median_stall(3 epoch,值域与实测 stall 同量级)与全轮 median
meds_a = [0.080, 0.085, 0.090]
round_a = 0.085
scaled = lambda xs, k: [v * k for v in xs]

r5 = bt.compare_epoch_medians(meds_a, scaled(meds_a, 1.05),
                              round_a, round_a * 1.05, tol)         # 逐 epoch/全轮 5%
r15 = bt.compare_epoch_medians(meds_a, scaled(meds_a, 1.15),
                               round_a, round_a * 1.15, tol)        # 15% → fail
rround = bt.compare_epoch_medians(meds_a, meds_a,
                                  round_a, round_a * 1.15, tol)     # 仅全轮超阈 → fail
rmiss_round = bt.compare_epoch_medians([], scaled(meds_a, 1.05),
                                       round_a, round_a * 1.05, tol)  # 缺轮 → fail
rmiss_epoch = bt.compare_epoch_medians(meds_a, scaled(meds_a, 1.05)[:2],
                                       round_a, round_a * 1.05, tol)  # 缺 epoch → fail
rmiss_none = bt.compare_epoch_medians([0.080, None, 0.090], scaled(meds_a, 1.05),
                                      round_a, round_a * 1.05, tol)   # 缺测 → fail

print(f"  5%  → max_rel_diff={r5['max_rel_diff']:.4f} verdict={r5['verdict']}")
print(f"  15% → max_rel_diff={r15['max_rel_diff']:.4f} verdict={r15['verdict']}")
print(f"  仅全轮 15% → 逐 epoch max_rel_diff={rround['max_rel_diff']:.4f} "
      f"全轮 rel_diff={rround['round_median']['rel_diff']:.4f} verdict={rround['verdict']}")
print(f"  缺轮 → verdict={rmiss_round['verdict']}({rmiss_round.get('note', '')})")
print(f"  缺 epoch → verdict={rmiss_epoch['verdict']}({rmiss_epoch.get('note', '')})")
print(f"  缺测 → verdict={rmiss_none['verdict']}({rmiss_none.get('note', '')})")
assert r5["verdict"] == "PASS" and r5["verdict_label"] == "对上", r5
assert r5["max_rel_diff"] <= tol, r5
assert r15["verdict"] == "FAIL" and r15["verdict_label"] == "对不上", r15
assert r15["max_rel_diff"] > tol, r15
assert rround["verdict"] == "FAIL", rround
assert rmiss_round["verdict"] == "FAIL", rmiss_round
assert rmiss_epoch["verdict"] == "FAIL", rmiss_epoch
assert rmiss_none["verdict"] == "FAIL", rmiss_none
# 报告项(不参与 verdict):窗口曲线 max_rel_diff 仍可计算/留档
base = [[0.20 + 0.010 * i for i in range(window)],
        [0.30 - 0.005 * i for i in range(window)]]
wres = bt.compare_window_curves(base, [[v * 1.15 for v in row] for row in base], tol)
print(f"  报告项(窗口曲线) → max_rel_diff={wres['max_rel_diff']:.4f}"
      f"(不参与 verdict)")
assert wres["max_rel_diff"] is not None and wres["max_rel_diff"] > tol, wres
print(f"SELFTEST OK:阈值 {tol:.0%} 逻辑成立(5% pass / 15% fail / 仅全轮超阈 fail / "
      f"缺轮/缺 epoch/缺测 fail)")
PYEOF
  then
    echo "自测通过(--selftest,未触碰装置)"
  else
    echo "自测失败" >&2
    exit 1
  fi
  exit 0
fi

# ---- tc 只读断言(同 up;sudo -S 优先,docker netns 回退;以出现 tbf 为准)----
TC_SHOW_VIA=""
TC_QDISC=""
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
  out="$(tc_show)" || { echo "  tc 断言失败:无法读取 $NAME 的 eth0 qdisc(容器不在?)" >&2; return 1; }
  if printf '%s' "$out" | grep -q 'tbf'; then
    TC_QDISC="$(printf '%s' "$out" | tr '\n' ';')"
    echo "  tc 已在位(via $TC_SHOW_VIA):$TC_QDISC"
    return 0
  fi
  echo "  tc 不在位(qdisc 无 tbf):$(printf '%s' "$out" | tr '\n' ';')" >&2
  return 1
}

blocked() {
  echo "BLOCKED:$1" >&2
  echo "  提示:先 make track-d-up,或手工重挂:bash $ROOT/scripts/tc_apply.sh $RATE $DELAY --target $TARGET" >&2
  if [ "$DRY" = 0 ]; then
    "$PY" - "$OUT_DIR/verify_${TS}.json" "$NAME" "$TC_QDISC" <<'PYEOF'
import json, sys
from datetime import datetime
path, name, qdisc = sys.argv[1:4]
out = {"track": "D", "kind": "verify",
       "date": datetime.now().isoformat(timespec="seconds"),
       "verdict": "BLOCKED", "verdict_label": "前置不满足(tc 不在位)",
       "container": name, "tc_qdisc": qdisc,
       "note": "先 make track-d-up,或手工重挂 tc_apply.sh <rate> <delay> --target source-d"}
open(path, "w").write(json.dumps(out, ensure_ascii=False, indent=2) + "\n")
print(f"  BLOCKED 记录 → {path}")
PYEOF
  fi
  exit 2
}

# ---- tc 挂载(restart 后复挂用;幂等)----
tc_tier_apply() {
  if ! grep -q -- '--target' "$ROOT/scripts/tc_apply.sh"; then
    echo "  BLOCKED:scripts/tc_apply.sh 尚不支持 --target(需 T2b 交付);" >&2
    return 2
  fi
  bash "$ROOT/scripts/tc_apply.sh" "$RATE" "$DELAY" --target "$TARGET"
}

# ---- 完整冷态(sync + drop_caches + docker restart;口径同 up ④)----
cold_reset_d() {
  echo "-- 冷态:sync + drop_caches + docker restart $NAME"
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
  echo "  $NAME 已重启(源站进程内热态清零;每轮完整冷态)"
}

# ---- 源站探活:HEAD 只取响应头,不下载正文(不动 page cache 冷读口径)----
wait_source_ready() {
  local i
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

TS="$(date +%Y%m%d-%H%M%S)"
echo "== D 轨 verify:两轮冷跑对账(每轮 $VERIFY_EPOCHS epoch;判据:epoch 级聚合 median stall,阈值 ≤$TOL;窗口 $WINDOW 步曲线为报告项)=="
echo "  容器=$NAME 结果=$OUT_DIR 轮输出=$OUT_DIR/verify_<轮>/"

if [ "$DRY" = 1 ]; then
  say "断言 $NAME eth0 qdisc 含 tbf(缺则 BLOCKED 退出 2)"
  for r in 1 2; do
    say "mkdir -p $OUT_DIR/verify_$r"
    say "docker update --memory=$MEM_LIMIT --memory-swap=$MEM_LIMIT $NAME   # 每轮冷态前欠配(同 up ②)"
    say "docker inspect -f '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}' $NAME(验证欠配)"
    say "# 第 $r 轮:sync + drop_caches(sudo -S .sudo-pass 优先,特权 docker 回退)+ docker restart $NAME"
    say "bash $ROOT/scripts/tc_apply.sh $RATE $DELAY --target $TARGET   # restart 重建 netns 丢 qdisc → 复挂"
    say "断言 $NAME eth0 qdisc 含 tbf"
    say "curl -sfI $URL0(HEAD 探活)"
    say "$PY $ROOT/scripts/bench_track_d.py --epochs $VERIFY_EPOCHS --out-dir $OUT_DIR/verify_$r"
  done
  say "docker update --memory=$RESTORE_MEM --memory-swap=$RESTORE_MEM $NAME   # 两轮结束恢复(EXIT trap 兜底)"
  say "$PY - <scripts_dir> $OUT_DIR/verify_1 $OUT_DIR/verify_2 $OUT_DIR/verify_${TS}.json $TOL $WINDOW"
  say "  判据(bench_track_d.compare_epoch_medians):两轮 per-epoch median_stall 逐 epoch 相对差 ≤$TOL 且 全轮 median_stall 相对差 ≤$TOL → PASS(对上)"
  say "  报告项(bench_track_d.compare_window_curves,不参与 verdict):窗口 $WINDOW 步 stall 均值曲线逐点 max_rel_diff 仅计算并写入 json"
  say "判据:PASS(≤$TOL → 对上)/ FAIL(> $TOL → 对不上);退出码 0/1"
  exit 0
fi

mkdir -p "$OUT_DIR"

echo ""
echo "== ⓪) 前置门:同档位 tc 断言 =="
tc_assert || blocked "源站 $NAME 的 tc 不在位(tc 须先由 track-d-up 挂好)"

for R in 1 2; do
  RDIR="$OUT_DIR/verify_$R"
  echo ""
  echo "===== 第 $R/2 轮:冷态 → 复挂 tc → $VERIFY_EPOCHS epoch ====="
  mkdir -p "$RDIR"
  tc_assert || blocked "第 $R 轮开跑前 tc 不在位"      # 每轮同档位断言
  # 欠配前置(每轮冷态前;docker update 命令与容器名取值同 track_d_up.sh ②)
  echo "-- 欠配前置(每轮冷态前):--memory/--memory-swap=${MEM_LIMIT} 字节(${CACHE_MB}MB,数据集 2GiB 的 50%)--"
  docker update --memory="$MEM_LIMIT" --memory-swap="$MEM_LIMIT" "$NAME" >/dev/null
  MEM_VERIFY="$(docker inspect -f '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}' "$NAME")"
  if [ "$MEM_VERIFY" != "$MEM_LIMIT $MEM_LIMIT" ]; then
    echo "  内存限制验证失败:HostConfig.Memory/MemorySwap = $MEM_VERIFY" >&2
    exit 1
  fi
  LIMIT_APPLIED=1
  echo "  欠配生效:HostConfig.Memory/MemorySwap = $MEM_VERIFY"
  cold_reset_d || exit 1
  echo "-- docker restart 重建 netns → qdisc 丢失,复挂 tc 并断言 --"
  tc_tier_apply
  tc_assert || exit 1
  wait_source_ready || exit 1
  "$PY" "$ROOT/scripts/bench_track_d.py" --epochs "$VERIFY_EPOCHS" --out-dir "$RDIR"
done

echo ""
echo "== 恢复内存限制(两轮已结束;请求 $RESTORE_MEM;--memory 0 不生效,同 up ⑥)=="
docker update --memory="$RESTORE_MEM" --memory-swap="$RESTORE_MEM" "$NAME" >/dev/null
RESTORE_VERIFY="$(docker inspect -f '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}' "$NAME")"
echo "  恢复验证:HostConfig.Memory/MemorySwap = $RESTORE_VERIFY"
LIMIT_APPLIED=0

echo ""
echo "== 判据:两轮 per-epoch median_stall 聚合对账(阈值 ≤$TOL;窗口曲线降级为报告项)=="
RC=0
if "$PY" - "$ROOT/scripts" "$OUT_DIR/verify_1" "$OUT_DIR/verify_2" \
     "$OUT_DIR/verify_${TS}.json" "$TOL" "$WINDOW" "$TS" "$TC_QDISC" "$TC_SHOW_VIA" \
     <<'PYEOF'
import json, sys
from datetime import datetime
from pathlib import Path

scripts_dir, d1, d2, out_path, tol, window, ts, qdisc, tc_via = sys.argv[1:10]
sys.path.insert(0, scripts_dir)
import bench_track_d as bt

def load_round(d):
    d = Path(d)
    meta = json.loads((d / "epochs_meta.json").read_text())
    files = sorted(d.glob("per_step_*.jsonl"))
    if len(files) != 1:
        raise SystemExit(f"{d} 下 per_step_*.jsonl 数量 = {len(files)}(应恰好 1)")
    return meta, files[0], bt.load_records(files[0])

m1, f1, r1 = load_round(d1)
m2, f2, r2 = load_round(d2)
tol = float(tol)
window = int(window)
epochs = int(m1["epochs"])
steps = int(m1["steps_per_epoch"])

# ---- 主判据:epoch 级聚合对账(两轮 per-epoch median_stall + 全轮 median_stall)----
st1 = bt.epoch_stall_stats(r1, epochs)
st2 = bt.epoch_stall_stats(r2, epochs)
rs1 = bt.round_stall_stats(r1)
rs2 = bt.round_stall_stats(r2)
res = bt.compare_epoch_medians([s["median_stall"] for s in st1],
                               [s["median_stall"] for s in st2],
                               rs1["median_stall"], rs2["median_stall"], tol)

# ---- 报告项:窗口曲线逐点对账(主对话裁决降级,不参与 verdict)----
c1 = bt.window_curves(r1, epochs, steps, window)
c2 = bt.window_curves(r2, epochs, steps, window)
wres = bt.compare_window_curves(c1, c2, tol)

def fmt(v):
    return "—" if v is None else format(v, ".3f")

print(f"  判据:epoch 级聚合(两轮 per-epoch median_stall 逐 epoch 相对差 ≤{tol:.0%}"
      f" 且 全轮 median_stall 相对差 ≤{tol:.0%});warmup {m1['warmup_steps']} 步"
      f"(epoch0 前,不计)")
print(f"  {'epoch':>5} {'轮1 median':>11} {'轮2 median':>11} {'相对差':>8}"
      f" {'轮1 mean':>10} {'轮2 mean':>10}")
for i, (s1, s2) in enumerate(zip(st1, st2)):
    row = res["per_epoch"][i] if i < len(res["per_epoch"]) else None
    d = "—" if row is None or row["rel_diff"] is None else format(row["rel_diff"], ".1%")
    print(f"  {i:>5} {fmt(s1['median_stall']):>11} {fmt(s2['median_stall']):>11} "
          f"{d:>8} {fmt(s1['mean_stall']):>10} {fmt(s2['mean_stall']):>10}")
rm = res["round_median"]
print(f"  逐 epoch max_rel_diff = "
      + ("—" if res["max_rel_diff"] is None else format(res["max_rel_diff"], ".1%")))
print(f"  全轮 median:轮1={fmt(rm['round_a'])} 轮2={fmt(rm['round_b'])} 相对差="
      + ("—" if rm["rel_diff"] is None else format(rm["rel_diff"], ".1%")))
if res.get("note"):
    print(f"  注:{res['note']}")
print(f"  verdict = {res['verdict']}({res['verdict_label']})")

# ---- 报告项打印(窗口曲线;不参与 verdict)----
print(f"  -- 报告项(不参与 verdict):窗口 {window} 步 stall 均值曲线逐点相对差 --")
print(f"     余数步 {steps - window * (steps // window)} 步不入窗")
for pe in wres["per_epoch"]:
    print(f"     epoch {pe['epoch']}:")
    print("     轮1  " + " ".join(fmt(v) for v in pe["curve_a"]))
    print("     轮2  " + " ".join(fmt(v) for v in pe["curve_b"]))
    print("     相对差 " + " ".join("—" if d is None else format(d, ".1%")
                                    for d in pe["rel_diffs"]))
    print("     max_rel_diff = " + ("—" if pe["max_rel_diff"] is None
                                    else format(pe["max_rel_diff"], ".1%")))
print(f"     总体 max_rel_diff = "
      + ("—" if wres["max_rel_diff"] is None else format(wres["max_rel_diff"], ".1%"))
      + "(报告项,不参与 verdict)")

per_epoch_rows = []
for i, (s1, s2) in enumerate(zip(st1, st2)):
    row = res["per_epoch"][i] if i < len(res["per_epoch"]) else None
    per_epoch_rows.append({
        "epoch": s1["epoch"],
        "median_a": s1["median_stall"], "median_b": s2["median_stall"],
        "mean_a": s1["mean_stall"], "mean_b": s2["mean_stall"],
        "rel_diff": row["rel_diff"] if row else None,
    })

out = {
    "track": "D", "kind": "verify", "ts": ts,
    "date": datetime.now().isoformat(timespec="seconds"),
    "rounds": 2, "verify_epochs": epochs,
    "steps_per_epoch": steps, "warmup_steps": int(m1["warmup_steps"]),
    "tol": tol, "seed": m1["seed"], "workers": m1["workers"],
    "metric": ("epoch 级聚合(判据):median_stall = 每步 wait_s/step_s 的中位数,"
               "epoch0 前 warmup 步不计;两轮 per-epoch median_stall 逐 epoch "
               "相对差与全轮 median_stall 相对差均 ≤ tol 判\"对上\""),
    "round_dirs": [str(Path(d1).resolve()), str(Path(d2).resolve())],
    "per_step": [str(f1.resolve()), str(f2.resolve())],
    "epoch_medians": {
        "per_epoch": per_epoch_rows,
        "max_rel_diff": res["max_rel_diff"],
        "full_round": {"median_a": rm["round_a"], "median_b": rm["round_b"],
                       "rel_diff": rm["rel_diff"]},
    },
    "window_curve": {
        "role": "report_only", "downgraded": True,
        "window_steps": window,
        "metric": "窗口内 stall 均值(wait_s/step_s);epoch0 前 warmup 步不计;余数步不入窗",
        "curves": {"round1": c1, "round2": c2},
        "per_epoch": wres["per_epoch"],
        "max_rel_diff": wres["max_rel_diff"],
        "n_points": wres["n_points"], "n_missing": wres["n_missing"],
        "downgrade_reason": (
            "主对话裁决(2026-10-13):窗口均值被 wait 分布重尾毛刺主导"
            "(同一冷启内相邻 epoch 逐点最大相对差已达 37%~56%,高于指标噪声底),"
            "原始逐点判据无判别力,降级为报告项;verdict 改由 epoch 级聚合对账决定。"),
        "note": "本报告项数值仍计算并留档,不参与 verdict。",
    },
    "verdict": res["verdict"], "verdict_label": res["verdict_label"],
    "tc_check": {"qdisc": qdisc, "via": tc_via},
}
Path(out_path).write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n")
print(f"已写 {out_path}")
sys.exit(0 if res["verdict"] == "PASS" else 1)
PYEOF
then
  RC=0
else
  RC=$?
fi

echo ""
if [ "$RC" = 0 ]; then
  echo "D 轨 verify:对上(两轮 per-epoch median_stall 聚合相对差 ≤$TOL)——装置可复现性成立"
else
  echo "D 轨 verify:对不上(epoch 级聚合超阈或缺测)——查装置后重跑(--selftest 可先自检判据)" >&2
fi
exit "$RC"

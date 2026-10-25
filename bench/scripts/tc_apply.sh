#!/usr/bin/env bash
# 参数化限速:对"源站出站方向"挂 tbf+netem(档位即实验自变量)。
#
# 挂点依据(现勘/探针实证,勿回退):必须挂"出站方"容器 netns 内 eth0 的 egress;
# docker0 桥主设备与宿主机 veth 的 host 侧 egress 均被实证否决(见
# probes/probe_a/tc_setup.sh 顶部记录与 docs/4 §5)。
#
# 用法:tc_apply.sh <rate> <delay> [burst] [--target minio|source-d]
#        [--jitter|--no-jitter] [--tier S|M|L] [--dry-run] [--self-test] [--help]
#   minio(默认):kind 内 MinIO Pod 的 eth0 egress —— 既有挂点,行为与改造前一致
#                (经节点容器 crictl inspectp 找 Pod sandbox netns 后 nsenter 挂点)。
#   source-d   :宿主机本地通路(v4 D 轨)—— 独立 nginx 源站容器(名取自 config.py 的
#                TRACK_D_SOURCE_CONTAINER,可被同名环境变量覆盖,便于用一次性容器验收)
#                的 eth0 egress;训练进程在宿主机、经该容器端口取数,限其出站带宽。
#   --dry-run  :只打印将执行的命令,不实际执行(不触碰集群与容器)。
#   --self-test:自检(不碰集群、不碰容器):burst 推导 / 抖动开关 / 旧调用形态解析。
#   --jitter   :开启 netem 抖动 → netem delay <delay> 10ms distribution normal
#                (均值仍取 <delay> 参数,抖动标准差固定 10ms;D31 只在 BW/r∈[1,2]
#                 的临界档启用,实测临界点只有 S 档@300mbit 与 M 档@1000mbit)。
#   --no-jitter:显式关闭抖动(默认即关闭)。
#   --tier     :按档位自动判定抖动,调 config.jitter_enabled(tier, rate_mbit);
#                显式 --jitter/--no-jitter 优先于 --tier。
#   缺省值:无档位参数时 minio 取 TC_RATE/TC_DELAY、source-d 取 TRACK_D_TC_RATE/
#           TRACK_D_TC_DELAY;latency 固定 50ms(与既有挂点一致)
#           —— 无参调用行为与改造前一致(make tc-up / cold_reset.sh 依赖此路径)。
#
# burst(D31):burst 与 rate 联动,取"≈0.1 秒的传输量"(config.burst_for_rate):
#   100mbit→1250k、300mbit→3750k、1000mbit→12500k。
#   显式给 burst 时以显式值为准(旧调用形态 tc_apply.sh 300mbit 10ms 1600k 不变);
#   不给 burst 时按 rate 推导;rate 解析不出 mbit 时退回 config.TC_BURST 并告警。
#   固定 1600k 跨档复用会让突发窗口从 128ms(100mbit)缩到 13ms(1000mbit),禁止。
#
# 幂等:先 del 再 add。Pod/容器重建后 netns 换新,qdisc 随之消失,需重挂。
# source-d 挂点需要 root:root 直跑 → sudo -n → sudo -S(bench/.sudo-pass,600);
# 三者皆不可用时回退特权 docker 容器(--net=container:<目标> --cap-add=NET_ADMIN,
# 镜像取本机已有的 TC_IMAGE,与第一步探针实测形态一致)。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
PY="${PY:-$REPO/.venv/bin/python}"   # 与 Makefile/探针先例统一(config.py 仅标准库,无第三方依赖)
CFG() { "$PY" "$ROOT/config.py" --sh "$1"; }
SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
JITTER_STDDEV="10ms"   # netem 抖动标准差固定 10ms(D31:抖动只在 BW/r∈[1,2] 的临界档启用)

# ---- config.py 里的现成公式:这里只调用,不重写(config.py 由别人拥有)----
CFG_BURST() {   # $1 = rate 的 mbit 数值 → "1250k"/"3750k"/"12500k"
  "$PY" -c 'import sys; sys.path.insert(0, sys.argv[1]); import config; print(config.burst_for_rate(float(sys.argv[2])))' "$ROOT" "$1"
}
CFG_JITTER() {  # $1 = tier, $2 = rate 的 mbit 数值 → 1/0
  "$PY" -c 'import sys; sys.path.insert(0, sys.argv[1]); import config; print(1 if config.jitter_enabled(sys.argv[2], float(sys.argv[3])) else 0)' "$ROOT" "$1" "$2"
}

# ---- rate → mbit 解析(只认带单位的写法,避免与 tc 自身语义对不上)----
rate_to_mbit() {
  local r="$1" n u m
  r="${r//[[:space:]]/}"
  n="${r%%[!0-9.]*}"
  [ -n "$n" ] || return 1
  u="${r#"$n"}"
  u="$(printf '%s' "$u" | tr 'A-Z' 'a-z')"
  case "$u" in
    mbit|mbps) m="$n" ;;
    kbit|kbps) m="$(awk -v x="$n" 'BEGIN{printf "%.6f", x/1000}')" ;;
    gbit|gbps) m="$(awk -v x="$n" 'BEGIN{printf "%.6f", x*1000}')" ;;
    bit|bps)   m="$(awk -v x="$n" 'BEGIN{printf "%.9f", x/1000000}')" ;;
    *) return 1 ;;
  esac
  printf '%s' "$m"
}

# ---- burst 推导(D31:burst ≈ 0.1 秒的传输量,与 rate 联动)----
derive_burst() {   # 用全局 RATE_MBIT;解析不出 mbit 时退回 config.TC_BURST
  if [ -n "$RATE_MBIT" ]; then CFG_BURST "$RATE_MBIT"; else CFG TC_BURST; fi
}

# ---- netem 参数(不开抖动 = delay <d>;开抖动 = delay <d> 10ms distribution normal)----
netem_args() {     # $1 = delay, $2 = on/off
  if [ "$2" = "on" ]; then printf 'delay %s %s distribution normal' "$1" "$JITTER_STDDEV"; else printf 'delay %s' "$1"; fi
}

# ---- 自检(不碰集群、不碰容器):burst 推导 / 抖动开关 / 旧调用形态解析 ----
self_test() {
  local fails=0 out
  pass() { echo "  [PASS] $1"; }
  fail() { echo "  [FAIL] $1" >&2; fails=$((fails + 1)); }
  expect_eq() {   # $1=说明 $2=期望 $3=实际
    if [ "$2" = "$3" ]; then pass "$1 = $3"; else fail "$1:期望 '$2',实际 '$3'"; fi
  }
  burst_of() {    # 从 dry-run 输出里取 burst 值
    printf '%s\n' "$1" | grep -o 'burst=[0-9a-z]*' | head -1 | cut -d= -f2
  }
  netem_of() {    # 从 dry-run 输出里取"生效的 netem 参数"
    printf '%s\n' "$1" | sed -n 's/^.*netem 生效参数=\(.*\)  (jitter=.*/\1/p' | head -1
  }

  echo "== (a) burst 推导(config.burst_for_rate:burst ≈ 0.1 秒的传输量)=="
  for pair in "100:1250k" "300:3750k" "1000:12500k"; do
    local mb="${pair%%:*}" want="${pair##*:}" got
    RATE_MBIT="$(rate_to_mbit "${mb}mbit" || true)"
    got="$(derive_burst "${mb}mbit")"
    expect_eq "burst(${mb}mbit)" "$want" "$got"
  done

  echo "== (b) 抖动开关确实改变 netem 参数=="
  expect_eq "netem(不开抖动)" "delay 20ms" "$(netem_args 20ms off)"
  expect_eq "netem(开抖动)"   "delay 20ms 10ms distribution normal" "$(netem_args 20ms on)"
  if [ "$(netem_args 20ms off)" != "$(netem_args 20ms on)" ]; then
    pass "两种形态确实不同"
  else
    fail "两种形态相同"
  fi

  echo "== (c) 旧调用形态仍被正确解析(子进程 --dry-run,不碰集群)=="
  out="$(bash "$SELF" 300mbit 10ms 1600k --dry-run 2>&1)" || fail "旧形态(显式 burst)执行失败"
  expect_eq "旧形态 burst(显式给值以显式为准)" "1600k" "$(burst_of "$out")"
  out="$(bash "$SELF" 300mbit 10ms --dry-run 2>&1)" || fail "旧形态(不给 burst)执行失败"
  expect_eq "旧形态 burst(缺省按 rate 推导)" "3750k" "$(burst_of "$out")"
  out="$(bash "$SELF" 300mbit 20ms --jitter --dry-run 2>&1)" || fail "--jitter 形态执行失败"
  expect_eq "--jitter 生效 netem" "delay 20ms 10ms distribution normal" "$(netem_of "$out")"
  out="$(bash "$SELF" 300mbit 20ms --dry-run 2>&1)" || fail "默认(不开抖动)形态执行失败"
  expect_eq "默认(不开抖动)生效 netem" "delay 20ms" "$(netem_of "$out")"
  out="$(bash "$SELF" --dry-run 2>&1)" || fail "无参调用形态执行失败"
  expect_eq "无参调用 burst(config 缺省 rate=100mbit)" "1250k" "$(burst_of "$out")"
  out="$(bash "$SELF" 100mbit 20ms --target source-d --dry-run 2>&1)" || fail "--target source-d 形态执行失败"
  pass "--target source-d 形态可解析"
  out="$(bash "$SELF" 100mbit 20ms --target minio --dry-run 2>&1)" || fail "--target minio 形态执行失败"
  pass "--target minio 形态可解析"

  if [ "$fails" -eq 0 ]; then
    echo "自检全部通过"
    return 0
  fi
  echo "自检失败:$fails 项未通过" >&2
  return 1
}

usage() {
  cat <<'EOF'
用法: tc_apply.sh <rate> <delay> [burst] [--target minio|source-d]
                 [--jitter|--no-jitter] [--tier S|M|L] [--dry-run] [--self-test]
  <rate>    限速值(如 100mbit / 300mbit);缺省:minio=TC_RATE,source-d=TRACK_D_TC_RATE
  <delay>   延迟值(如 20ms / 10ms);缺省:minio=TC_DELAY,source-d=TRACK_D_TC_DELAY
  [burst]   tbf 突发桶;缺省按 rate 推导(config.burst_for_rate,≈0.1 秒传输量:
            100mbit→1250k、300mbit→3750k、1000mbit→12500k);显式给值以显式为准;
            rate 解析不出 mbit 时退回 TC_BURST 并告警
  --target minio     默认;kind 内 MinIO Pod 的 eth0 egress(既有行为,不变)
  --target source-d  v4 D 轨:本地源站容器 TRACK_D_SOURCE_CONTAINER 的 eth0 egress
  --jitter           开 netem 抖动 → netem delay <delay> 10ms distribution normal
                     (均值=delay 参数,标准差固定 10ms;D31 只在 BW/r∈[1,2] 的临界档开)
  --no-jitter        关 netem 抖动(默认)→ netem delay <delay>
  --tier S|M|L       按档位判定抖动:config.jitter_enabled(tier, rate_mbit);
                     显式 --jitter/--no-jitter 优先
  --dry-run          只打印将执行的命令,不实际执行(带/不带抖动的两种形态都打印)
  --self-test        自检:burst 推导 / 抖动开关 / 旧调用形态解析(不碰集群与容器)
  --help             本用法
示例:
  tc_apply.sh 100mbit 20ms                    # 兼容既有调用(无抖动)→ MinIO
  tc_apply.sh 300mbit 10ms 1600k              # 兼容既有调用(显式 burst,值不变)→ MinIO
  tc_apply.sh 300mbit 20ms --jitter           # 临界档:netem delay 20ms 10ms distribution normal
  tc_apply.sh 100mbit 20ms --target source-d  # D 轨本地源站
EOF
}

# ---- 参数解析:位置参数(rate delay burst)+ 选项可任意顺序;既有调用形态全部兼容 ----
TARGET="minio"; DRY_RUN=0; POS=()
JITTER_MODE="auto"   # auto(按 --tier 判定)/on(--jitter)/off(--no-jitter)
TIER=""              # 可选:S|M|L,供 config.jitter_enabled 判定
SELF_TEST=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h)  usage; exit 0 ;;
    --dry-run)  DRY_RUN=1; shift ;;
    --jitter)   JITTER_MODE="on"; shift ;;
    --no-jitter) JITTER_MODE="off"; shift ;;
    --tier)
      [ "$#" -ge 2 ] || { echo "错误:--tier 缺值(应为 S|M|L)" >&2; usage >&2; exit 2; }
      TIER="$2"; shift 2 ;;
    --tier=*)   TIER="${1#--tier=}"; shift ;;
    --self-test) SELF_TEST=1; shift ;;
    --target)
      [ "$#" -ge 2 ] || { echo "错误:--target 缺值(应为 minio 或 source-d)" >&2; usage >&2; exit 2; }
      TARGET="$2"; shift 2 ;;
    --target=*) TARGET="${1#--target=}"; shift ;;
    -*) echo "错误:未知选项 $1" >&2; usage >&2; exit 2 ;;
    *)  POS+=("$1"); shift ;;
  esac
done
case "$TARGET" in
  minio|source-d) ;;
  *) echo "错误:--target 只支持 minio|source-d(收到 '$TARGET')" >&2; usage >&2; exit 2 ;;
esac
[ "${#POS[@]}" -le 3 ] || { echo "错误:位置参数过多(最多 <rate> <delay> <burst>)" >&2; usage >&2; exit 2; }
case "$TIER" in
  ""|S|M|L) ;;
  *) echo "错误:--tier 只支持 S|M|L(收到 '$TIER')" >&2; usage >&2; exit 2 ;;
esac

# ---- 自检入口(不碰集群、不碰容器)----
if [ "$SELF_TEST" = 1 ]; then self_test; exit $?; fi

# ---- 档位解析(无位置参数时按目标取 config 缺省;minio 分支与改造前逐字一致)----
if [ "$TARGET" = minio ]; then
  RATE="${POS[0]:-$(CFG TC_RATE)}"
  DELAY="${POS[1]:-$(CFG TC_DELAY)}"
else
  RATE="${POS[0]:-$(CFG TRACK_D_TC_RATE)}"
  DELAY="${POS[1]:-$(CFG TRACK_D_TC_DELAY)}"
fi

# ---- burst(D31):显式值优先;缺省按 rate 推导(=0.1 秒的传输量)----
RATE_MBIT="$(rate_to_mbit "$RATE" || true)"
if [ "${#POS[@]}" -ge 3 ]; then
  BURST="${POS[2]}"
  BURST_SRC="显式给定(位置参数第 3 个)"
elif [ -n "$RATE_MBIT" ]; then
  BURST="$(derive_burst "$RATE")"
  BURST_SRC="按 rate 推导:${RATE} → ${RATE_MBIT} mbit → burst=${BURST}(config.burst_for_rate,≈0.1 秒传输量)"
else
  BURST="$(CFG TC_BURST)"
  BURST_SRC="rate='${RATE}' 解析不出 mbit,退回 config.TC_BURST"
  echo "告警:rate='$RATE' 解析不出 mbit,burst 退回 config.TC_BURST($BURST);请用 100mbit/300mbit/1000mbit 这类带单位写法" >&2
fi

if [ "${#POS[@]}" -eq 0 ]; then
  echo "用法: tc_apply.sh <rate> <delay> [burst] [--target minio|source-d](未给档位参数,按 config 缺省执行:$TARGET rate=$RATE delay=$DELAY burst=$BURST;完整帮助 --help)"
fi
for v in "$RATE" "$DELAY" "$BURST"; do
  case "$v" in
    ''|*[!A-Za-z0-9.]*) echo "错误:档位值 '$v' 不合法(只允许字母/数字/小数点,如 100mbit、20ms、1600k)" >&2; exit 2 ;;
  esac
done

# ---- netem 抖动(D31):只在 BW/r∈[1,2] 的临界档开;显式开关优先于 --tier 判定 ----
JITTER_STATE="off"; JITTER_REASON="默认关闭"
if [ "$JITTER_MODE" = "on" ]; then
  JITTER_STATE="on"; JITTER_REASON="显式 --jitter"
elif [ "$JITTER_MODE" = "off" ]; then
  JITTER_REASON="显式 --no-jitter"
elif [ -n "$TIER" ]; then
  if [ -n "$RATE_MBIT" ]; then
    if [ "$(CFG_JITTER "$TIER" "$RATE_MBIT")" = "1" ]; then
      JITTER_STATE="on"
      JITTER_REASON="按档位判定:config.jitter_enabled($TIER, ${RATE_MBIT})=True(临界档 BW/r∈[1,2])"
    else
      JITTER_REASON="按档位判定:config.jitter_enabled($TIER, ${RATE_MBIT})=False(非临界档)"
    fi
  else
    JITTER_REASON="给了 --tier $TIER,但 rate='$RATE' 解析不出 mbit,无法判定抖动"
  fi
fi
NETEM_PLAIN="$(netem_args "$DELAY" off)"
NETEM_JITTER="$(netem_args "$DELAY" on)"
if [ "$JITTER_STATE" = "on" ]; then NETEM_ARGS="$NETEM_JITTER"; else NETEM_ARGS="$NETEM_PLAIN"; fi

case "$TARGET" in
# ==================== minio(既有挂点,命令形态与改造前逐字一致)====================
minio)
  if [ "$DRY_RUN" = 1 ]; then
    echo "[dry-run] target=minio rate=$RATE delay=$DELAY latency=50ms"
    echo "[dry-run]   burst=$BURST  ($BURST_SRC)"
    echo "[dry-run]   netem 生效参数=$NETEM_ARGS  (jitter=$JITTER_STATE;$JITTER_REASON)"
    cat <<EOF
[dry-run] 将执行(kind 内 MinIO Pod eth0 egress;本次不实际执行):
[dry-run]   kubectl -n minio get pod -l app=minio -o jsonpath='{.items[0].metadata.name}'   # → POD
[dry-run]   kubectl -n minio get pod "\$POD" -o jsonpath='{.spec.nodeName}'                  # → NODE
[dry-run]   docker exec -i "\$NODE" bash -s -- $RATE $DELAY $BURST "\$POD" '$NETEM_ARGS' <<'EOS'
[dry-run]     CID=\$(crictl pods --state Ready --name "\$POD" -q | head -1)
[dry-run]     NETNS=\$(crictl inspectp "\$CID" | grep -oE '"/var/run/netns/[^"]+"' | head -1 | tr -d '"')
[dry-run]     nsenter --net="\$NETNS" tc qdisc del dev eth0 root 2>/dev/null || true
[dry-run]     nsenter --net="\$NETNS" tc qdisc add dev eth0 root handle 1: tbf rate $RATE burst $BURST latency 50ms
[dry-run]     nsenter --net="\$NETNS" tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM_ARGS
[dry-run]     nsenter --net="\$NETNS" tc qdisc show dev eth0
[dry-run]   EOS
[dry-run] 对照:两种 netem 形态(上面那行是实际生效的):
[dry-run]   无抖动(--no-jitter/默认): nsenter --net="\$NETNS" tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM_PLAIN
[dry-run]   有抖动(--jitter):        nsenter --net="\$NETNS" tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM_JITTER
EOF
    exit 0
  fi
  POD=$(kubectl -n minio get pod -l app=minio -o jsonpath='{.items[0].metadata.name}')
  NODE=$(kubectl -n minio get pod "$POD" -o jsonpath='{.spec.nodeName}')
  echo "MinIO Pod=$POD 节点=$NODE 档位:rate=$RATE delay=$DELAY"
  echo "burst=$BURST  (推导依据:$BURST_SRC)"
  echo "netem=$NETEM_ARGS  (jitter=$JITTER_STATE;$JITTER_REASON)"

  # 节点容器内:定位 Pod sandbox 的 netns,给其 eth0 挂 qdisc
  docker exec -i "$NODE" bash -s -- "$RATE" "$DELAY" "$BURST" "$POD" "$NETEM_ARGS" <<'EOF'
set -euo pipefail
RATE="$1"; DELAY="$2"; BURST="$3"; POD="$4"; NETEM="$5"
CID=$(crictl pods --state Ready --name "$POD" -q | head -1)
NETNS=$(crictl inspectp "$CID" | grep -oE '"/var/run/netns/[^"]+"' | head -1 | tr -d '"')
[ -n "$NETNS" ] || { echo "找不到 Pod sandbox netns(CID=$CID)"; exit 1; }
echo "netns=$NETNS"
nsenter --net="$NETNS" tc qdisc del dev eth0 root 2>/dev/null || true
nsenter --net="$NETNS" tc qdisc add dev eth0 root handle 1: \
  tbf rate "$RATE" burst "$BURST" latency 50ms
nsenter --net="$NETNS" tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM
echo "--- 已挂载 ---"
nsenter --net="$NETNS" tc qdisc show dev eth0
EOF
  echo "提示:换档位直接重跑本脚本;拆除用 tc_teardown.sh"
  ;;
# ================= source-d(v4 D 轨:宿主机本地源站容器挂点)=================
source-d)
  NAME="${TRACK_D_SOURCE_CONTAINER:-$(CFG TRACK_D_SOURCE_CONTAINER)}"
  TC_IMAGE="${TC_IMAGE:-kindest/node:v1.28.15}"   # 本机已有(第一步探针回退实测形态),内含 /usr/sbin/tc
  if [ "$DRY_RUN" = 1 ]; then
    echo "[dry-run] target=source-d container=$NAME rate=$RATE delay=$DELAY latency=50ms"
    echo "[dry-run]   burst=$BURST  ($BURST_SRC)"
    echo "[dry-run]   netem 生效参数=$NETEM_ARGS  (jitter=$JITTER_STATE;$JITTER_REASON)"
    cat <<EOF
[dry-run] 将执行(本地源站容器 eth0 egress;本次不实际执行):
[dry-run]   docker inspect -f '{{.State.Pid}}' $NAME   # → PID
[dry-run]   # 首选通道 root 直跑 / sudo -n / sudo -S(.sudo-pass):
[dry-run]   nsenter -t \$PID -n tc qdisc del dev eth0 root 2>/dev/null || true
[dry-run]   nsenter -t \$PID -n tc qdisc add dev eth0 root handle 1: tbf rate $RATE burst $BURST latency 50ms
[dry-run]   nsenter -t \$PID -n tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM_ARGS
[dry-run]   nsenter -t \$PID -n tc qdisc show dev eth0
[dry-run]   # 回退(无 sudo 权限):特权容器共享目标 netns,镜像 $TC_IMAGE
[dry-run]   docker run --rm --net=container:$NAME --cap-add=NET_ADMIN --entrypoint /bin/sh $TC_IMAGE -c \\
[dry-run]     'tc qdisc del dev eth0 root 2>/dev/null || true; tc qdisc add dev eth0 root handle 1: tbf rate $RATE burst $BURST latency 50ms && tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM_ARGS && tc qdisc show dev eth0'
[dry-run] 对照:两种 netem 形态(上面那行是实际生效的):
[dry-run]   无抖动(--no-jitter/默认): nsenter -t \$PID -n tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM_PLAIN
[dry-run]   有抖动(--jitter):        nsenter -t \$PID -n tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM_JITTER
EOF
    exit 0
  fi
  PID=$(docker inspect -f '{{.State.Pid}}' "$NAME" 2>/dev/null) \
    || { echo "错误:容器 $NAME 不存在(先启动 D 轨源站;一次性容器测试可用环境变量 TRACK_D_SOURCE_CONTAINER 覆盖容器名)" >&2; exit 1; }
  { [ -n "$PID" ] && [ "$PID" != 0 ]; } || { echo "错误:容器 $NAME 不在运行(PID=$PID)" >&2; exit 1; }

  # ---- root 通道探测:root 直跑 > sudo -n > sudo -S(.sudo-pass);皆不可用则 docker 回退 ----
  ROOT_VIA=""
  if [ "$(id -u)" -eq 0 ]; then ROOT_VIA="root"
  elif sudo -n true 2>/dev/null; then ROOT_VIA="sudo -n"
  elif [ -r "$ROOT/.sudo-pass" ] && printf '%s\n' "$(cat "$ROOT/.sudo-pass")" | sudo -S -p '' true 2>/dev/null; then
    ROOT_VIA="sudo -S(.sudo-pass)"
  fi
  run_root() {
    case "$ROOT_VIA" in
      root)      "$@" ;;
      "sudo -n") sudo -n "$@" ;;
      *)         printf '%s\n' "$(cat "$ROOT/.sudo-pass")" | sudo -S -p '' "$@" ;;
    esac
  }
  echo "SourceD 容器=$NAME 档位:rate=$RATE delay=$DELAY"
  echo "burst=$BURST  (推导依据:$BURST_SRC)"
  echo "netem=$NETEM_ARGS  (jitter=$JITTER_STATE;$JITTER_REASON)"
  if [ -n "$ROOT_VIA" ]; then
    run_root nsenter -t "$PID" -n tc qdisc del dev eth0 root 2>/dev/null || true
    if run_root nsenter -t "$PID" -n tc qdisc add dev eth0 root handle 1: \
         tbf rate "$RATE" burst "$BURST" latency 50ms \
       && run_root nsenter -t "$PID" -n tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM_ARGS; then
      echo "--- 已挂载(root 通道:$ROOT_VIA,nsenter 容器 netns)---"
      run_root nsenter -t "$PID" -n tc qdisc show dev eth0
    else
      echo "错误:tc 挂载失败(root 通道:$ROOT_VIA)" >&2; exit 1
    fi
  else
    docker image inspect "$TC_IMAGE" >/dev/null 2>&1 \
      || { echo "错误:本机无回退镜像 $TC_IMAGE(用 TC_IMAGE= 指定本地已有镜像)" >&2; exit 1; }
    echo "未取得 root 通道 → 回退特权 docker 容器(镜像 $TC_IMAGE,共享容器 $NAME 的 netns)"
    docker run --rm --net="container:$NAME" --cap-add=NET_ADMIN --entrypoint /bin/sh "$TC_IMAGE" -c "
      tc qdisc del dev eth0 root 2>/dev/null || true
      tc qdisc add dev eth0 root handle 1: tbf rate $RATE burst $BURST latency 50ms &&
      tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM_ARGS &&
      echo '--- 已挂载(特权 docker 回退)---' && tc qdisc show dev eth0" \
      || { echo "错误:docker 回退挂载失败" >&2; exit 1; }
  fi
  echo "提示:换档位直接重跑本脚本;拆除用 tc_teardown.sh --target source-d"
  ;;
esac

#!/usr/bin/env bash
# 参数化限速:对"源站出站方向"挂 tbf+netem(档位即实验自变量)。
#
# 挂点依据(现勘/探针实证,勿回退):必须挂"出站方"容器 netns 内 eth0 的 egress;
# docker0 桥主设备与宿主机 veth 的 host 侧 egress 均被实证否决(见
# probes/probe_a/tc_setup.sh 顶部记录与 docs/4.1 的回源带宽整形一节)。
#
# 用法:tc_apply.sh <rate> <delay> [burst] [--target minio|source-d]
#        [--per-conn <rate>] [--loss <pct>] [--dry-run] [--self-test] [--help]
#   minio(默认):kind 内 MinIO Pod 的 eth0 egress —— 既有挂点,行为与改造前一致
#                (经节点容器 crictl inspectp 找 Pod sandbox netns 后 nsenter 挂点)。
#   source-d   :宿主机本地通路(v4 train)—— 独立 nginx 源站容器(名取自 config.py 的
#                TRAIN_SOURCE_CONTAINER,可被同名环境变量覆盖,便于用一次性容器验收)
#                的 eth0 egress;训练进程在宿主机、经该容器端口取数,限其出站带宽。
#   --per-conn :单条 TCP 连接的速率上限(如 32mbit)。给出时 qdisc 形态多一层:
#                根 tbf 之下挂 fq,fq 的 maxrate 给这个上限,netem 挂在 fq 的 leaf
#                class 上。不给出时形态是根 tbf 直接挂 netem,与改造前逐字一致。
#   --loss     :netem 丢包率(0 到 100 的小数,1 表示 1%)。缺省 0,不出现 loss 子句。
#   --dry-run  :只打印将执行的命令,不实际执行(不触碰集群与容器)。
#   --self-test:自检(不碰集群、不碰容器):burst 推导 / netem 形态 / 单流上限形态 /
#                丢包参数拼装 / 旧调用形态解析。
#   netem 只挂固定 delay,不加抖动:随机时延逐包重抽会把包序打乱,300mbit 档实测把吞吐
#   从 35.6MB/s 打到 <0.6MB/s(TCPSackRecovery 占已发段的 15%),详见 docs/4.1 的回源带宽整形一节 的限速口径。
#   缺省值:无档位参数时 minio 取 TC_RATE/TC_DELAY、source-d 取 TRAIN_TC_RATE/
#           TRAIN_TC_DELAY;latency 固定 50ms(与既有挂点一致)
#           —— 无参调用行为与改造前一致(make tc-up / cold_reset.sh 依赖此路径)。
#
# burst(D31):burst 与 rate 联动,取"约 0.1 秒的传输量"(config.burst_for_rate):
#   100mbit→1250k、300mbit→3750k、1000mbit→12500k。
#   显式给 burst 时以显式值为准(旧调用形态 tc_apply.sh 300mbit 10ms 1600k 不变);
#   不给 burst 时按 rate 推导;rate 解析不出 mbit 时退回 config.TC_BURST 并告警。
#   固定 1600k 跨档复用会让突发窗口从 131ms(100mbit)缩到 13ms(1000mbit),禁止。
#
# burst 的两个单位坑(2026-11-03 在一次性容器内实测,iproute2-6.1.0):
#   · tc 把 k 解析成 1024:1250k 是 1 280 000 字节,对 @100 的 tbf 速率 12.5 MB/s
#     是 102 ms 突发窗口(按 1000 算会读成 100 ms);3750k 是 3 840 000 字节。
#   · 命令行上的 latency 50ms 不覆盖 burst:同一 qdisc 把 latency 换成 5ms/50ms/
#     500ms,回读的 burst 恒为 1250Kb;latency 只定队列长度上限。
#   回读形态有两种:`tc qdisc show` 打印 burst 1250Kb( KiB)或 burst 3839962b(字节,
#   iproute2 按 32 位字取整会掉几十字节零头)。make bench-doctor 折算成字节后与本档
#   期望值比,容 ±1%。
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

# ---- config.py 里的现成公式:这里只调用,不重写(config.py 由别人拥有)----
CFG_BURST() {   # $1 = rate 的 mbit 数值 → "1250k"/"3750k"/"12500k"
  "$PY" -c 'import sys; sys.path.insert(0, sys.argv[1]); import config; print(config.burst_for_rate(float(sys.argv[2])))' "$ROOT" "$1"
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

# ---- netem 参数:固定时延 + 可选丢包 ----
netem_args() {     # $1 = delay, $2 = loss 百分比(可空,0 表示不丢包)
  local loss="${2:-}"
  if [ -n "$loss" ] && [ "$loss" != "0" ]; then
    printf 'delay %s loss %s%%' "$1" "$loss"
  else
    printf 'delay %s' "$1"
  fi
}

# ---- 自检(不碰集群、不碰容器):burst 推导 / netem 固定时延形态 / 调用形态解析 ----
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
    printf '%s\n' "$1" | sed -n 's/^.*netem 生效参数=\(.*\)$/\1/p' | head -1
  }
  shape_of() {    # 从 dry-run 输出里取 qdisc 形态
    printf '%s\n' "$1" | sed -n 's/^.*qdisc 形态=\(.*\)$/\1/p' | head -1
  }

  echo "== (a) burst 推导(config.burst_for_rate:burst ≈ 0.1 秒的传输量)=="
  for pair in "100:1250k" "300:3750k" "1000:12500k"; do
    local mb="${pair%%:*}" want="${pair##*:}" got
    RATE_MBIT="$(rate_to_mbit "${mb}mbit" || true)"
    got="$(derive_burst "${mb}mbit")"
    expect_eq "burst(${mb}mbit)" "$want" "$got"
  done

  echo "== (b) netem 只有固定时延;随机时延形态与抖动开关都不存在 =="
  expect_eq "netem 形态" "delay 20ms" "$(netem_args 20ms)"
  out="$(bash "$SELF" 300mbit 20ms --dry-run 2>&1)" || fail "默认形态执行失败"
  expect_eq "dry-run 生效 netem" "delay 20ms" "$(netem_of "$out")"
  if printf '%s\n' "$out" | grep -q 'distribution normal'; then
    fail "生效命令里出现随机时延(distribution normal)"
  else
    pass "生效命令里没有随机时延"
  fi
  if bash "$SELF" 300mbit 20ms --jitter --dry-run >/dev/null 2>&1; then
    fail "--jitter 仍被接受(抖动开关没撤净)"
  else
    pass "--jitter 被拒绝"
  fi
  if bash "$SELF" 300mbit 20ms --tier S --dry-run >/dev/null 2>&1; then
    fail "--tier 仍被接受(按档位判定抖动没撤净)"
  else
    pass "--tier 被拒绝"
  fi

  echo "== (c) netem 丢包形态与单流上限形态 =="
  expect_eq "netem 形态(无丢包)" "delay 20ms" "$(netem_args 20ms 0)"
  expect_eq "netem 形态(带丢包)" "delay 200ms loss 1%" "$(netem_args 200ms 1)"
  out="$(bash "$SELF" 1000mbit 20ms --dry-run 2>&1)" || fail "默认形态执行失败"
  expect_eq "缺省单流上限时形态" "tbf → netem" "$(shape_of "$out")"
  if printf '%s\n' "$out" | grep -q 'fq maxrate'; then
    fail "缺省时出现了 fq maxrate(单流上限不该默认开)"
  else
    pass "缺省时没有 fq maxrate"
  fi
  out="$(bash "$SELF" 1000mbit 20ms --per-conn 32mbit --dry-run 2>&1)" || fail "带单流上限形态执行失败"
  expect_eq "带单流上限时形态" "tbf → fq(maxrate=32mbit) → netem" "$(shape_of "$out")"
  if printf '%s\n' "$out" | grep -q 'fq maxrate 32mbit'; then
    pass "带单流上限时挂 fq maxrate"
  else
    fail "带单流上限时没有 fq maxrate 命令"
  fi
  if printf '%s\n' "$out" | grep -q 'parent 10:1 handle 20: netem'; then
    pass "netem 挂在 fq 的 leaf class 上"
  else
    fail "netem 没有挂到 fq 的 leaf class"
  fi
  out="$(bash "$SELF" 1000mbit 200ms --loss 1 --dry-run 2>&1)" || fail "带丢包形态执行失败"
  expect_eq "带丢包时 netem 参数" "delay 200ms loss 1%" "$(netem_of "$out")"

  echo "== (d) 非法值拒绝 =="
  if bash "$SELF" 1000mbit 20ms --per-conn abc --dry-run >/dev/null 2>&1; then
    fail "--per-conn abc 仍被接受"
  else
    pass "--per-conn abc 被拒绝"
  fi
  if bash "$SELF" 1000mbit 20ms --per-conn 0mbit --dry-run >/dev/null 2>&1; then
    fail "--per-conn 0mbit 仍被接受(零上限无意义)"
  else
    pass "--per-conn 0mbit 被拒绝"
  fi
  if bash "$SELF" 1000mbit 20ms --loss 101 --dry-run >/dev/null 2>&1; then
    fail "--loss 101 仍被接受(超出 100)"
  else
    pass "--loss 101 被拒绝"
  fi
  if bash "$SELF" 1000mbit 20ms --loss abc --dry-run >/dev/null 2>&1; then
    fail "--loss abc 仍被接受"
  else
    pass "--loss abc 被拒绝"
  fi

  echo "== (e) 旧调用形态仍被正确解析(子进程 --dry-run,不碰集群)=="
  out="$(bash "$SELF" 300mbit 10ms 1600k --dry-run 2>&1)" || fail "旧形态(显式 burst)执行失败"
  expect_eq "旧形态 burst(显式给值以显式为准)" "1600k" "$(burst_of "$out")"
  out="$(bash "$SELF" 300mbit 10ms --dry-run 2>&1)" || fail "旧形态(不给 burst)执行失败"
  expect_eq "旧形态 burst(缺省按 rate 推导)" "3750k" "$(burst_of "$out")"
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
                 [--per-conn <rate>] [--loss <pct>] [--dry-run] [--self-test]
  <rate>    限速值(如 100mbit / 300mbit);缺省:minio=TC_RATE,source-d=TRAIN_TC_RATE
  <delay>   延迟值(如 20ms / 10ms);缺省:minio=TC_DELAY,source-d=TRAIN_TC_DELAY
  [burst]   tbf 突发桶;缺省按 rate 推导(config.burst_for_rate,约 0.1 秒传输量:
            100mbit→1250k、300mbit→3750k、1000mbit→12500k;k 按 1024 解析);
            显式给值以显式为准;rate 解析不出 mbit 时退回 TC_BURST 并告警
  --target minio     默认;kind 内 MinIO Pod 的 eth0 egress(既有行为,不变)
  --target source-d  v4 train:本地源站容器 TRAIN_SOURCE_CONTAINER 的 eth0 egress
  --per-conn <rate>  单条 TCP 连接的速率上限(如 32mbit)。给出时 qdisc 形态是
                     根 tbf → fq(maxrate) → netem;不给出时是 根 tbf → netem
  --loss <pct>       netem 丢包率(0 到 100 的小数,1 表示 1%);缺省 0
  --dry-run          只打印将执行的命令,不实际执行
  --self-test        自检:burst 推导 / netem 形态 / 单流上限形态 / 丢包拼装 /
                     旧调用形态解析(不碰集群与容器)
  --help             本用法
示例:
  tc_apply.sh 100mbit 20ms                    # 兼容既有调用 → MinIO
  tc_apply.sh 300mbit 10ms 1600k              # 兼容既有调用(显式 burst,值不变)→ MinIO
  tc_apply.sh 100mbit 20ms --target source-d  # train 本地源站
  tc_apply.sh 1000mbit 20ms --per-conn 32mbit # 单流上限 32mbit,聚合带宽不限
EOF
}

# ---- 参数解析:位置参数(rate delay burst)+ 选项可任意顺序;既有调用形态全部兼容 ----
TARGET="minio"; DRY_RUN=0; POS=()
SELF_TEST=0
PER_CONN=""; LOSS="0"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h)  usage; exit 0 ;;
    --dry-run)  DRY_RUN=1; shift ;;
    --self-test) SELF_TEST=1; shift ;;
    --target)
      [ "$#" -ge 2 ] || { echo "错误:--target 缺值(应为 minio 或 source-d)" >&2; usage >&2; exit 2; }
      TARGET="$2"; shift 2 ;;
    --target=*) TARGET="${1#--target=}"; shift ;;
    --per-conn)
      [ "$#" -ge 2 ] || { echo "错误:--per-conn 缺值(应形如 32mbit)" >&2; usage >&2; exit 2; }
      PER_CONN="$2"; shift 2 ;;
    --per-conn=*) PER_CONN="${1#--per-conn=}"; shift ;;
    --loss)
      [ "$#" -ge 2 ] || { echo "错误:--loss 缺值(应为 0 到 100 的小数)" >&2; usage >&2; exit 2; }
      LOSS="$2"; shift 2 ;;
    --loss=*) LOSS="${1#--loss=}"; shift ;;
    -*) echo "错误:未知选项 $1" >&2; usage >&2; exit 2 ;;
    *)  POS+=("$1"); shift ;;
  esac
done
case "$TARGET" in
  minio|source-d) ;;
  *) echo "错误:--target 只支持 minio|source-d(收到 '$TARGET')" >&2; usage >&2; exit 2 ;;
esac
[ "${#POS[@]}" -le 3 ] || { echo "错误:位置参数过多(最多 <rate> <delay> <burst>)" >&2; usage >&2; exit 2; }

# ---- 单流上限与丢包率的取值校验 ----
if [ -n "$PER_CONN" ]; then
  PC_MBIT="$(rate_to_mbit "$PER_CONN" || true)"
  if [ -z "$PC_MBIT" ] || [ "$PC_MBIT" = "0" ]; then
    echo "错误:--per-conn '$PER_CONN' 解析不出 mbit 数值(应形如 32mbit、4mbit)" >&2; exit 2
  fi
fi
case "$LOSS" in
  ''|*[!0-9.]*) echo "错误:--loss '$LOSS' 不合法(应为 0 到 100 的小数)" >&2; exit 2 ;;
esac
if awk -v l="$LOSS" 'BEGIN{exit !(l>=0 && l<=100)}'; then :; else
  echo "错误:--loss '$LOSS' 超出 0 到 100" >&2; exit 2
fi

# ---- 自检入口(不碰集群、不碰容器)----
if [ "$SELF_TEST" = 1 ]; then self_test; exit $?; fi

# ---- 档位解析(无位置参数时按目标取 config 缺省;minio 分支与改造前逐字一致)----
if [ "$TARGET" = minio ]; then
  RATE="${POS[0]:-$(CFG TC_RATE)}"
  DELAY="${POS[1]:-$(CFG TC_DELAY)}"
else
  RATE="${POS[0]:-$(CFG TRAIN_TC_RATE)}"
  DELAY="${POS[1]:-$(CFG TRAIN_TC_DELAY)}"
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

# ---- netem 形态:固定时延 + 可选丢包 ----
NETEM_ARGS="$(netem_args "$DELAY" "$LOSS")"

# ---- qdisc 形态:单流不限时是 根 tbf → netem;给出单流上限时是 根 tbf → fq(maxrate) → netem ----
if [ -n "$PER_CONN" ]; then
  QDISC_SHAPE="tbf → fq(maxrate=$PER_CONN) → netem"
  FQ_LINE='[dry-run]     nsenter --net="$NETNS" tc qdisc add dev eth0 parent 1:1 handle 10: fq maxrate '"$PER_CONN"
  NETEM_LINE='[dry-run]     nsenter --net="$NETNS" tc qdisc add dev eth0 parent 10:1 handle 20: netem '"$NETEM_ARGS"
  FQ_LINE_SD='[dry-run]   nsenter -t $PID -n tc qdisc add dev eth0 parent 1:1 handle 10: fq maxrate '"$PER_CONN"
  NETEM_LINE_SD='[dry-run]   nsenter -t $PID -n tc qdisc add dev eth0 parent 10:1 handle 20: netem '"$NETEM_ARGS"
else
  QDISC_SHAPE="tbf → netem"
  FQ_LINE=""
  NETEM_LINE='[dry-run]     nsenter --net="$NETNS" tc qdisc add dev eth0 parent 1:1 handle 10: netem '"$NETEM_ARGS"
  FQ_LINE_SD=""
  NETEM_LINE_SD='[dry-run]   nsenter -t $PID -n tc qdisc add dev eth0 parent 1:1 handle 10: netem '"$NETEM_ARGS"
fi

case "$TARGET" in
# ==================== minio(既有挂点,命令形态与改造前逐字一致)====================
minio)
  if [ "$DRY_RUN" = 1 ]; then
    echo "[dry-run] target=minio rate=$RATE delay=$DELAY latency=50ms"
    echo "[dry-run]   burst=$BURST  ($BURST_SRC)"
    echo "[dry-run]   qdisc 形态=$QDISC_SHAPE"
    echo "[dry-run]   netem 生效参数=$NETEM_ARGS"
    cat <<EOF
[dry-run] 将执行(kind 内 MinIO Pod eth0 egress;本次不实际执行):
[dry-run]   kubectl -n minio get pod -l app=minio -o jsonpath='{.items[0].metadata.name}'   # → POD
[dry-run]   kubectl -n minio get pod "\$POD" -o jsonpath='{.spec.nodeName}'                  # → NODE
[dry-run]   docker exec -i "\$NODE" bash -s -- $RATE $DELAY $BURST "\$POD" '$NETEM_ARGS' '$PER_CONN' <<'EOS'
[dry-run]     CID=\$(crictl pods --state Ready --name "\$POD" -q | head -1)
[dry-run]     NETNS=\$(crictl inspectp "\$CID" | grep -oE '"/var/run/netns/[^"]+"' | head -1 | tr -d '"')
[dry-run]     nsenter --net="\$NETNS" tc qdisc del dev eth0 root 2>/dev/null || true
[dry-run]     nsenter --net="\$NETNS" tc qdisc add dev eth0 root handle 1: tbf rate $RATE burst $BURST latency 50ms
$FQ_LINE
$NETEM_LINE
[dry-run]     nsenter --net="\$NETNS" tc qdisc show dev eth0
[dry-run]   EOS
EOF
    exit 0
  fi
  POD=$(kubectl -n minio get pod -l app=minio -o jsonpath='{.items[0].metadata.name}')
  NODE=$(kubectl -n minio get pod "$POD" -o jsonpath='{.spec.nodeName}')
  echo "MinIO Pod=$POD 节点=$NODE 档位:rate=$RATE delay=$DELAY"
  echo "burst=$BURST  (推导依据:$BURST_SRC)"
  echo "qdisc 形态=$QDISC_SHAPE"
  echo "netem=$NETEM_ARGS"

  # 节点容器内:定位 Pod sandbox 的 netns,给其 eth0 挂 qdisc
  docker exec -i "$NODE" bash -s -- "$RATE" "$DELAY" "$BURST" "$POD" "$NETEM_ARGS" "$PER_CONN" <<'EOF'
set -euo pipefail
RATE="$1"; DELAY="$2"; BURST="$3"; POD="$4"; NETEM="$5"; PER_CONN="$6"
CID=$(crictl pods --state Ready --name "$POD" -q | head -1)
NETNS=$(crictl inspectp "$CID" | grep -oE '"/var/run/netns/[^"]+"' | head -1 | tr -d '"')
[ -n "$NETNS" ] || { echo "找不到 Pod sandbox netns(CID=$CID)"; exit 1; }
echo "netns=$NETNS"
nsenter --net="$NETNS" tc qdisc del dev eth0 root 2>/dev/null || true
nsenter --net="$NETNS" tc qdisc add dev eth0 root handle 1: \
  tbf rate "$RATE" burst "$BURST" latency 50ms
if [ -n "$PER_CONN" ]; then
  # sch_fq 可能是可加载模块,内建于内核时 modprobe 失败可忽略;下面挂 fq 失败才报错
  modprobe sch_fq 2>/dev/null || true
  nsenter --net="$NETNS" tc qdisc add dev eth0 parent 1:1 handle 10: fq maxrate "$PER_CONN"
  nsenter --net="$NETNS" tc qdisc add dev eth0 parent 10:1 handle 20: netem $NETEM
else
  nsenter --net="$NETNS" tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM
fi
echo "--- 已挂载 ---"
nsenter --net="$NETNS" tc qdisc show dev eth0
EOF
  echo "提示:换档位直接重跑本脚本;拆除用 tc_teardown.sh"
  ;;
# ================= source-d(v4 train:宿主机本地源站容器挂点)=================
source-d)
  NAME="${TRAIN_SOURCE_CONTAINER:-$(CFG TRAIN_SOURCE_CONTAINER)}"
  TC_IMAGE="${TC_IMAGE:-kindest/node:v1.28.15}"   # 本机已有(第一步探针回退实测形态),内含 /usr/sbin/tc
  if [ "$DRY_RUN" = 1 ]; then
    echo "[dry-run] target=source-d container=$NAME rate=$RATE delay=$DELAY latency=50ms"
    echo "[dry-run]   burst=$BURST  ($BURST_SRC)"
    echo "[dry-run]   netem 生效参数=$NETEM_ARGS"
    cat <<EOF
[dry-run] 将执行(本地源站容器 eth0 egress;本次不实际执行):
[dry-run]   docker inspect -f '{{.State.Pid}}' $NAME   # → PID
[dry-run]   # 首选通道 root 直跑 / sudo -n / sudo -S(.sudo-pass):
[dry-run]   nsenter -t \$PID -n tc qdisc del dev eth0 root 2>/dev/null || true
[dry-run]   nsenter -t \$PID -n tc qdisc add dev eth0 root handle 1: tbf rate $RATE burst $BURST latency 50ms
$FQ_LINE_SD
$NETEM_LINE_SD
[dry-run]   nsenter -t \$PID -n tc qdisc show dev eth0
[dry-run]   # 回退(无 sudo 权限):特权容器共享目标 netns,镜像 $TC_IMAGE
[dry-run]   docker run --rm --net=container:$NAME --cap-add=NET_ADMIN --entrypoint /bin/sh $TC_IMAGE -c \\
[dry-run]     'tc qdisc del dev eth0 root 2>/dev/null || true; tc qdisc add dev eth0 root handle 1: tbf rate $RATE burst $BURST latency 50ms && tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM_ARGS && tc qdisc show dev eth0'
EOF
    exit 0
  fi
  PID=$(docker inspect -f '{{.State.Pid}}' "$NAME" 2>/dev/null) \
    || { echo "错误:容器 $NAME 不存在(先启动 train源站;一次性容器测试可用环境变量 TRAIN_SOURCE_CONTAINER 覆盖容器名)" >&2; exit 1; }
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
  echo "qdisc 形态=$QDISC_SHAPE"
  echo "netem=$NETEM_ARGS"
  if [ -n "$ROOT_VIA" ]; then
    run_root nsenter -t "$PID" -n tc qdisc del dev eth0 root 2>/dev/null || true
    if run_root nsenter -t "$PID" -n tc qdisc add dev eth0 root handle 1: \
         tbf rate "$RATE" burst "$BURST" latency 50ms; then
      if [ -n "$PER_CONN" ]; then
        # sch_fq 可能是可加载模块,内建于内核时 modprobe 失败可忽略;挂 fq 失败才报错
        run_root modprobe sch_fq 2>/dev/null || true
        run_root nsenter -t "$PID" -n tc qdisc add dev eth0 parent 1:1 handle 10: fq maxrate "$PER_CONN" \
          && run_root nsenter -t "$PID" -n tc qdisc add dev eth0 parent 10:1 handle 20: netem $NETEM_ARGS
      else
        run_root nsenter -t "$PID" -n tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM_ARGS
      fi
      echo "--- 已挂载(root 通道:$ROOT_VIA,nsenter 容器 netns)---"
      run_root nsenter -t "$PID" -n tc qdisc show dev eth0
    else
      echo "错误:tc 挂载失败(root 通道:$ROOT_VIA)" >&2; exit 1
    fi
  else
    docker image inspect "$TC_IMAGE" >/dev/null 2>&1 \
      || { echo "错误:本机无回退镜像 $TC_IMAGE(用 TC_IMAGE= 指定本地已有镜像)" >&2; exit 1; }
    echo "未取得 root 通道 → 回退特权 docker 容器(镜像 $TC_IMAGE,共享容器 $NAME 的 netns)"
    if [ -n "$PER_CONN" ]; then
      LEAF_SETUP="modprobe sch_fq 2>/dev/null || true; tc qdisc add dev eth0 parent 1:1 handle 10: fq maxrate $PER_CONN && tc qdisc add dev eth0 parent 10:1 handle 20: netem $NETEM_ARGS"
    else
      LEAF_SETUP="tc qdisc add dev eth0 parent 1:1 handle 10: netem $NETEM_ARGS"
    fi
    docker run --rm --net="container:$NAME" --cap-add=NET_ADMIN --entrypoint /bin/sh "$TC_IMAGE" -c "
      tc qdisc del dev eth0 root 2>/dev/null || true
      tc qdisc add dev eth0 root handle 1: tbf rate $RATE burst $BURST latency 50ms &&
      $LEAF_SETUP &&
      echo '--- 已挂载(特权 docker 回退)---' && tc qdisc show dev eth0" \
      || { echo "错误:docker 回退挂载失败" >&2; exit 1; }
  fi
  echo "提示:换档位直接重跑本脚本;拆除用 tc_teardown.sh --target source-d"
  ;;
esac

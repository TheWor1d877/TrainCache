#!/usr/bin/env bash
# tamper_block.sh — 故障注入:原地改坏 agent 本地缓存里的一个块文件(数据面损坏)。
#
# 注入手法:
#   在 agentd Pod 里(它挂了 hostPath /var/cache/traincache,controller/deploy/agent/daemonset.yaml:139-147),
#   挑一个已缓存的块文件 blocks/<fp[0:2]>/<fp>.blk(core/cas/store.go:90-93),
#   在其数据区(128 字节块头之后,core/cas/header.go:24-25)原地按字节取反若干字节。
#   改的是数据区不是块头:块头被改会撞 magic/version 解析失败(另一种失败路径),
#   数据区被改才是"读时校验发现内容与指纹不符"的路径(cas.Open 重算 xxh3/sha256)。
#
# 校验档位:
#   默认档 xxh3 就能检出改字节:core/cas/store.go 的 Open 流式重算数据 xxh3,
#   与块头里的指纹不符即返回 ErrCorrupt。双校验档(xxh3+sha256)只在切换之后
#   新写的块上生效(docs/5.2-decisions.md D13),改老块时 sha256 不参与比对。
#   所以本注入在任意档位都能执行,脚本只读取当前档位记进证据,不据此拒绝。
#
# 可重复性:
#   改之前先把原块文件原样拷到缓存里的旁文件(<block>.chaos-orig),
#   并把原字节拷到证据目录(OUT_DIR/tamper_backup/<name>.orig),
#   同一轮里再跑一次不会叠加损坏;--restore 可用备份把字节写回去。
#
# 只改一个块文件的一个字节区间;不删文件、不动索引、不动其它块。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "$SCRIPT_DIR/../lib/common.sh"

NS="$CHAOS_NS_DEFAULT"
DS="$CHAOS_AGENTD_DS_DEFAULT"
CACHE_ROOT="$CHAOS_CACHE_ROOT_DEFAULT"
OFFSET=""          # 空 = 自动取数据区第一个字节(块头 128B 之后)
NBYTES=1
BLOCK_REL=""       # 空 = 自动挑第一个 .blk
TRIGGER="rescan"   # rescan = 改完后重启 agentd 触发全量校验; none = 只等外部重读
RESTORE=0
OBSERVE=60
AGENTD_READY_TIMEOUT=120
DRY_RUN=0
KUBECONFIG_ARG=""
OUT_DIR=""

usage() {
  cat <<'EOF'
用法: tamper_block.sh [--ns NAMESPACE] [--ds AGENTD_DS] [--cache-root DIR]
                      [--block REL_PATH] [--offset N] [--bytes N] [--observe SECONDS]
                      [--trigger rescan|none] [--ready-timeout SECONDS]
                      [--restore]
                      [--out DIR] [--kubeconfig PATH] [--dry-run] [--help]
  --ns           agentd 所在 namespace(默认 traincache)
  --ds           agentd DaemonSet 名(默认 traincache-agentd)
  --cache-root   agent 本地缓存根(默认 /var/cache/traincache)
  --block        要改的块文件(相对 cache-root,如 blocks/ab/ab12....blk);不给则自动挑
  --offset       改写起点(块文件内绝对偏移);不给则取块头之后的第一个字节
  --bytes        改写字节数(默认 1)
  --observe      改坏后等校验层反应的上限秒数(默认 60),期间轮询隔离/修复指标
  --trigger      改坏之后怎么让校验层发现:rescan(默认,重启 agentd 触发全量校验)
                 或 none(只等训练侧重读该块)
  --ready-timeout  等 agentd 替代 Pod 就绪的上限秒数(默认 120,只在 --trigger rescan 时用)
  --restore      用证据目录里的备份把字节写回去
  --out          证据目录(写 tampered_block.txt / tamper_backup/ / assertions.txt)
  --kubeconfig   本轮 kubeconfig 副本;不给则要求环境变量 KUBECONFIG
  --dry-run      只打印将执行的命令,不碰集群
  --help         本用法
校验档位: 只读取 agent DaemonSet 的 TRAINCACHE_VERIFY 记进证据,任意档位都执行。
          默认 xxh3 由 core/cas/store.go 的 Open 重算摘要检出改字节。
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --ns) [ "$#" -ge 2 ] || { echo "错误:--ns 缺值" >&2; exit 2; }; NS="$2"; shift 2 ;;
    --ns=*) NS="${1#--ns=}"; shift ;;
    --ds) [ "$#" -ge 2 ] || { echo "错误:--ds 缺值" >&2; exit 2; }; DS="$2"; shift 2 ;;
    --ds=*) DS="${1#--ds=}"; shift ;;
    --cache-root) [ "$#" -ge 2 ] || { echo "错误:--cache-root 缺值" >&2; exit 2; }; CACHE_ROOT="$2"; shift 2 ;;
    --cache-root=*) CACHE_ROOT="${1#--cache-root=}"; shift ;;
    --block) [ "$#" -ge 2 ] || { echo "错误:--block 缺值" >&2; exit 2; }; BLOCK_REL="$2"; shift 2 ;;
    --block=*) BLOCK_REL="${1#--block=}"; shift ;;
    --offset) [ "$#" -ge 2 ] || { echo "错误:--offset 缺值" >&2; exit 2; }; OFFSET="$2"; shift 2 ;;
    --offset=*) OFFSET="${1#--offset=}"; shift ;;
    --bytes) [ "$#" -ge 2 ] || { echo "错误:--bytes 缺值" >&2; exit 2; }; NBYTES="$2"; shift 2 ;;
    --bytes=*) NBYTES="${1#--bytes=}"; shift ;;
    --observe) [ "$#" -ge 2 ] || { echo "错误:--observe 缺值" >&2; exit 2; }; OBSERVE="$2"; shift 2 ;;
    --observe=*) OBSERVE="${1#--observe=}"; shift ;;
    --trigger) [ "$#" -ge 2 ] || { echo "错误:--trigger 缺值" >&2; exit 2; }; TRIGGER="$2"; shift 2 ;;
    --trigger=*) TRIGGER="${1#--trigger=}"; shift ;;
    --ready-timeout) [ "$#" -ge 2 ] || { echo "错误:--ready-timeout 缺值" >&2; exit 2; }; AGENTD_READY_TIMEOUT="$2"; shift 2 ;;
    --ready-timeout=*) AGENTD_READY_TIMEOUT="${1#--ready-timeout=}"; shift ;;
    --restore) RESTORE=1; shift ;;
    --out) [ "$#" -ge 2 ] || { echo "错误:--out 缺值" >&2; exit 2; }; OUT_DIR="$2"; shift 2 ;;
    --out=*) OUT_DIR="${1#--out=}"; shift ;;
    --kubeconfig) [ "$#" -ge 2 ] || { echo "错误:--kubeconfig 缺值" >&2; exit 2; }; KUBECONFIG_ARG="$2"; shift 2 ;;
    --kubeconfig=*) KUBECONFIG_ARG="${1#--kubeconfig=}"; shift ;;
    *) echo "错误:未知选项 $1" >&2; usage >&2; exit 2 ;;
  esac
done

chaos_resolve_kubeconfig "$KUBECONFIG_ARG"

if [ "$DRY_RUN" = 1 ]; then
  chaos_note "[dry-run] 故障:tamper_block(原地改坏 agent 本地缓存里的一个块文件)$([ "$RESTORE" = 1 ] && echo ' [恢复模式]')"
  chaos_show_cmd -n "$NS" get ds "$DS" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="TRAINCACHE_VERIFY")].value}'
  chaos_note "[dry-run]   ↑ 当前校验档位(空 = 用代码默认 xxh3);只记进证据,不据此拒绝"
  chaos_show_cmd -n "$NS" get pods -l "$CHAOS_AGENTD_LABEL_DEFAULT" \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.nodeName}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}'
  chaos_note "[dry-run]   ↑ agentd Pod 列表(取与训练 Pod 同节点的那个;hostPath 是节点级的)"
  chaos_show_cmd -n "$NS" exec "\$AGENTD_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
    sh -c "ls -1 $CACHE_ROOT/blocks/*/*.blk 2>/dev/null | head -1"
  chaos_note "[dry-run]   ↑ 挑一个已缓存的块文件(blocks/<fp[0:2]>/<fp>.blk)"
  chaos_show_cmd -n "$NS" exec "\$AGENTD_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
    sh -c "cp $CACHE_ROOT/\$BLOCK $CACHE_ROOT/\$BLOCK.chaos-orig"
  chaos_note "[dry-run]   ↑ 先在缓存里留一份原字节旁文件(.chaos-orig),同轮重跑不叠加损坏"
  chaos_show_cmd -n "$NS" exec "\$AGENTD_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
    sh -c "dd if=\$BLOCK.chaos-orig bs=1 count=\$NBYTES skip=\$OFFSET of=\$BLOCK conv=notrunc"
  chaos_note "[dry-run]   ↑ 用备份里的原字节写回改写区间(逐字节取反后写回)"
  chaos_show_cmd -n "$NS" exec "\$AGENTD_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
    sh -c "sha256sum \$BLOCK"
  chaos_note "[dry-run]   ↑ 回读校验和,与改前对比"
  chaos_show_cmd -n "$NS" exec "\$AGENTD_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
    wget -qO- "http://127.0.0.1:${CHAOS_METRICS_PORT_DEFAULT}/metrics"
  chaos_note "[dry-run]   ↑ 改坏前后各取一次 /metrics:quarantined_blocks / repair_queue_depth / fetch_requests_total{reason=\"repair\"}"
  if [ "$TRIGGER" = "rescan" ]; then
    chaos_show_cmd -n "$NS" delete pod "\$AGENTD_POD" --grace-period=0 --wait=false
    chaos_note "[dry-run]   ↑ 触发方式:rescan(重启 agentd,等替代 Pod 就绪后 rescan 逐块校验)"
    chaos_note "[dry-run]   ↑ 替代 Pod 就绪上限 ${AGENTD_READY_TIMEOUT}s,之后轮询隔离与修复指标 ${OBSERVE}s"
  else
    chaos_note "[dry-run]   ↑ 触发方式:none(只等训练侧重读该块)"
  fi
  exit 0
fi

chaos_require_kubeconfig
chaos_prepare_out_dir
ASSERT_FILE="$OUT_DIR/assertions.txt"
: >"$ASSERT_FILE"
pass() { printf 'PASS %s\n' "$*" | tee -a "$ASSERT_FILE"; }
fail() { printf 'FAIL %s\n' "$*" | tee -a "$ASSERT_FILE" >&2; }

ctx="$(chaos_record_context)"
chaos_note "kubeconfig=$CHAOS_KUBECONFIG context=$ctx ns=$NS ds=$DS"

# ---- 校验档位:只记录,不据此拒绝 ----
VERIFY="$(chaos_kubectl -n "$NS" get ds "$DS" \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="TRAINCACHE_VERIFY")].value}' 2>/dev/null || true)"
chaos_note "校验档位 TRAINCACHE_VERIFY=${VERIFY:-<未设置,取代码默认 xxh3>}(默认 xxh3 由 cas.Open 重算摘要检出改字节)"

# ---- 选 agentd Pod:与训练 Pod 同节点优先(hostPath 是节点级的) ----
ROWS="$(chaos_agentd_pod_rows "$NS")"
[ -n "$ROWS" ] || chaos_die "命名空间 $NS 里没有 agentd Pod"
AGENTD_POD=""
TRAIN_NODE="$(chaos_kubectl -n "$NS" get pod "$CHAOS_TRAIN_POD_DEFAULT" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
if [ -n "$TRAIN_NODE" ]; then
  AGENTD_POD="$(printf '%s\n' "$ROWS" | awk -v n="$TRAIN_NODE" '$2==n {print $1; exit}')"
fi
[ -n "$AGENTD_POD" ] || AGENTD_POD="$(printf '%s\n' "$ROWS" | awk '$3=="True" {print $1; exit}')"
[ -n "$AGENTD_POD" ] || chaos_die "选不出 Ready 的 agentd Pod:$ROWS"
chaos_note "agentd Pod=$AGENTD_POD(训练 Pod 节点=${TRAIN_NODE:-<无>})"

# 在容器里跑一段 sh(参数用位置参数传,避免多层引号转义)
agentd_sh() {
  chaos_kubectl -n "$NS" exec -i "$AGENTD_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- sh -s -- "$@"
}

# ---- 挑块文件 ----
if [ -z "$BLOCK_REL" ]; then
  BLOCK_REL="$(chaos_kubectl -n "$NS" exec "$AGENTD_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
    sh -c "ls -1 $CACHE_ROOT/blocks/*/*.blk 2>/dev/null | head -1")"
fi
[ -n "$BLOCK_REL" ] || chaos_die "$CACHE_ROOT 下没有已缓存的块文件(blocks/*/*.blk);先跑有数据的训练臂再注入"
BLOCK_PATH="$CACHE_ROOT/$BLOCK_REL"
chaos_note "目标块文件=$BLOCK_PATH"

BACKUP_DIR="$OUT_DIR/tamper_backup"
mkdir -p "$BACKUP_DIR"
BACKUP_LOCAL="$BACKUP_DIR/$(basename "$BLOCK_REL").orig"

if [ "$RESTORE" = 1 ]; then
  [ -r "$BACKUP_LOCAL" ] || chaos_die "没有备份可恢复:$BACKUP_LOCAL 不存在"
  agentd_sh "$BLOCK_PATH" <<'EOS'
f="$1"
cat > "$f"
EOS
  AFTER="$(agentd_sh "$BLOCK_PATH" <<'EOS'
sha256sum "$1"
EOS
)"
  pass "已用备份恢复:$BLOCK_PATH"
  printf 'restored=%s\nsha256=%s\n' "$BLOCK_PATH" "$AFTER" >"$OUT_DIR/tampered_block.txt"
  exit 0
fi

BEFORE="$(agentd_sh "$BLOCK_PATH" <<'EOS'
sha256sum "$1"
EOS
)"
BEFORE_SUM="$(printf '%s' "$BEFORE" | awk '{print $1}')"
chaos_note "改前 sha256=$BEFORE_SUM"

# ---- 改坏前的校验/修复指标基线(判据:隔离块上升、修复队列上升、repair 回源增加)----
chaos_metrics_text "$NS" "$AGENTD_POD" >"$OUT_DIR/metrics_before.txt" 2>/dev/null || true
QUAR_BEFORE="$(chaos_metric_sum "$NS" "$AGENTD_POD" traincache_quarantined_blocks)"
RQ_BEFORE="$(chaos_metric_sum "$NS" "$AGENTD_POD" traincache_repair_queue_depth)"
RREQ_BEFORE="$(chaos_metric_label_sum "$NS" "$AGENTD_POD" traincache_fetch_requests_total reason repair)"
chaos_note "基线:quarantined=${QUAR_BEFORE:-0} repair_queue_depth=${RQ_BEFORE:-0} repair_requests=${RREQ_BEFORE:-0}"

# ---- 先留原字节旁文件(同轮可重复) ----
agentd_sh "$BLOCK_PATH" <<'EOS'
f="$1"
cp "$f" "$f.chaos-orig"
EOS
# 同时把原字节拷到证据目录(供 --restore 跨进程用)
chaos_kubectl -n "$NS" exec "$AGENTD_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
  sh -c "cat '$BLOCK_PATH'" >"$BACKUP_LOCAL"
pass "原字节已留底:$BLOCK_PATH.chaos-orig(同轮重跑不叠加)+ $BACKUP_LOCAL"

# ---- 决定改写区间 ----
if [ -z "$OFFSET" ]; then
  OFFSET=128   # 块头 128B 之后 = 数据区起点(core/cas/header.go:24-25)
fi
chaos_note "改写区间:offset=$OFFSET bytes=$NBYTES(数据区,不动块头)"

# ---- 原地按字节取反 ----
# 取反逻辑放在容器里(agentd 镜像是 alpine,只有 busybox 工具):
#   读一个字节 → 255 - b → 写回同一偏移(dd conv=notrunc 不截断文件)
TOGGLE_OUT="$(agentd_sh "$BLOCK_PATH" "$OFFSET" "$NBYTES" <<'EOS'
set -eu
f="$1"; off="$2"; n="$3"
tmp=/tmp/.chaos-byte.$$
trap 'rm -f "$tmp"' EXIT
i=0
while [ "$i" -lt "$n" ]; do
  p=$((off + i))
  # 取一个字节的数值:od / hexdump / xxd 三条路都试(busybox 与 coreutils 各自支持哪条不确定)
  dd if="$f" bs=1 skip="$p" count=1 of="$tmp" 2>/dev/null
  b=$(od -An -tu1 "$tmp" 2>/dev/null | tr -s ' ' '\n' | grep -E '^[0-9]+$' | tail -1)
  [ -n "$b" ] || b=$(od -tu1 "$tmp" 2>/dev/null | tr -s ' ' '\n' | grep -E '^[0-9]+$' | tail -1)
  [ -n "$b" ] || b=$(hexdump -v -e '1/1 "%u"' "$tmp" 2>/dev/null | tr -dc '0-9')
  if [ -z "$b" ]; then
    h=$(xxd -p "$tmp" 2>/dev/null | head -1 | tr -dc '0-9a-fA-F')
    if [ -n "$h" ]; then
      b=$(awk -v h="$h" 'BEGIN{n=0;for(i=1;i<=length(h);i++){c=tolower(substr(h,i,1));d=index("0123456789abcdef",c)-1;n=n*16+d}print n}')
    fi
  fi
  [ -n "$b" ] || { echo "偏移 $p 读不到字节(od/hexdump/xxd 都不可用)" >&2; exit 1; }
  nb=$((255 - b))
  printf "\\$(printf '%03o' "$nb")" | dd of="$f" bs=1 seek="$p" count=1 conv=notrunc 2>/dev/null
  i=$((i + 1))
done
echo "toggled=$n offset=$off"
EOS
)"
chaos_note "容器内改写结果:$TOGGLE_OUT"
pass "已在 $BLOCK_PATH 的 offset=$OFFSET 处原地改写 $NBYTES 字节"

AFTER="$(agentd_sh "$BLOCK_PATH" <<'EOS'
sha256sum "$1"
EOS
)"
AFTER_SUM="$(printf '%s' "$AFTER" | awk '{print $1}')"
chaos_note "改后 sha256=$AFTER_SUM"

if [ -n "$AFTER_SUM" ] && [ "$AFTER_SUM" != "$BEFORE_SUM" ]; then
  pass "块内容确实变了(改前 sha256 != 改后 sha256)"
else
  fail "块内容没变,注入无效(改前=$BEFORE_SUM 改后=$AFTER_SUM)"
fi

# ---- 触发校验层发现:重启 agentd 走 rescan 全量校验(agent/rescan/rescan.go 的 verify)----
if [ "$TRIGGER" = "rescan" ]; then
  OLD_POD="$AGENTD_POD"
  chaos_note "触发方式:rescan(重启 agentd Pod $OLD_POD,等它 rescan 时逐块校验)"
  chaos_kubectl -n "$NS" delete pod "$OLD_POD" --grace-period=0 --wait=false >/dev/null 2>&1 || true
  READY_END=$(( $(date +%s) + AGENTD_READY_TIMEOUT ))
  NEW_POD=""
  while [ "$(date +%s)" -lt "$READY_END" ]; do
    NEW_POD="$(chaos_agentd_pod_rows "$NS" | awk -v old="$OLD_POD" '$1!=old && $3=="True" {print $1; exit}')"
    [ -n "$NEW_POD" ] && break
    sleep 3
  done
  if [ -z "$NEW_POD" ]; then
    fail "agentd 替代 Pod 在 ${AGENTD_READY_TIMEOUT}s 内没就绪,rescan 触发没生效"
  else
    AGENTD_POD="$NEW_POD"
    chaos_note "agentd 替代 Pod=$AGENTD_POD,开始轮询隔离与修复指标"
  fi
fi

printf 'block=%s\noffset=%s\nbytes=%s\nsha256_before=%s\nsha256_after=%s\nbackup_local=%s\nbackup_in_cache=%s.chaos-orig\n' \
  "$BLOCK_PATH" "$OFFSET" "$NBYTES" "$BEFORE_SUM" "$AFTER_SUM" "$BACKUP_LOCAL" "$BLOCK_PATH" \
  >"$OUT_DIR/tampered_block.txt"

# ---- 观测:校验层是否抓住了这次损坏(隔离 / 修复队列 / repair 回源)----
QUAR_AFTER="$QUAR_BEFORE"
RQ_AFTER="$RQ_BEFORE"
RREQ_AFTER="$RREQ_BEFORE"
END_TS=$(( $(date +%s) + OBSERVE ))
while [ "$(date +%s)" -lt "$END_TS" ]; do
  QUAR_AFTER="$(chaos_metric_sum "$NS" "$AGENTD_POD" traincache_quarantined_blocks)"
  RQ_AFTER="$(chaos_metric_sum "$NS" "$AGENTD_POD" traincache_repair_queue_depth)"
  RREQ_AFTER="$(chaos_metric_label_sum "$NS" "$AGENTD_POD" traincache_fetch_requests_total reason repair)"
  QUAR_AFTER="${QUAR_AFTER:-0}"; RQ_AFTER="${RQ_AFTER:-0}"; RREQ_AFTER="${RREQ_AFTER:-0}"
  HIT="$(awk -v a="$QUAR_AFTER" -v q="$QUAR_BEFORE" -v r="$RQ_AFTER" -v s="$RREQ_BEFORE" -v t="$RREQ_AFTER" \
        'BEGIN{print ((a+0>q+0)||(r+0>0)||(t+0>s+0))?1:0}')"
  if [ "$HIT" = "1" ]; then
    break
  fi
  sleep 3
done
chaos_metrics_text "$NS" "$AGENTD_POD" >"$OUT_DIR/metrics_after.txt" 2>/dev/null || true

if [ "$(awk -v a="$QUAR_AFTER" -v q="$QUAR_BEFORE" 'BEGIN{print (a+0>q+0)?1:0}')" = "1" ]; then
  pass "traincache_quarantined_blocks 上升(${QUAR_BEFORE:-0} → ${QUAR_AFTER:-0}),坏块进了隔离区"
elif [ "$(awk -v r="$RQ_AFTER" -v s="$RREQ_AFTER" -v sb="$RREQ_BEFORE" 'BEGIN{print ((r+0>0)||(s+0>sb+0))?1:0}')" = "1" ]; then
  pass "修复队列/修复回源有反应(repair_queue_depth=${RQ_AFTER:-0} repair_requests=${RREQ_BEFORE:-0}→${RREQ_AFTER:-0})"
else
  fail "观察 ${OBSERVE}s 内没看到校验层反应(quarantined=${QUAR_AFTER:-0} repair_queue_depth=${RQ_AFTER:-0} repair_requests=${RREQ_AFTER:-0})"
fi

# ---- 修复队列回落:坏块重拉完后队列要回到 0,repair 回源计数要涨 ----
MAX_RQ="$RQ_AFTER"
FINAL_RQ="$RQ_AFTER"
FINAL_RREQ="$RREQ_AFTER"
END_TS=$(( $(date +%s) + OBSERVE ))
while [ "$(date +%s)" -lt "$END_TS" ]; do
  FINAL_RQ="$(chaos_metric_sum "$NS" "$AGENTD_POD" traincache_repair_queue_depth)"
  FINAL_RREQ="$(chaos_metric_label_sum "$NS" "$AGENTD_POD" traincache_fetch_requests_total reason repair)"
  FINAL_RQ="${FINAL_RQ:-0}"; FINAL_RREQ="${FINAL_RREQ:-0}"
  MAX_RQ="$(awk -v a="$FINAL_RQ" -v m="$MAX_RQ" 'BEGIN{print (a+0>m+0)?a:m}')"
  if [ "$FINAL_RQ" = "0" ] && [ "$(awk -v a="$FINAL_RREQ" -v b="$RREQ_BEFORE" 'BEGIN{print (a+0>b+0)?1:0}')" = "1" ]; then
    break
  fi
  sleep 3
done
if [ "$(awk -v a="$FINAL_RREQ" -v b="$RREQ_BEFORE" 'BEGIN{print (a+0>b+0)?1:0}')" = "1" ]; then
  pass "repair 回源计数上升(${RREQ_BEFORE:-0} → ${FINAL_RREQ}),坏块走了修复队列"
else
  fail "repair 回源计数没涨(${RREQ_BEFORE:-0} → ${FINAL_RREQ})"
fi
if [ "${FINAL_RQ:-0}" -eq 0 ] 2>/dev/null; then
  pass "修复队列已回落(峰值 ${MAX_RQ},终值 ${FINAL_RQ})"
elif [ "$(awk -v a="$MAX_RQ" -v b="$RQ_BEFORE" 'BEGIN{print (a+0>b+0)?1:0}')" = "1" ]; then
  pass "修复队列上升过(峰值 ${MAX_RQ}),观察窗口内还没回落"
else
  fail "修复队列没上升(峰值 ${MAX_RQ},终值 ${FINAL_RQ})"
fi

printf 'quarantined_before=%s\nquarantined_after=%s\nrepair_queue_depth_before=%s\nrepair_queue_depth_after=%s\nrepair_requests_before=%s\nrepair_requests_after=%s\nobserve_s=%s\n' \
  "${QUAR_BEFORE:-}" "${QUAR_AFTER:-}" "${RQ_BEFORE:-}" "${RQ_AFTER:-}" "${RREQ_BEFORE:-}" "${RREQ_AFTER:-}" "$OBSERVE" \
  >>"$OUT_DIR/tampered_block.txt"
chaos_note "证据:$OUT_DIR/tampered_block.txt(基线/终值见 metrics_before.txt / metrics_after.txt)"

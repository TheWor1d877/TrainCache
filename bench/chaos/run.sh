#!/usr/bin/env bash
# run.sh — TrainCache 韧性(混沌)套件的运行器。
#
# 用法:
#   run.sh --list                 列出所有故障与一句话含义
#   run.sh --dry-run              打印每个故障将要执行的命令,不碰集群
#   run.sh --fault <name>         只跑一个故障(注入 + 断言)
#   run.sh --all                  跑强制集(mandatory set)
#
# 产物:
#   $CHAOS_DATA_ROOT/<run-id>/<fault>/   每个故障的证据目录(命令回显/指标/断言)
#   $CHAOS_DATA_ROOT/<run-id>/summary.txt 汇总:每个故障 PASS/FAIL + 观测到的指标值
#   CHAOS_DATA_ROOT 默认 ~/tcd/chaos(共享数据层,只增不删;每轮新建 run-id 目录)
#   run-id = UTC 时间戳 + 四位随机后缀(同一秒并发跑也不撞目录)
#
# 强制集(mandatory set)的口径:
#   四个必跑故障 —— kill_agent(agentd 被 SIGKILL)、netem_degrade(源站网络降级)、
#   minio_down(源站整体下线)、stop_ordersrc(订单写端停摆)。
#   tamper_block 不在强制集里:它要求 agentd 跑在 TRAINCACHE_VERIFY=xxh3+sha256
#   档位(否则改坏的块可能不被校验层发现,注入就没有判据意义),该前置由使用者
#   显式开好再用 --fault tamper_block 单跑。
#
# 集群安全:
#   · 必须显式给 KUBECONFIG(环境变量)或 --kubeconfig <本轮副本>;不给就拒绝跑。
#     装置不执行 kubectl config use-context,不动全局 ~/.kube/config。
#   · 不删任何集群对象;minio_down 只 scale 副本数,PVC 与桶数据留着。
#   · 共享数据层 ~/tcd 不做 rm -rf;每轮只新建目录。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

MODE="list"
FAULT=""
KUBECONFIG_ARG=""
DRY_RUN=0
RUN_DIR=""
DATA_ROOT="$(chaos_data_root)"
KEEP_RUN_DIR=0
EXTRA_ARGS=()
PASSTHRU=0

MANDATORY_FAULTS="kill_agent netem_degrade minio_down stop_ordersrc"
ALL_FAULTS="kill_agent netem_degrade minio_down tamper_block stop_ordersrc"

usage() {
  cat <<'EOF'
用法: run.sh [--list | --dry-run | --fault NAME | --all]
              [--kubeconfig PATH] [--data-root DIR] [--keep-run-dir]
              [-- 透传给注入脚本的参数] [--help]
  --list           列出故障与一句话含义(默认模式)
  --dry-run        打印每个故障将要执行的命令,不碰集群(可与 --fault 组合)
  --fault NAME     只跑一个故障(注入 + 断言)
  --all            跑强制集:kill_agent netem_degrade minio_down stop_ordersrc
  --kubeconfig     本轮 kubeconfig 副本;不给则要求环境变量 KUBECONFIG
  --data-root      证据根目录(默认 ~/tcd/chaos,即 $CHAOS_DATA_ROOT)
  --keep-run-dir   保留本轮 run-id 目录(默认也保留:数据层只增不删)
  -- <参数...>     其后参数原样透传给注入脚本(如 -- --timeout 60)
  --help           本用法
故障清单:
  kill_agent     SIGKILL agentd Pod,等 DaemonSet 拉起替代 Pod,记录重建耗时
  netem_degrade  源站容器 eth0 egress 挂 netem(时延+抖动+丢包),测链路降级
  minio_down     MinIO Deployment 缩到 0 副本,测源站整体不可用
  tamper_block   原地改坏 agent 本地缓存里的一个块文件,测数据面校验
                 (需 TRAINCACHE_VERIFY=xxh3+sha256,不在强制集内)
  stop_ordersrc  冻结 ordersrc sidecar 写端,让订单 ring 停摆,测降级判定
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --list) MODE="list"; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --fault) [ "$#" -ge 2 ] || { echo "错误:--fault 缺值" >&2; exit 2; }; MODE="fault"; FAULT="$2"; shift 2 ;;
    --fault=*) MODE="fault"; FAULT="${1#--fault=}"; shift ;;
    --all) MODE="all"; shift ;;
    --kubeconfig) [ "$#" -ge 2 ] || { echo "错误:--kubeconfig 缺值" >&2; exit 2; }; KUBECONFIG_ARG="$2"; shift 2 ;;
    --kubeconfig=*) KUBECONFIG_ARG="${1#--kubeconfig=}"; shift ;;
    --data-root) [ "$#" -ge 2 ] || { echo "错误:--data-root 缺值" >&2; exit 2; }; DATA_ROOT="$2"; shift 2 ;;
    --data-root=*) DATA_ROOT="${1#--data-root=}"; shift ;;
    --keep-run-dir) KEEP_RUN_DIR=1; shift ;;
    --) PASSTHRU=1; shift ;;
    *)
      if [ "$PASSTHRU" = 1 ]; then EXTRA_ARGS+=("$1"); shift; continue; fi
      echo "错误:未知选项 $1" >&2; usage >&2; exit 2
      ;;
  esac
done

chaos_resolve_kubeconfig "$KUBECONFIG_ARG"

fault_meaning() {
  case "$1" in
    kill_agent)    printf 'SIGKILL agentd Pod,等 DaemonSet 拉起替代 Pod,记录重建耗时' ;;
    netem_degrade) printf '源站容器 eth0 egress 挂 netem(时延+抖动+丢包),测链路降级' ;;
    minio_down)    printf 'MinIO Deployment 缩到 0 副本,测源站整体不可用' ;;
    tamper_block)  printf '原地改坏 agent 本地缓存里的一个块文件,测数据面校验(需 xxh3+sha256 档位)' ;;
    stop_ordersrc) printf '冻结 ordersrc sidecar 写端,让订单 ring 停摆,测降级判定' ;;
    *)             printf '<未知故障>' ;;
  esac
}

fault_script() {
  case "$1" in
    kill_agent|netem_degrade|minio_down|tamper_block|stop_ordersrc)
      printf '%s/inject/%s.sh' "$CHAOS_ROOT" "$1" ;;
    *) printf '' ;;
  esac
}

is_known_fault() {
  case " $ALL_FAULTS " in *" $1 "*) return 0 ;; *) return 1 ;; esac
}

list_faults() {
  printf '== TrainCache 韧性套件:故障清单 ==\n'
  printf '强制集(--all):%s\n' "$MANDATORY_FAULTS"
  printf 'tamper_block 不在强制集(需 TRAINCACHE_VERIFY=xxh3+sha256 才有判据意义)\n\n'
  for f in $ALL_FAULTS; do
    printf '  %-16s %s\n' "$f" "$(fault_meaning "$f")"
  done
  printf '\n用法: run.sh --list | --dry-run | --fault <name> | --all\n'
}

if [ "$MODE" = "list" ] && [ "$DRY_RUN" != 1 ]; then
  list_faults
  exit 0
fi

# ---- dry-run:只打印命令 ----
if [ "$DRY_RUN" = 1 ]; then
  TARGETS="$ALL_FAULTS"
  # 同时给了 --fault 时只干跑那一个(便于配故障专属参数)
  if [ -n "$FAULT" ]; then TARGETS="$FAULT"; fi
  printf '== 干跑:下列命令是每个故障将会执行的(本次不实际执行)==\n'
  for f in $TARGETS; do
    is_known_fault "$f" || { echo "错误:未知故障 '$f'" >&2; usage >&2; exit 2; }
    printf '\n---- %s: %s ----\n' "$f" "$(fault_meaning "$f")"
    if [ -n "$KUBECONFIG_ARG" ]; then
      bash "$(fault_script "$f")" --dry-run --kubeconfig "$KUBECONFIG_ARG" ${EXTRA_ARGS+"${EXTRA_ARGS[@]}"}
    elif [ -n "${CHAOS_KUBECONFIG:-}" ]; then
      bash "$(fault_script "$f")" --dry-run --kubeconfig "$CHAOS_KUBECONFIG" ${EXTRA_ARGS+"${EXTRA_ARGS[@]}"}
    else
      bash "$(fault_script "$f")" --dry-run ${EXTRA_ARGS+"${EXTRA_ARGS[@]}"}
    fi
  done
  exit 0
fi

# ---- 真跑:先备好 run 目录 ----
if [ "$MODE" = "fault" ]; then
  is_known_fault "$FAULT" || { echo "错误:未知故障 '$FAULT'" >&2; usage >&2; exit 2; }
  TARGETS="$FAULT"
else
  TARGETS="$MANDATORY_FAULTS"
fi

# 没有 kubeconfig 就不跑(装置不替调用方选集群)
if [ "$CHAOS_KC_MODE" = "none" ]; then
  echo "错误:未提供 kubeconfig。请设置环境变量 KUBECONFIG,或用 --kubeconfig 指向本轮副本(装置不切全局 kubectl context)。" >&2
  exit 2
fi

RUN_ID="$(chaos_run_id)"
RUN_DIR="$DATA_ROOT/$RUN_ID"
mkdir -p "$RUN_DIR"
chaos_note "run-id=$RUN_ID 证据目录=$RUN_DIR"
CTX="$(chaos_record_context)"
printf 'run_id=%s\ncontext=%s\nkubeconfig=%s\nstarted=%s\n' \
  "$RUN_ID" "$CTX" "$CHAOS_KUBECONFIG" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$RUN_DIR/run_meta.txt"
chaos_note "kubeconfig=$CHAOS_KUBECONFIG context=$CTX"

SUMMARY="$RUN_DIR/summary.txt"
{
  printf '== TrainCache 韧性套件 结果 ==\n'
  printf 'run-id: %s\n' "$RUN_ID"
  printf 'context: %s\n' "$CTX"
  printf '开始: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '\n'
} >"$SUMMARY"

TOTAL=0
PASSED=0
FAILED=0

run_one_fault() {
  local f="$1"
  local script dir rc
  script="$(fault_script "$f")"
  dir="$RUN_DIR/$f"
  mkdir -p "$dir"
  printf '\n---- %s: %s ----\n' "$f" "$(fault_meaning "$f")"
  TOTAL=$((TOTAL + 1))
  set +e
  bash "$script" --out "$dir" --kubeconfig "$CHAOS_KUBECONFIG" \
    ${EXTRA_ARGS+"${EXTRA_ARGS[@]}"} >"$dir/output.log" 2>&1
  rc=$?
  set -e
  cat "$dir/output.log"
  local status="PASS"
  [ "$rc" -eq 0 ] || status="FAIL(rc=$rc)"
  {
    printf '%-16s %s\n' "$f" "$status"
    printf '  含义: %s\n' "$(fault_meaning "$f")"
    printf '  证据: %s\n' "$dir"
    local ef
    for ef in metrics.txt timings.txt params.txt replicas.txt ring_stat.txt tampered_block.txt; do
      if [ -f "$dir/$ef" ]; then
        printf '  观测(%s):\n' "$ef"
        sed 's/^/    /' "$dir/$ef"
      fi
    done
    if [ -f "$dir/assertions.txt" ]; then
      printf '  断言:\n'
      sed 's/^/    /' "$dir/assertions.txt"
      if grep -q '^FAIL' "$dir/assertions.txt"; then
        status="FAIL(断言未通过)"
      fi
    fi
    printf '\n'
  } >>"$SUMMARY"
  # 重算最终状态:上面 status 可能在断言检查时改写,这里统一落一次
  if [ "$rc" -ne 0 ] || grep -q '^FAIL' "$dir/assertions.txt" 2>/dev/null; then
    FAILED=$((FAILED + 1))
    sed -i "s|^$f .*|$f FAIL(rc=$rc)|" "$SUMMARY"
  else
    PASSED=$((PASSED + 1))
    sed -i "s|^$f .*|$f PASS|" "$SUMMARY"
  fi
}

for f in $TARGETS; do
  run_one_fault "$f"
done

{
  printf '== 汇总 ==\n'
  printf '共 %d 个故障:PASS %d / FAIL %d\n' "$TOTAL" "$PASSED" "$FAILED"
  printf '结束: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '证据根目录: %s\n' "$RUN_DIR"
} >>"$SUMMARY"

chaos_note ""
cat "$SUMMARY"
[ "$FAILED" -eq 0 ]

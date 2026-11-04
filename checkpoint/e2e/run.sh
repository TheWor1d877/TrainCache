#!/usr/bin/env bash
# run.sh — checkpoint M6 kind end-to-end: save step 1 to MinIO, recreate the
# Pod from an empty work volume, restore step 1.
#
# Usage:
#   ./checkpoint/e2e/run.sh [clean|setup|run|assert]   (default: run)
#
#   clean   delete the kind cluster and runtime logs
#   setup   create/refresh the cluster, build and load images, start MinIO,
#           create the bucket, apply the Secret/ConfigMap and Pod
#   run     setup + save/recreate/restore assertions (idempotent)
#   assert  re-run the assertions against the current cluster (no mutation)
#
# Environment overrides:
#   CLUSTER=kind-traincheckpoint-e2e
#   NODE_IMAGE=<kind node image, default kindest/node:v1.37.0>
#   SKIP_BUILD=1   reuse an existing checkpoint image without a freshness check
#
# Exit codes: 0 all assertions passed; 1 a step/assertion failed; 2 usage or
# missing tool. On failure the kind cluster is kept for inspection and the
# Pod describe/logs are printed.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
E2E="$SCRIPT_DIR"

CLUSTER="${CLUSTER:-kind-traincheckpoint-e2e}"
NODE_IMAGE="${NODE_IMAGE:-kindest/node:v1.37.0}"
TAG=dev

NS=traincache
POD=traincache-checkpoint-e2e
TASK=t1
STEP=1
PREFIX=e2e
MANIFEST_KEY="$PREFIX/$TASK/step-$STEP/rank-0/manifest.json"

IMG="ghcr.io/thewor1d877/traincache/traincache-checkpointd:$TAG"
IMG_MINIO="pgsty/minio:RELEASE.2026-08-04T00-00-00Z"
IMG_BUSYBOX="busybox:1.36.1"

LOG_DIR="${TMPDIR:-/tmp}/traincheckpoint-e2e"
MODE="${1:-run}"
FAILS=0
PF_PID=""

say()  { printf '%s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
ok()   { printf '  [OK]   %s\n' "$*"; }
info() { printf '  [INFO] %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; FAILS=$((FAILS + 1)); }

start_logging() {
  mkdir -p "$LOG_DIR"
  : >"$LOG_DIR/run.log"
  exec > >(tee -a "$LOG_DIR/run.log") 2>&1
}

stop_port_forward() {
  if [[ -n "$PF_PID" ]]; then
    kill "$PF_PID" >/dev/null 2>&1 || true
    wait "$PF_PID" 2>/dev/null || true
    PF_PID=""
  fi
}
trap stop_port_forward EXIT

require_tools() {
  local missing=0 t
  for t in docker kind kubectl go; do
    if ! command -v "$t" >/dev/null 2>&1; then
      say "FATAL: missing required tool: $t"
      missing=1
    fi
  done
  [[ "$missing" -eq 0 ]] || exit 2
}

use_context() {
  kubectl config use-context "kind-$CLUSTER" >/dev/null
}

cluster_up() {
  step "kind cluster ($CLUSTER, node $NODE_IMAGE)"
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    info "reusing existing cluster"
  else
    kind create cluster --name "$CLUSTER" --image "$NODE_IMAGE" \
      --config "$E2E/kind.yaml" --wait 240s \
      || { fail "kind create cluster"; return 1; }
  fi
  use_context || { fail "kubectl context kind-$CLUSTER"; return 1; }
  kubectl cluster-info >/dev/null 2>&1 || { fail "cluster unreachable"; return 1; }
  ok "cluster reachable ($(kubectl version -o json 2>/dev/null | sed -n 's/.*"gitVersion": *"\([^"]*\)".*/\1/p' | tail -1))"
}

build_checkpoint_image() {
  step "checkpoint image"
  if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
    if ! docker image inspect "$IMG" >/dev/null 2>&1; then
      fail "SKIP_BUILD=1 but $IMG is absent"
      return 1
    fi
    info "SKIP_BUILD=1: reusing $IMG without a freshness check"
    ok "checkpoint image present"
    return 0
  fi
  "$REPO_ROOT/hack/build-images.sh" --prepare-only >"$LOG_DIR/prepare.log" 2>&1 \
    || { fail "prepare base images"; tail -20 "$LOG_DIR/prepare.log"; return 1; }
  if ! TAG="$TAG" "$REPO_ROOT/hack/build-images.sh" checkpoint >"$LOG_DIR/build.log" 2>&1; then
    fail "build checkpoint image"
    tail -30 "$LOG_DIR/build.log"
    return 1
  fi
  docker image inspect "$IMG" >/dev/null 2>&1 \
    || { fail "built image $IMG not found"; return 1; }
  ok "built $IMG"
}

load_images() {
  step "load images into kind"
  local img
  for img in "$IMG_MINIO" "$IMG_BUSYBOX"; do
    if ! docker image inspect "$img" >/dev/null 2>&1; then
      info "pulling $img on the host"
      docker pull "$img" >"$LOG_DIR/pull-$(basename "${img%%:*}").log" 2>&1 \
        || { fail "pull $img"; return 1; }
    fi
  done
  if ! "$REPO_ROOT/hack/kind-load.sh" "$CLUSTER" "$IMG" "$IMG_MINIO" "$IMG_BUSYBOX" \
      >"$LOG_DIR/kind-load.log" 2>&1; then
    fail "kind load images"
    tail -20 "$LOG_DIR/kind-load.log"
    return 1
  fi
  ok "checkpointd, minio and busybox loaded"
}

deploy_minio() {
  step "MinIO"
  kubectl apply -f "$E2E/manifests/minio.yaml" >/dev/null \
    || { fail "apply minio.yaml"; return 1; }
  kubectl -n "$NS" rollout status deploy/minio --timeout=180s >/dev/null \
    && ok "MinIO ready" || { fail "MinIO rollout"; return 1; }
}

start_port_forward() {
  local local_port="$1" target="$2" i
  kubectl -n "$NS" port-forward "$target" "$local_port:9000" \
    >"$LOG_DIR/port-forward-$local_port.log" 2>&1 &
  PF_PID=$!
  for i in $(seq 1 60); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$local_port") 2>/dev/null; then
      exec 3>&- 3<&-
      return 0
    fi
    if ! kill -0 "$PF_PID" >/dev/null 2>&1; then
      return 1
    fi
    sleep 0.5
  done
  return 1
}

ensure_bucket() {
  step "MinIO bucket"
  start_port_forward 19000 svc/minio \
    || { fail "MinIO port-forward not ready"; stop_port_forward; return 1; }
  if ! (cd "$REPO_ROOT" && go run ./checkpoint/e2e/asserts3 \
      -endpoint 127.0.0.1:19000 -access-key traincache \
      -secret-key traincache-probe -bucket traincache -ensure-bucket) \
      >"$LOG_DIR/bucket.log" 2>&1; then
    fail "create bucket traincache"
    tail -10 "$LOG_DIR/bucket.log"
    stop_port_forward
    return 1
  fi
  stop_port_forward
  ok "$(cat "$LOG_DIR/bucket.log")"
}

deploy_workload() {
  step "checkpoint Secret/ConfigMap and Pod"
  kubectl apply -f "$E2E/manifests/checkpoint-config.yaml" >/dev/null \
    || { fail "apply checkpoint-config.yaml"; return 1; }
  kubectl apply -f "$E2E/manifests/pod.yaml" >/dev/null \
    || { fail "apply pod.yaml"; return 1; }
  ok "workload applied"
}

wait_pod_ready() {
  local timeout="$1"
  local deadline=$((SECONDS + timeout)) ready
  while (( SECONDS < deadline )); do
    ready="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)"
    [[ "$ready" == "True" ]] && return 0
    sleep 2
  done
  return 1
}

wait_log_pattern() {
  local container="$1" pattern="$2" timeout="$3"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if kubectl -n "$NS" logs "$POD" -c "$container" --tail=5000 2>/dev/null | grep -qF "$pattern"; then
      return 0
    fi
    if kubectl -n "$NS" logs "$POD" -c "$container" --tail=5000 --previous 2>/dev/null | grep -qF "$pattern"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

assert_manifest() {
  step "assert S3 manifest ($MANIFEST_KEY)"
  start_port_forward 19000 svc/minio \
    || { fail "MinIO port-forward not ready"; stop_port_forward; return 1; }
  if ! (cd "$REPO_ROOT" && go run ./checkpoint/e2e/asserts3 \
      -endpoint 127.0.0.1:19000 -access-key traincache \
      -secret-key traincache-probe -bucket traincache \
      -key "$MANIFEST_KEY" -task "$TASK" -step "$STEP" -rank 0 -world-size 1) \
      >"$LOG_DIR/asserts3.log" 2>&1; then
    fail "S3 manifest assertion"
    tail -20 "$LOG_DIR/asserts3.log"
    stop_port_forward
    return 1
  fi
  stop_port_forward
  ok "$(tail -1 "$LOG_DIR/asserts3.log")"
}

assert_restored_file() {
  step "assert restored file"
  local deadline=$((SECONDS + 30)) size=""
  while (( SECONDS < deadline )); do
    size="$(kubectl -n "$NS" exec "$POD" -c trainer -- \
      sh -c 'wc -c < /checkpoints-work/t1/step-1/data.bin' 2>/dev/null)" || size=""
    size="${size//[^0-9]/}"
    if [[ "$size" == "1048576" ]]; then
      ok "step-1/data.bin is 1 MiB"
      return 0
    fi
    sleep 1
  done
  fail "step-1/data.bin is not 1 MiB (last size: ${size:-missing})"
  return 1
}

recreate_pod() {
  step "recreate Pod (fresh emptyDir)"
  kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=true --timeout=120s >/dev/null \
    || { fail "delete Pod"; return 1; }
  kubectl apply -f "$E2E/manifests/pod.yaml" >/dev/null \
    || { fail "recreate Pod"; return 1; }
  ok "Pod recreated"
}

dump_state() {
  say ""
  say "---- diagnostics (cluster kept) ----"
  kubectl -n "$NS" get pod "$POD" -o wide 2>&1 || true
  kubectl -n "$NS" describe pod "$POD" 2>&1 | tail -60 || true
  kubectl -n "$NS" logs "$POD" -c checkpointd --tail=120 2>&1 || true
  kubectl -n "$NS" logs "$POD" -c trainer --tail=40 2>&1 || true
  kubectl -n "$NS" get events --sort-by=.lastTimestamp 2>&1 | tail -30 || true
  say "logs: $LOG_DIR/run.log"
}

setup() {
  require_tools
  cluster_up || return 1
  build_checkpoint_image || return 1
  load_images || return 1
  deploy_minio || return 1
  ensure_bucket || return 1
  deploy_workload || return 1
}

run_flow() {
  step "wait for Pod Ready"
  wait_pod_ready 300 || { fail "Pod did not become Ready within 300s"; return 1; }
  ok "Pod Ready"

  step "wait for save of step 1"
  wait_log_pattern checkpointd "上传完成 step 1" 120 \
    || { fail "no \"上传完成 step 1\" in checkpointd logs within 120s"; return 1; }
  ok "step 1 uploaded"

  assert_manifest || return 1

  recreate_pod || return 1

  step "wait for recreated Pod Ready"
  wait_pod_ready 300 || { fail "recreated Pod did not become Ready within 300s"; return 1; }
  ok "recreated Pod Ready"

  assert_restored_file || return 1

  step "wait for restore of step 1"
  wait_log_pattern checkpointd "恢复完成 step 1" 120 \
    || { fail "no \"恢复完成 step 1\" in checkpointd logs within 120s"; return 1; }
  ok "step 1 restored"
}

assert_all() {
  step "assert current Pod is Ready"
  wait_pod_ready 60 || { fail "Pod is not Ready"; return 1; }
  ok "Pod Ready"
  assert_manifest || return 1
  assert_restored_file || return 1
  step "assert restore marker"
  wait_log_pattern checkpointd "恢复完成 step 1" 60 \
    || { fail "no \"恢复完成 step 1\" in current checkpointd logs"; return 1; }
  ok "restore marker present"
}

clean() {
  step "clean"
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    kind delete cluster --name "$CLUSTER" || { fail "kind delete cluster"; return 1; }
    ok "cluster $CLUSTER deleted"
  else
    info "cluster $CLUSTER absent"
  fi
  rm -rf "$LOG_DIR"
  ok "runtime logs removed"
}

main() {
  start_logging
  say "TrainCache checkpoint M6 e2e (mode=$MODE cluster=$CLUSTER)"
  local rc=0
  case "$MODE" in
    clean)  clean || rc=1 ;;
    setup)  setup || rc=1 ;;
    run)    { setup && run_flow; } || rc=1 ;;
    assert) assert_all || rc=1 ;;
    *)
      say "usage: $0 [clean|setup|run|assert]"
      exit 2
      ;;
  esac
  if (( FAILS > 0 )) || (( rc != 0 )); then
    [[ "$MODE" == "clean" ]] || dump_state
    say ""
    say "RESULT: FAIL ($FAILS failure(s)); logs: $LOG_DIR/run.log"
    exit 1
  fi
  say ""
  say "RESULT: PASS"
  exit 0
}

main

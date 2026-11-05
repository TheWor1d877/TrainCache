#!/usr/bin/env bash
# run.sh — checkpoint kind end-to-end: the training Pod opts into
# checkpointing and is admitted through the MutatingWebhookConfiguration, so
# the sidecar under test is the one the webhook injects. The run saves step 1
# to MinIO, recreates the Pod from an empty work volume and restores step 1.
#
# A second mode covers the volume-full failure path: the state volume is a
# tmpfs the trainer fills, so the resume-state write of the S3 plugin fails
# with ENOSPC and the sidecar keeps its working directory.
#
# Usage:
#   ./checkpoint/e2e/run.sh [clean|setup|run|assert|enospc]   (default: run)
#
#   clean   delete the kind cluster and runtime logs
#   setup   create/refresh the cluster, build and load images, start MinIO,
#           create the bucket, deploy the webhook, apply the Secret,
#           ConfigMap and Pod
#   run     setup + admission/save/recreate/restore assertions
#   assert  re-run the assertions against the current cluster (no mutation)
#   enospc  setup + the volume-full failure assertions
#   loop    setup + the 100-step steady-state assertions
#
# Environment overrides:
#   CLUSTER=kind-traincheckpoint-e2e
#   NODE_IMAGE=<kind node image, default kindest/node:v1.37.0>
#   SKIP_BUILD=1   reuse an existing image without a freshness check
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
POD_ENOSPC=traincache-checkpoint-enospc
POD_LOOP=traincache-checkpoint-loop
TASK=t1
STEP=1
# 前缀每次运行唯一：上一轮留下的 step 会被 sidecar 先恢复，同一个 step 不再
# 上传，断言会误判成没上传。
PREFIX="e2e-$(date +%s)"
MANIFEST_KEY="$PREFIX/$TASK/step-$STEP/rank-0/manifest.json"
# 训练程序写的是 1 MiB 全零，恢复后的文件要和它逐字节一致。
EXPECTED_SHA="$(head -c 1048576 /dev/zero | sha256sum | awk '{print $1}')"

IMG="ghcr.io/thewor1d877/traincache/traincache-checkpointd:$TAG"
IMG_WEBHOOK="ghcr.io/thewor1d877/traincache/webhook:$TAG"
IMG_MINIO="pgsty/minio:RELEASE.2026-08-04T00-00-00Z"
IMG_BUSYBOX="busybox:1.36.1"
WEBHOOK_MWC=traincache-pod-warmup

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
  for t in docker kind kubectl go openssl; do
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

build_images() {
  step "images"
  local comp
  for comp in checkpoint webhook; do
    if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
      local img="$IMG"
      [[ "$comp" == "webhook" ]] && img="$IMG_WEBHOOK"
      docker image inspect "$img" >/dev/null 2>&1 \
        || { fail "SKIP_BUILD=1 but $img is absent"; return 1; }
      info "SKIP_BUILD=1: reusing $comp image without a freshness check"
      continue
    fi
    # build-images 只做本地基础镜像检查，不联网；CI runner 上没有这些名字，
    # 基础镜像清单按 Dockerfile 的 FROM 取，不手抄。
    while IFS= read -r base; do
      [[ -n "$base" ]] || continue
      if ! docker image inspect "$base" >/dev/null 2>&1; then
        docker pull "$base" >/dev/null 2>&1 || { fail "pull $base"; return 1; }
      fi
    done < <("$REPO_ROOT/hack/build-images.sh" --list-bases "$comp")
    TAG="$TAG" "$REPO_ROOT/hack/build-images.sh" "$comp" >"$LOG_DIR/build-$comp.log" 2>&1 \
      || { fail "build $comp image"; tail -30 "$LOG_DIR/build-$comp.log"; return 1; }
  done
  docker image inspect "$IMG" >/dev/null 2>&1 \
    || { fail "built image $IMG not found"; return 1; }
  docker image inspect "$IMG_WEBHOOK" >/dev/null 2>&1 \
    || { fail "built image $IMG_WEBHOOK not found"; return 1; }
  ok "checkpointd and webhook images ready"
}

load_images() {
  step "load images into kind"
  local img
  for img in "$IMG" "$IMG_WEBHOOK" "$IMG_MINIO" "$IMG_BUSYBOX"; do
    if ! docker image inspect "$img" >/dev/null 2>&1; then
      info "pulling $(basename "$img") on the host"
      docker pull "$img" >"$LOG_DIR/pull-$(basename "${img%%:*}").log" 2>&1 \
        || { fail "pull $img"; return 1; }
    fi
  done
  "$REPO_ROOT/hack/kind-load.sh" "$CLUSTER" "$IMG" "$IMG_WEBHOOK" "$IMG_MINIO" "$IMG_BUSYBOX" \
    >"$LOG_DIR/kind-load.log" 2>&1 \
    || { fail "kind load images"; tail -20 "$LOG_DIR/kind-load.log"; return 1; }
  ok "checkpointd, webhook, minio and busybox loaded"
}

deploy_minio() {
  step "MinIO"
  kubectl apply -f "$E2E/manifests/minio.yaml" >/dev/null \
    || { fail "apply minio.yaml"; return 1; }
  kubectl -n "$NS" rollout status deploy/minio --timeout=180s >/dev/null \
    && ok "MinIO ready" || { fail "MinIO rollout"; return 1; }
}

# deploy_webhook 装发布清单里的 webhook：TLS Secret、RBAC、Service、
# Deployment 与 MutatingWebhookConfiguration。CA 要写进两条 webhook 条目，
# checkpoint 那条的 failurePolicy 是 Fail，caBundle 为空时带 checkpoint 标签
# 的 Pod 会被准入拒绝。
deploy_webhook() {
  step "webhook (TLS, MutatingWebhookConfiguration, Deployment)"
  local tls="$LOG_DIR/tls" ca_b64
  mkdir -p "$tls"
  openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -keyout "$tls/ca.key" -out "$tls/ca.crt" -subj "/CN=traincache-ca" >/dev/null 2>&1 \
    || { fail "generate webhook CA"; return 1; }
  openssl req -newkey rsa:2048 -nodes -keyout "$tls/tls.key" -out "$tls/tls.csr" \
    -subj "/CN=traincache-webhook.$NS.svc" >/dev/null 2>&1 \
    || { fail "generate webhook CSR"; return 1; }
  printf 'subjectAltName=DNS:traincache-webhook.%s.svc,DNS:traincache-webhook.%s.svc.cluster.local\n' \
    "$NS" "$NS" >"$tls/san.ext"
  openssl x509 -req -in "$tls/tls.csr" -CA "$tls/ca.crt" -CAkey "$tls/ca.key" -CAcreateserial \
    -out "$tls/tls.crt" -days 2 -extfile "$tls/san.ext" >/dev/null 2>&1 \
    || { fail "sign webhook cert"; return 1; }
  kubectl -n "$NS" create secret tls traincache-webhook-tls \
    --cert="$tls/tls.crt" --key="$tls/tls.key" --dry-run=client -o yaml \
    | kubectl apply -f - >/dev/null \
    || { fail "apply webhook TLS secret"; return 1; }

  kubectl apply -k "$REPO_ROOT/controller/config/crd/bases" >/dev/null \
    || { fail "apply CRDs"; return 1; }
  local f
  for f in rbac service deployment mutatingwebhook; do
    kubectl apply -f "$REPO_ROOT/controller/deploy/webhook/$f.yaml" >/dev/null \
      || { fail "apply webhook/$f.yaml"; return 1; }
  done
  # kind 只有一个 control-plane 节点且带 NoSchedule 污点，发布清单不带容忍。
  kubectl -n "$NS" patch deploy traincache-webhook --type=json \
    -p '[{"op":"add","path":"/spec/template/spec/tolerations","value":[{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}]}]' >/dev/null \
    || { fail "patch webhook toleration"; return 1; }

  ca_b64="$(base64 -w0 "$tls/ca.crt")"
  kubectl patch mutatingwebhookconfiguration "$WEBHOOK_MWC" --type=strategic \
    -p "{\"webhooks\":[{\"name\":\"pods.traincache.io\",\"clientConfig\":{\"caBundle\":\"$ca_b64\"}},{\"name\":\"pods-checkpoint.traincache.io\",\"clientConfig\":{\"caBundle\":\"$ca_b64\"}}]}" >/dev/null \
    || { fail "patch webhook caBundle"; return 1; }
  # 镜像 tag 是固定的 dev，Pod 规格没变就不会重建，跑的还是上一轮的二进制。
  kubectl -n "$NS" rollout restart deploy/traincache-webhook >/dev/null \
    || { fail "restart webhook deployment"; return 1; }
  kubectl -n "$NS" rollout status deploy/traincache-webhook --timeout=180s >/dev/null \
    && ok "webhook ready" || { fail "webhook rollout"; return 1; }
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
  sed "s|backendPrefix: e2e|backendPrefix: $PREFIX|" \
    "$E2E/manifests/checkpoint-config.yaml" | kubectl apply -f - >/dev/null \
    || { fail "apply checkpoint-config.yaml"; return 1; }
  # Pod 的 spec 不可变，上一轮的 Pod 要先删掉，否则 apply 改不动它。
  kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=true >/dev/null \
    || { fail "delete pod $POD"; return 1; }
  kubectl apply -f "$E2E/manifests/pod.yaml" >/dev/null \
    || { fail "apply pod.yaml"; return 1; }
  ok "workload applied"
}

wait_pod_ready() {
  local pod="$1" timeout="$2"
  local deadline=$((SECONDS + timeout)) ready
  while (( SECONDS < deadline )); do
    ready="$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)"
    [[ "$ready" == "True" ]] && return 0
    sleep 2
  done
  return 1
}

wait_log_pattern() {
  local pod="$1" container="$2" pattern="$3" timeout="$4"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    # 日志先落文件再 grep：管道里 grep -q 命中即退出会让 kubectl 收到 SIGPIPE，
    # 在 pipefail 下把整条管道判成失败。
    if pod_log "$pod" "$container" | grep -qF "$pattern"; then
      return 0
    fi
    if pod_log "$pod" "$container" --previous | grep -qF "$pattern"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

# pod_log 输出容器日志；--previous 取上一个实例的日志。
pod_log() {
  local pod="$1" container="$2" prev="${3:-}"
  local out="$LOG_DIR/pod-log-$pod-$container${prev:+-prev}.txt"
  if [[ "$prev" == "--previous" ]]; then
    kubectl -n "$NS" logs "$pod" -c "$container" --tail=5000 --previous >"$out" 2>/dev/null
  else
    kubectl -n "$NS" logs "$pod" -c "$container" --tail=5000 >"$out" 2>/dev/null
  fi
  cat "$out"
}

log_has() {
  local pod="$1" container="$2" pattern="$3"
  pod_log "$pod" "$container" | grep -qF "$pattern"
}

# assert_admission_injection 读回准入后的 Pod，确认 sidecar 是 webhook 注入的
# 而不是清单里写死的：清单里只有 trainer 一个容器。
assert_admission_injection() {
  step "assert admission injected the checkpoint sidecar"
  local injected volumes task
  injected="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.spec.initContainers[*].name}' 2>/dev/null)"
  case " $injected " in
    *" checkpointd "*) ok "admission injected the checkpointd init container" ;;
    *) fail "no checkpointd init container (init containers: ${injected:-none})"; return 1 ;;
  esac
  volumes="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.spec.volumes[*].name}' 2>/dev/null)"
  case " $volumes " in
    *" checkpoint-work "*) ok "admission injected the checkpoint-work volume" ;;
    *) fail "no checkpoint-work volume (volumes: ${volumes:-none})"; return 1 ;;
  esac
  task="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.spec.initContainers[?(@.name=="checkpointd")].env[?(@.name=="TASK_ID")].value}' 2>/dev/null)"
  [[ "$task" == "$TASK" ]] \
    && ok "injected sidecar got TASK_ID=$TASK" \
    || fail "injected sidecar TASK_ID = ${task:-none}"
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

file_sha() {
  local pod="$1" path="$2"
  kubectl -n "$NS" exec "$pod" -c trainer -- sha256sum "$path" 2>/dev/null | awk '{print $1}'
}

assert_restored_file() {
  step "assert restored file"
  local want="$1" deadline=$((SECONDS + 60)) got=""
  while (( SECONDS < deadline )); do
    got="$(file_sha "$POD" "/checkpoints-work/$TASK/step-$STEP/data.bin")"
    if [[ -n "$got" && "$got" == "$want" ]]; then
      ok "step-$STEP/data.bin matches the checksum of the uploaded file"
      return 0
    fi
    sleep 1
  done
  fail "step-$STEP/data.bin checksum ${got:-missing} != $want"
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
  kubectl -n "$NS" get pod -o wide 2>&1 || true
  kubectl -n "$NS" describe pod "$POD" 2>&1 | tail -40 || true
  kubectl -n "$NS" logs "$POD" -c checkpointd --tail=120 2>&1 || true
  kubectl -n "$NS" logs "$POD" -c trainer --tail=40 2>&1 || true
  kubectl -n "$NS" logs "$POD_ENOSPC" -c checkpointd --tail=120 2>&1 || true
  kubectl -n "$NS" get events --sort-by=.lastTimestamp 2>&1 | tail -30 || true
  say "logs: $LOG_DIR/run.log"
}

setup() {
  require_tools
  cluster_up || return 1
  build_images || return 1
  load_images || return 1
  deploy_minio || return 1
  ensure_bucket || return 1
  deploy_webhook || return 1
  deploy_workload || return 1
}

run_flow() {
  step "wait for Pod Ready"
  wait_pod_ready "$POD" 300 || { fail "Pod did not become Ready within 300s"; return 1; }
  ok "Pod Ready"

  assert_admission_injection || return 1

  step "wait for save of step 1"
  wait_log_pattern "$POD" checkpointd "上传完成 step 1" 120 \
    || { fail "no \"上传完成 step 1\" in checkpointd logs within 120s"; return 1; }
  ok "step 1 uploaded"

  assert_manifest || return 1

  # 保存成功后 sidecar 会删掉工作卷上的副本，所以比对的是固定内容的校验和。
  recreate_pod || return 1

  step "wait for recreated Pod Ready"
  wait_pod_ready "$POD" 300 || { fail "recreated Pod did not become Ready within 300s"; return 1; }
  ok "recreated Pod Ready"

  assert_restored_file "$EXPECTED_SHA" || return 1

  step "wait for restore of step 1"
  wait_log_pattern "$POD" checkpointd "恢复完成 step 1" 120 \
    || { fail "no \"恢复完成 step 1\" in checkpointd logs within 120s"; return 1; }
  ok "step 1 restored"
}

# run_enospc_scenario 覆盖写满的失败路径：状态卷是 tmpfs，训练程序把它写满，
# S3 插件写续传状态时拿到 ENOSPC。断言 sidecar 保留工作目录、停止接收新
# step，且不会把 step 上传成功。
run_enospc_scenario() {
  step "volume-full scenario: apply pod-enospc.yaml"
  kubectl -n "$NS" delete pod "$POD_ENOSPC" --ignore-not-found --wait=true >/dev/null \
    || { fail "delete pod $POD_ENOSPC"; return 1; }
  kubectl apply -f "$E2E/manifests/pod-enospc.yaml" >/dev/null \
    || { fail "apply pod-enospc.yaml"; return 1; }

  step "wait for the ENOSPC Pod Ready"
  wait_pod_ready "$POD_ENOSPC" 300 \
    || { fail "ENOSPC Pod did not become Ready within 300s"; return 1; }
  ok "ENOSPC Pod Ready"

  step "wait for the final save failure"
  wait_log_pattern "$POD_ENOSPC" checkpointd "保存最终失败" 300 \
    || { fail "no \"保存最终失败\" in checkpointd logs within 300s"; return 1; }
  ok "checkpointd reported the final save failure"

  log_has "$POD_ENOSPC" checkpointd "no space left on device" \
    && ok "the failure carries the ENOSPC errno" \
    || fail "no ENOSPC text in the checkpointd logs"

  local kept
  kept="$(kubectl -n "$NS" exec "$POD_ENOSPC" -c trainer -- \
    ls /checkpoints-work/$TASK/step-$STEP/data.bin 2>/dev/null)"
  [[ "$kept" == *data.bin ]] \
    && ok "the working directory is kept after the failure" \
    || fail "the working directory was removed (ls: ${kept:-missing})"

  log_has "$POD_ENOSPC" checkpointd "上传完成 step 1" \
    && fail "step 1 was uploaded although the save failed" \
    || ok "step 1 was not uploaded"

  # step 2 在失败之后写出；sidecar 停止接收新 step，所以它不会被上传。
  step "wait for the step-2 window to pass"
  sleep 150
  log_has "$POD_ENOSPC" checkpointd "上传完成 step 2" \
    && fail "step 2 was uploaded after the sidecar stopped accepting steps" \
    || ok "step 2 was not uploaded"

  step "assert the sidecar is still running"
  wait_pod_ready "$POD_ENOSPC" 30 \
    && ok "the sidecar survived the failure" \
    || fail "the sidecar is not Ready after the failure"
}

assert_all() {
  step "assert current Pod is Ready"
  wait_pod_ready "$POD" 60 || { fail "Pod is not Ready"; return 1; }
  ok "Pod Ready"
  assert_admission_injection || return 1
  assert_manifest || return 1
  step "assert restore marker"
  wait_log_pattern "$POD" checkpointd "恢复完成 step 1" 60 \
    || { fail "no \"恢复完成 step 1\" in current checkpointd logs"; return 1; }
  ok "restore marker present"
}

run_loop_scenario() {
  step "steady-state loop: apply pod-loop.yaml"
  # Pod 的 spec 不可变，上一轮的 Pod 要先删掉，否则 apply 不会换掉它。
  kubectl -n "$NS" delete pod "$POD_LOOP" --ignore-not-found --wait=true >/dev/null \
    || { fail "delete pod $POD_LOOP"; return 1; }
  sed "s|\"prefix\":\"e2e-loop\"|\"prefix\":\"e2e-loop-$PREFIX\"|" \
    "$E2E/manifests/pod-loop.yaml" | kubectl apply -f - >/dev/null \
    || { fail "apply pod-loop.yaml"; return 1; }

  step "wait for the loop Pod Ready"
  wait_pod_ready "$POD_LOOP" 300 \
    || { fail "loop Pod did not become Ready within 300s"; return 1; }
  ok "loop Pod Ready"

  # 句柄增长看的是 sidecar 进程打开的文件数，所以在第 20 步之后取一次基线。
  step "wait for the first 20 steps"
  wait_log_pattern "$POD_LOOP" trainer "trainer: step 20 done" 600 \
    || { fail "the first 20 steps did not finish within 600s"; return 1; }
  local fds_base
  fds_base="$(sidecar_fds)"
  [[ -n "$fds_base" ]] \
    && ok "sidecar open fds after 20 steps: $fds_base" \
    || fail "cannot read the sidecar fd count"

  step "wait for the 100-step loop"
  wait_log_pattern "$POD_LOOP" trainer "trainer: loop done: 100 steps" 1800 \
    || { fail "the 100-step loop did not finish within 1800s"; return 1; }
  ok "the trainer wrote 100 steps"

  step "count the uploads"
  # 日志先落文件再 grep：管道里 grep -q 命中即退出会让 kubectl 收到 SIGPIPE，
  # 在 pipefail 下把整条管道判成失败。
  local logs="$LOG_DIR/loop-checkpointd.log"
  kubectl -n "$NS" logs "$POD_LOOP" -c checkpointd --tail=20000 >"$logs" 2>/dev/null
  local uploads
  uploads="$(grep -c '上传完成 step ' "$logs")"
  [[ "$uploads" == "100" ]] \
    && ok "100 uploads reported" \
    || fail "expected 100 uploads, got ${uploads:-0}"
  local missing
  missing=""
  for n in 1 2 3 50 99 100; do
    grep -qF "上传完成 step $n" "$logs" || missing="$missing $n"
  done
  [[ -z "$missing" ]] \
    && ok "the sampled steps are all uploaded" \
    || fail "no upload log for steps:$missing"

  step "assert the working directory and the cache"
  local left cache
  left="$(kubectl -n "$NS" exec "$POD_LOOP" -c trainer -- ls -1 /checkpoints-work/$TASK 2>/dev/null | wc -l)"
  [[ "$left" == "0" ]] \
    && ok "no working copy is left behind" \
    || fail "the working directory still holds ${left} entries"
  cache="$(kubectl -n "$NS" exec "$POD_LOOP" -c trainer -- ls -1 /checkpoints-nvme/$TASK 2>/dev/null | wc -l)"
  [[ "$cache" -le 2 ]] \
    && ok "the NVMe cache holds ${cache} entries (retention 1)" \
    || fail "the NVMe cache holds ${cache} entries, retention is 1"

  step "assert the sidecar handles"
  local fds_end
  fds_end="$(sidecar_fds)"
  [[ -n "$fds_end" && -n "$fds_base" ]] \
    && ok "sidecar open fds: ${fds_base} after 20 steps, ${fds_end} after 100 steps" \
    || fail "cannot compare the sidecar fd counts"
  if [[ -n "$fds_end" && -n "$fds_base" ]] && (( fds_end - fds_base <= 4 )); then
    ok "the open fd count did not grow with the step count"
  else
    fail "the open fd count grew from ${fds_base:-?} to ${fds_end:-?}"
  fi

  step "assert the sidecar is still running"
  wait_pod_ready "$POD_LOOP" 30 \
    && ok "the sidecar survived the loop" \
    || fail "the sidecar is not Ready after the loop"
}

# sidecar_fds 返回 sidecar 进程打开的文件数；读不到时返回空串。
sidecar_fds() {
  kubectl -n "$NS" exec "$POD_LOOP" -c checkpointd -- \
    sh -c 'ls /proc/1/fd | wc -l' 2>/dev/null | tr -d '[:space:]'
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
  say "TrainCache checkpoint e2e (mode=$MODE cluster=$CLUSTER)"
  local rc=0
  case "$MODE" in
    clean)  clean || rc=1 ;;
    setup)  setup || rc=1 ;;
    run)    { setup && run_flow; } || rc=1 ;;
    assert) assert_all || rc=1 ;;
    enospc) { setup && run_enospc_scenario; } || rc=1 ;;
    loop)   { setup && run_loop_scenario; } || rc=1 ;;
    *)
      say "usage: $0 [clean|setup|run|assert|enospc|loop]"
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

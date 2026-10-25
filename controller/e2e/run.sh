#!/usr/bin/env bash
# run.sh — TrainCache M11 kind end-to-end: DataSet + WarmupTask -> preheat ->
# gate release -> scheduler scoring.
#
# Usage:
#   ./e2e/run.sh [clean|setup|run|assert]   (default: run)
#
#   clean   delete the kind cluster and runtime artifacts
#   setup   create/refresh cluster, build+load images, deploy control plane,
#           MinIO, credentials and publish the test snapshot
#   run     setup + workload assertions (idempotent)
#   assert  re-run the assertions against the current cluster
#
# Environment overrides:
#   CLUSTER=traincache-e2e  REGISTRY=<mirror>  TAG=0.8.0  FORCE_BUILD=1
#   SKIP_BUILD=1            (reuse images without a freshness check; may test
#                            stale code, never the default)
#   SKIP_NODE_LABELS=1      (skip the simulated admin labelling step)
#
# Images are rebuilt automatically when any of their build inputs (the
# go.work files plus the per-component source trees listed next to each
# build_image call) is newer than the image's creation time.
#
# The s3-access Node label is NEVER written by TrainCache components. This
# script performs the labelling once in setup as an explicit simulation of the
# cluster administrator's manual action (docs/7.0, D30). Set
# SKIP_NODE_LABELS=1 and label the workers yourself to avoid it entirely.
#
# Exit codes: 0 all assertions passed; 1 an assertion/deployment failed;
#             2 usage or missing tool.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DEPLOY="$REPO_ROOT/controller/deploy"
E2E="$SCRIPT_DIR"
# Artifacts live in the runtime data layer (sibling of the repo), not in the
# working tree. TRAINCACHE_DATA_ROOT overrides it (CI points it inside the workspace).
DATA_ROOT="${TRAINCACHE_DATA_ROOT:-$(cd "$REPO_ROOT/.." && pwd)/traincachedata}"
RESULTS="$DATA_ROOT/controller/e2e/results"
LOGS="$RESULTS/logs"
DATA="$RESULTS/data"
TLS="$RESULTS/tls"

CLUSTER="${CLUSTER:-traincache-e2e}"
REGISTRY="${REGISTRY:-dgl3i4ucowsgvvdwty.xuanyuan.run}"
TAG="${TAG:-0.8.0}"
NODE_IMAGE="${NODE_IMAGE:-$REGISTRY/kindest/node:v1.37.0}"
IMG_MANAGER="$REGISTRY/traincache/controller-manager:$TAG"
IMG_WEBHOOK="$REGISTRY/traincache/webhook:$TAG"
IMG_SCHEDULER="$REGISTRY/traincache/kube-scheduler:$TAG"
IMG_AGENT="$REGISTRY/traincache/traincache-agentd:$TAG"
# Third-party fixtures are preloaded too: kind's containerd does not use the
# host docker registry mirror and cannot always reach the mirror domain.
IMG_MINIO="$REGISTRY/pgsty/minio:RELEASE.2026-08-04T00-00-00Z"
IMG_BUSYBOX="$REGISTRY/library/busybox:1.36.1"

NS=traincache
TENANT=traincache-e2e
TASK=train-task
DATASET=train-data
JOB=train-job
TASK_REF="$TENANT/$TASK"
SCORE_PREFIX="traincache.io/scores"
GATE="traincache.io/warmup"
WEBHOOK_SVC="traincache-webhook.$NS.svc"

CHUNK_SIZE=1048576
SHARD_BYTES=$((8 * 1024 * 1024)) # 8 chunks per shard

FAILS=0
MODE="${1:-run}"
RUN_STARTED="$(date +%s)"
PF_PID=""

say()  { printf '%s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
ok()   { printf '  [OK]   %s\n' "$*"; }
info() { printf '  [INFO] %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; FAILS=$((FAILS + 1)); }
skip() { printf '  [SKIP] %s\n' "$*"; }
warn() { printf '  [WARN] %s\n' "$*"; }

start_logging() {
  mkdir -p "$RESULTS" "$LOGS" "$DATA" "$TLS"
  : >"$RESULTS/run.log"
  exec > >(tee -a "$RESULTS/run.log") 2>&1
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
  for t in docker kind kubectl jq openssl go; do
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

clean() {
  step "clean"
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    kind delete cluster --name "$CLUSTER"
    ok "cluster $CLUSTER deleted"
  else
    info "cluster $CLUSTER absent"
  fi
  clean_artifacts
  ok "runtime artifacts removed (results/.gitignore kept)"
}

# 只清理本脚本自己写出的那几类文件，不整目录递归删：数据层只追加不覆盖，
# `rm -rf "$RESULTS/data"` 会连历史轮次的产物一起删掉。目录本身保留。
clean_artifacts() {
  local f
  # 顶层：只有本脚本 collect_artifacts / write_summary 写出的那几个文件。
  for f in run.log publish.txt summary.md \
           dataset.yaml warmuptask.yaml pods.yaml nodes.yaml \
           events.txt scheduler-deploy.yaml; do
    rm -f "$RESULTS/$f"
  done
  # logs/：本脚本的运行日志。Pod 名带随机后缀，只能整类清，
  # 否则上一轮的 agent-*.log 会混进本轮的断言。
  rm -f "$LOGS"/*.log
  # data/ 与 tls/ 不删：分片由 dd 每轮整体覆写、证书由 openssl 每轮重新生成，
  # 留着旧文件不影响本轮，且避免动数据层里的实体产物。
}

cluster_up() {
  step "kind cluster ($CLUSTER, node $NODE_IMAGE)"
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    info "reusing existing cluster"
  else
    kind create cluster --name "$CLUSTER" --image "$NODE_IMAGE" \
      --config "$E2E/kind.yaml" --wait 240s || { fail "kind create cluster"; return 1; }
  fi
  use_context || { fail "kubectl context kind-$CLUSTER"; return 1; }
  kubectl cluster-info >/dev/null 2>&1 || { fail "cluster unreachable"; return 1; }
  ok "cluster reachable ($(kubectl version -o json 2>/dev/null | jq -r '.serverVersion.gitVersion // "unknown"'))"
}

# image_is_fresh IMAGE PATH... — true when every file under the given source
# paths is older than the image's creation time. Uses a temp reference file so
# find's -newer comparison keeps the image timestamp's sub-second precision.
image_is_fresh() {
  local image="$1"; shift
  local created ref newer
  created="$(docker image inspect -f '{{.Created}}' "$image" 2>/dev/null)" || return 1
  [[ -n "$created" ]] || return 1
  ref="$(mktemp "${TMPDIR:-/tmp}/traincache-imgtime.XXXXXX")" || return 1
  if ! touch -d "$created" "$ref" 2>/dev/null; then
    rm -f "$ref"
    return 1
  fi
  newer="$(find "$@" -type f -newer "$ref" -print -quit 2>/dev/null)"
  rm -f "$ref"
  [[ -z "$newer" ]]
}

build_image() {
  local name="$1" image="$2" dockerfile="$3" context="$4"
  shift 4
  if [[ "${SKIP_BUILD:-0}" == "1" ]] && docker image inspect "$image" >/dev/null 2>&1; then
    warn "SKIP_BUILD=1: reusing $name image without a freshness check; may test stale code ($image)"
    return 0
  fi
  if [[ "${FORCE_BUILD:-0}" == "1" ]]; then
    info "rebuild because $name: FORCE_BUILD=1"
  elif ! docker image inspect "$image" >/dev/null 2>&1; then
    info "building $name: image not found"
  elif image_is_fresh "$image" "$@"; then
    info "image $name already built ($image)"
    return 0
  else
    info "rebuild because $name: source newer than image"
  fi
  local log="$LOGS/build-$name.log"
  info "building $name -> $image (log: $log)"
  if ! docker build -f "$dockerfile" -t "$image" "$context" >"$log" 2>&1; then
    fail "build $name"
    tail -20 "$log"
    return 1
  fi
  ok "built $name"
}

load_image() {
  local image="$1"
  kind load docker-image --name "$CLUSTER" "$image" >/dev/null 2>&1 \
    && ok "loaded $(basename "$image")" \
    || { fail "kind load $image"; return 1; }
}

build_images() {
  step "images"
  # go.work references all three modules, so every build needs a context with
  # agent/, core/ and controller/. Stage just those instead of sending the
  # multi-gigabyte repository root to the Docker daemon.
  local stage
  stage="$(mktemp -d "${TMPDIR:-/tmp}/traincache-build.XXXXXX")"
  cp -a "$REPO_ROOT/go.work" "$REPO_ROOT/go.work.sum" "$REPO_ROOT/agent" "$REPO_ROOT/core" "$REPO_ROOT/controller" "$stage/" \
    || { fail "stage build context"; rm -rf "$stage"; return 1; }
  # Per-component build inputs. The Dockerfiles copy the whole workspace, but
  # controller binaries only import controller/, core/ and agent/api while
  # agentd imports agent/ and core/. Checking each component separately means
  # an agent-only edit does not needlessly rebuild the control plane.
  local controller_src=(
    "$REPO_ROOT/go.work" "$REPO_ROOT/go.work.sum"
    "$REPO_ROOT/controller/cmd" "$REPO_ROOT/controller/internal"
    "$REPO_ROOT/controller/api" "$REPO_ROOT/controller/config"
    "$REPO_ROOT/controller/deploy"
    "$REPO_ROOT/core" "$REPO_ROOT/agent/api"
  )
  local agent_src=(
    "$REPO_ROOT/go.work" "$REPO_ROOT/go.work.sum"
    "$REPO_ROOT/agent" "$REPO_ROOT/core"
  )
  build_image manager "$IMG_MANAGER" "$REPO_ROOT/controller/deploy/manager/Dockerfile" "$stage" "${controller_src[@]}" || { rm -rf "$stage"; return 1; }
  build_image webhook "$IMG_WEBHOOK" "$REPO_ROOT/controller/deploy/webhook/Dockerfile" "$stage" "${controller_src[@]}" || { rm -rf "$stage"; return 1; }
  build_image scheduler "$IMG_SCHEDULER" "$REPO_ROOT/controller/deploy/scheduler/Dockerfile" "$stage" "${controller_src[@]}" || { rm -rf "$stage"; return 1; }
  build_image agent "$IMG_AGENT" "$REPO_ROOT/agent/Dockerfile" "$stage" "${agent_src[@]}" || { rm -rf "$stage"; return 1; }
  rm -rf "$stage"

  load_image "$IMG_MANAGER" || return 1
  load_image "$IMG_WEBHOOK" || return 1
  load_image "$IMG_SCHEDULER" || return 1
  load_image "$IMG_AGENT" || return 1

  local img
  for img in "$IMG_MINIO" "$IMG_BUSYBOX"; do
    if ! docker image inspect "$img" >/dev/null 2>&1; then
      info "pulling $img on the host (docker uses the registry mirror)"
      docker pull "$img" >"$LOGS/pull-$(basename "${img%%:*}").log" 2>&1 \
        || { fail "pull $img"; return 1; }
    fi
    load_image "$img" || return 1
  done
}

make_tls_secret() {
  step "webhook TLS"
  kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  local ca_key="$TLS/ca.key" ca_crt="$TLS/ca.crt" key="$TLS/tls.key" csr="$TLS/tls.csr" crt="$TLS/tls.crt"
  openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -keyout "$ca_key" -out "$ca_crt" -subj "/CN=traincache-e2e-ca" >/dev/null 2>&1 \
    || { fail "generate CA"; return 1; }
  openssl req -newkey rsa:2048 -nodes -keyout "$key" -out "$csr" \
    -subj "/CN=$WEBHOOK_SVC" >/dev/null 2>&1 || { fail "generate webhook CSR"; return 1; }
  printf 'subjectAltName=DNS:%s,DNS:%s.%s.svc.cluster.local\n' "$WEBHOOK_SVC" "traincache-webhook" "$NS" >"$TLS/san.ext"
  openssl x509 -req -in "$csr" -CA "$ca_crt" -CAkey "$ca_key" -CAcreateserial \
    -out "$crt" -days 2 -extfile "$TLS/san.ext" >/dev/null 2>&1 \
    || { fail "sign webhook cert"; return 1; }
  kubectl -n "$NS" create secret tls traincache-webhook-tls \
    --cert="$crt" --key="$key" --dry-run=client -o yaml | kubectl apply -f - >/dev/null \
    || { fail "apply webhook TLS secret"; return 1; }
  ok "TLS secret traincache-webhook-tls"
}

deploy_minio() {
  step "MinIO"
  # Recreate the Deployment so a previous failed image pull (before the image
  # was loaded into kind) does not leave the deployment at its progress
  # deadline. The PVC survives, so previously published data is retained.
  kubectl -n minio delete deploy minio --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl apply -f "$E2E/manifests/minio.yaml" >/dev/null || { fail "apply minio"; return 1; }
  kubectl -n minio rollout status deploy/minio --timeout=180s >/dev/null \
    && ok "MinIO ready" || { fail "MinIO rollout"; return 1; }
}

deploy_control_plane() {
  step "control plane manifests"
  # Tenant namespace + credentials (also the Agent's copy in traincache).
  kubectl apply -f "$E2E/manifests/tenant.yaml" >/dev/null || { fail "apply tenant.yaml"; return 1; }
  kubectl apply -k "$DEPLOY" >/dev/null || { fail "kubectl apply -k deploy"; return 1; }

  local ca_b64
  ca_b64="$(base64 -w0 "$TLS/ca.crt")"
  kubectl patch mutatingwebhookconfiguration traincache-pod-warmup --type=json \
    -p "[{\"op\":\"replace\",\"path\":\"/webhooks/0/clientConfig/caBundle\",\"value\":\"$ca_b64\"}]" >/dev/null \
    || { fail "patch webhook caBundle"; return 1; }
  ok "manifests applied and webhook CA injected"

  # Images keep a fixed tag (never :latest); force Pods to pick up the image
  # freshly loaded into kind instead of reusing a previous run's container.
  kubectl -n "$NS" rollout restart deploy/traincache-controller-manager deploy/traincache-webhook >/dev/null
  kubectl -n kube-system rollout restart deploy/traincache-scheduler >/dev/null
  kubectl -n "$NS" rollout restart ds/traincache-agentd >/dev/null

  kubectl -n "$NS" rollout status deploy/traincache-controller-manager --timeout=180s >/dev/null \
    && ok "controller-manager ready" || { fail "controller-manager rollout"; return 1; }
  kubectl -n "$NS" rollout status deploy/traincache-webhook --timeout=180s >/dev/null \
    && ok "webhook ready" || { fail "webhook rollout"; return 1; }
  kubectl -n kube-system rollout status deploy/traincache-scheduler --timeout=180s >/dev/null \
    && ok "scheduler ready" || { fail "scheduler rollout"; return 1; }
  kubectl -n "$NS" rollout status ds/traincache-agentd --timeout=240s >/dev/null \
    && ok "agent DaemonSet ready" || { fail "agent DaemonSet rollout"; return 1; }
}

label_workers() {
  step "s3-access node labels (manual admin action simulation)"
  local all workers spare
  all="$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"
  [[ -n "$all" ]] || { fail "no worker nodes"; return 1; }
  # GWAA only considers s3-access nodes. Label the first two workers so the
  # workload pins Pods to worker2/worker3: the Pod assigned to the unlabelled
  # third Node's sibling ("worker") must land elsewhere, which makes the
  # NodeDeviation scenario deterministic while leaving two feasible Nodes.
  workers="$(printf '%s\n' "$all" | head -2)"
  spare="$(printf '%s\n' "$all" | tail -n +3)"
  if [[ "${SKIP_NODE_LABELS:-0}" == "1" ]]; then
    info "SKIP_NODE_LABELS=1; assuming the administrator labelled: $workers and left unlabelled: ${spare:-none}"
    return 0
  fi
  local n
  for n in $workers; do
    kubectl label node "$n" traincache.io/s3-access=true --overwrite >/dev/null \
      && ok "labelled $n (simulated admin action, not a TrainCache component)" \
      || fail "label node $n"
  done
  [[ -z "$spare" ]] || info "left without s3-access (deviation target): $spare"
}

publish_data() {
  step "publish test snapshot to MinIO"
  kubectl -n minio port-forward svc/minio 19000:9000 >"$LOGS/port-forward.log" 2>&1 &
  PF_PID=$!
  local i ready=0
  for i in $(seq 1 40); do
    if (exec 3<>/dev/tcp/127.0.0.1/19000) 2>/dev/null; then exec 3>&- 3<&-; ready=1; break; fi
    sleep 0.5
  done
  if [[ "$ready" != "1" ]]; then fail "MinIO port-forward not ready"; stop_port_forward; return 1; fi

  if ! (cd "$REPO_ROOT" && go run ./controller/e2e/mkbucket \
      --endpoint 127.0.0.1:19000 --access-key traincache \
      --secret-key traincache-probe --bucket traincache) >"$LOGS/mkbucket.log" 2>&1; then
    fail "create MinIO bucket"
    tail -10 "$LOGS/mkbucket.log"
    stop_port_forward
    return 1
  fi

  dd if=/dev/urandom of="$DATA/shard-000.tar" bs=1M count=8 status=none
  dd if=/dev/urandom of="$DATA/shard-001.tar" bs=1M count=8 status=none
  local out
  out="$(cd "$REPO_ROOT" && go run ./core/cmd/trainpublish \
    --dataset "$DATASET" --endpoint 127.0.0.1:19000 \
    --access-key traincache --secret-key traincache-probe --bucket traincache \
    --chunk-size "$CHUNK_SIZE" --concurrency 4 \
    "$DATA/shard-000.tar" "$DATA/shard-001.tar" 2>>"$LOGS/trainpublish.log")"
  local rc=$?
  stop_port_forward
  if [[ "$rc" -ne 0 ]]; then
    fail "trainpublish"
    tail -10 "$LOGS/trainpublish.log"
    return 1
  fi
  printf '%s\n' "$out" | tee "$RESULTS/publish.txt"
  ok "snapshot published"
}

setup() {
  require_tools
  cluster_up || return 1
  build_images || return 1
  deploy_minio || return 1
  make_tls_secret || return 1
  label_workers || return 1
  publish_data || return 1
  deploy_control_plane || return 1
}

scale_manager() {
  local replicas="$1"
  kubectl -n "$NS" scale deploy/traincache-controller-manager --replicas="$replicas" >/dev/null
  if [[ "$replicas" == "0" ]]; then
    for i in $(seq 1 60); do
      [[ -z "$(kubectl -n "$NS" get pods -l app.kubernetes.io/name=traincache-controller-manager -o name)" ]] && return 0
      sleep 1
    done
    return 1
  fi
  kubectl -n "$NS" rollout status deploy/traincache-controller-manager --timeout=180s >/dev/null
}

wait_dataset_ready() {
  local deadline=$((SECONDS + 180)) phase
  while (( SECONDS < deadline )); do
    phase="$(kubectl -n "$TENANT" get dataset "$DATASET" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)"
    if [[ "$phase" == "True" ]]; then return 0; fi
    sleep 2
  done
  return 1
}

wait_workload_pods() {
  local deadline=$((SECONDS + 180)) count
  while (( SECONDS < deadline )); do
    count="$(kubectl -n "$TENANT" get pods -l job-name="$JOB" --no-headers 2>/dev/null | wc -l)"
    if [[ "$count" -eq 2 ]]; then
      local p injected=0
      for p in $(kubectl -n "$TENANT" get pods -l job-name="$JOB" -o name); do
        local task_ann gate
        task_ann="$(kubectl -n "$TENANT" get "$p" -o jsonpath='{.metadata.annotations.traincache\.io/warmup-task}' 2>/dev/null)"
        gate="$(kubectl -n "$TENANT" get "$p" -o jsonpath='{range .spec.schedulingGates[*]}{.name}{"\n"}{end}' 2>/dev/null)"
        if [[ "$task_ann" == "$TASK_REF" ]] && grep -qx "$GATE" <<<"$gate"; then
          injected=$((injected + 1))
        fi
      done
      [[ "$injected" -eq 2 ]] && return 0
    fi
    sleep 2
  done
  return 1
}

wait_task_phase() {
  local want="$1" timeout="$2" deadline=$((SECONDS + $2)) phase
  while (( SECONDS < deadline )); do
    phase="$(kubectl -n "$TENANT" get warmuptask "$TASK" -o jsonpath='{.status.phase}' 2>/dev/null)"
    [[ "$phase" == "$want" ]] && return 0
    case "$phase" in Degraded|Failed) return 2;; esac
    sleep 2
  done
  return 1
}

wait_gates_removed() {
  local deadline=$((SECONDS + $1)) p gate
  while (( SECONDS < deadline )); do
    local remaining=0
    for p in $(kubectl -n "$TENANT" get pods -l job-name="$JOB" -o name); do
      gate="$(kubectl -n "$TENANT" get "$p" -o jsonpath='{range .spec.schedulingGates[*]}{.name}{"\n"}{end}' 2>/dev/null)"
      grep -qx "$GATE" <<<"$gate" && remaining=$((remaining + 1))
    done
    [[ "$remaining" -eq 0 ]] && return 0
    sleep 2
  done
  return 1
}

wait_pods_running() {
  local timeout="$1" deadline=$((SECONDS + $1)) phase
  while (( SECONDS < deadline )); do
    phase="$(kubectl -n "$TENANT" get pods -l job-name="$JOB" --no-headers 2>/dev/null | awk '{print $3}' | sort -u | tr '\n' ' ')"
    [[ "$phase" == "Running " ]] && return 0
    sleep 2
  done
  return 1
}

score_key() {
  local ns="$1" name="$2"
  local prefix="$name"
  if (( ${#prefix} > 32 )); then prefix="${prefix:0:32}"; fi
  prefix="$(printf '%s' "$prefix" | sed -e 's/[-.]*$//')"
  local hash
  hash="$(printf '%s/%s' "$ns" "$name" | sha256sum | cut -c1-12)"
  printf '%s-%s-%s' "$SCORE_PREFIX" "$prefix" "$hash"
}

collect_artifacts() {
  mkdir -p "$LOGS"
  kubectl -n "$NS" logs deploy/traincache-controller-manager --tail=5000 >"$LOGS/controller-manager.log" 2>&1 || true
  kubectl -n "$NS" logs deploy/traincache-webhook --tail=5000 >"$LOGS/webhook.log" 2>&1 || true
  kubectl -n kube-system logs deploy/traincache-scheduler --tail=20000 >"$LOGS/scheduler.log" 2>&1 || true
  kubectl -n minio logs deploy/minio --tail=2000 >"$LOGS/minio.log" 2>&1 || true
  local p
  for p in $(kubectl -n "$NS" get pods -l app=traincache-agentd -o name); do
    kubectl -n "$NS" logs "$p" --tail=3000 >"$LOGS/agent-${p##*/}.log" 2>&1 || true
  done
  kubectl -n "$TENANT" get dataset "$DATASET" -o yaml >"$RESULTS/dataset.yaml" 2>&1 || true
  kubectl -n "$TENANT" get warmuptask "$TASK" -o yaml >"$RESULTS/warmuptask.yaml" 2>&1 || true
  kubectl -n "$TENANT" get pods -l job-name="$JOB" -o yaml >"$RESULTS/pods.yaml" 2>&1 || true
  kubectl get nodes -o yaml >"$RESULTS/nodes.yaml" 2>&1 || true
  kubectl -n "$TENANT" get events --sort-by=.lastTimestamp >"$RESULTS/events.txt" 2>&1 || true
  kubectl -n kube-system get deploy/traincache-scheduler -o yaml >"$RESULTS/scheduler-deploy.yaml" 2>&1 || true
}

assert_webhook_injection() {
  step "assert 1: webhook injected gate (Pods Pending)"
  local p index task_ann gates phase scheduled reason
  for p in $(kubectl -n "$TENANT" get pods -l job-name="$JOB" -o name); do
    task_ann="$(kubectl -n "$TENANT" get "$p" -o jsonpath='{.metadata.annotations.traincache\.io/warmup-task}')"
    index="$(kubectl -n "$TENANT" get "$p" -o jsonpath='{.metadata.annotations.traincache\.io/pod-index}')"
    gates="$(kubectl -n "$TENANT" get "$p" -o jsonpath='{range .spec.schedulingGates[*]}{.name}{"\n"}{end}')"
    phase="$(kubectl -n "$TENANT" get "$p" -o jsonpath='{.status.phase}')"
    scheduled="$(kubectl -n "$TENANT" get "$p" -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].reason}')"
    reason="$(kubectl -n "$TENANT" get "$p" -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].message}')"
    [[ "$task_ann" == "$TASK_REF" ]] && ok "$p annotation warmup-task=$task_ann" || fail "$p annotation warmup-task=$task_ann"
    [[ "$index" =~ ^[01]$ ]] && ok "$p annotation pod-index=$index" || fail "$p annotation pod-index='$index'"
    grep -qx "$GATE" <<<"$gates" && ok "$p schedulingGates contains $GATE" || fail "$p missing scheduling gate"
    [[ "$phase" == "Pending" ]] && ok "$p phase=Pending while gated" \
      || fail "$p expected Pending while gated, got phase=$phase"
    info "$p PodScheduled reason=$scheduled message=${reason:-none}"
  done
}

assert_preheat_and_release() {
  step "assert 2: preheat progress and gate release"
  local phase gate_state ready_rows pod_count
  phase="$(kubectl -n "$TENANT" get warmuptask "$TASK" -o jsonpath='{.status.phase}')"
  gate_state="$(kubectl -n "$TENANT" get warmuptask "$TASK" -o jsonpath='{.status.gateDecision.state}')"
  ready_rows="$(kubectl -n "$TENANT" get warmuptask "$TASK" -o json | jq '[.status.podNodeReadiness[]?] | length')"
  pod_count="$(kubectl -n "$TENANT" get warmuptask "$TASK" -o jsonpath='{.spec.podCount}')"
  [[ "$phase" == "Released" ]] && ok "WarmupTask phase=Released" || fail "WarmupTask phase=$phase"
  [[ "$gate_state" == "Released" ]] && ok "gateDecision.state=Released ($(kubectl -n "$TENANT" get warmuptask "$TASK" -o jsonpath='{.status.gateDecision.reason}'))" \
    || fail "gateDecision.state=$gate_state"
  # M12/S15: exactly one PodNodeReadiness entry per Pod; candidate-node
  # granularity lives in status.readinessTable.
  [[ "$ready_rows" -eq "$pod_count" ]] && ok "preheat progress rows=$ready_rows (one per Pod)" \
    || fail "podNodeReadiness rows=$ready_rows, want podCount=$pod_count"
  # The readiness facts must show actual ready blocks on the assigned nodes.
  local progress
  progress="$(kubectl -n "$TENANT" get warmuptask "$TASK" -o json | jq '[.status.podNodeReadiness[]? | select((.readyInWindow // 0) > 0 or ((.readyBlockBitmap // "") | length > 0))] | length')"
  [[ "$progress" -ge 1 ]] && ok "ready-block facts observed ($progress Pod(s))" || fail "no ready-block progress recorded"
  local consumed
  consumed="$(kubectl -n "$TENANT" get warmuptask "$TASK" -o jsonpath='{.status.consumeOrderSource}')"
  info "consumeOrderSource=$consumed"

  # Hard log assertion: the control plane must show accepted submits and the
  # release transition in kubectl logs.
  local mlog="$LOGS/controller-manager.log"
  kubectl -n "$NS" logs deploy/traincache-controller-manager --tail=5000 >"$mlog" 2>&1 || true
  grep -qF '"msg":"agent submit"' "$mlog" && ok "controller log: agent submit accepted" \
    || fail "controller log has no accepted 'agent submit'"
  grep -qF '"msg":"gate released"' "$mlog" && ok "controller log: gate released" \
    || fail "controller log has no 'gate released'"
}

assert_gates_and_scheduling() {
  step "assert 3: gate removed and Pods Running on the custom scheduler"
  local p gates sched phase
  for p in $(kubectl -n "$TENANT" get pods -l job-name="$JOB" -o name); do
    gates="$(kubectl -n "$TENANT" get "$p" -o jsonpath='{range .spec.schedulingGates[*]}{.name}{"\n"}{end}')"
    grep -qx "$GATE" <<<"$gates" && fail "$p still gated" || ok "$p gate removed"
    sched="$(kubectl -n "$TENANT" get "$p" -o jsonpath='{.spec.schedulerName}')"
    [[ "$sched" == "traincache-scheduler" ]] && ok "$p schedulerName=$sched" || fail "$p schedulerName=$sched"
    phase="$(kubectl -n "$TENANT" get "$p" -o jsonpath='{.status.phase}')"
    [[ "$phase" == "Running" ]] && ok "$p phase=Running" || fail "$p phase=$phase"
  done
}

assert_node_scores() {
  step "assert 4: per-task Node score annotations"
  local key nodes n ann
  key="$(score_key "$TENANT" "$TASK")"
  info "score annotation key: $key"
  nodes="$(kubectl -n "$TENANT" get warmuptask "$TASK" -o json | jq -r '.status.candidateNodes[]?')"
  if [[ -z "$nodes" ]]; then fail "candidateNodes is empty"; return; fi
  for n in $nodes; do
    ann="$(kubectl get node "$n" -o json | jq -r --arg k "$key" '.metadata.annotations[$k] // empty')"
    if [[ -z "$ann" ]]; then
      fail "node $n missing annotation $key"
      continue
    fi
    if jq -e --arg task "$TASK_REF" '
      .v == 1 and .task == $task and (.g >= 1) and
      (.t | type == "string") and
      (.p["0"] | type == "number") and (.p["0"] >= 0 and .p["0"] <= 100) and
      (.p["1"] | type == "number") and (.p["1"] >= 0 and .p["1"] <= 100)
    ' >/dev/null 2>&1 <<<"$ann"; then
      ok "node $n annotation parses; p=$(jq -c '.p' <<<"$ann") g=$(jq -r '.g' <<<"$ann")"
    else
      fail "node $n annotation invalid: $ann"
    fi
  done
}

assert_scheduler_scored() {
  step "assert 5: scheduler plugin consumed the annotations"
  local log="$LOGS/scheduler.log" key
  key="$(score_key "$TENANT" "$TASK")"
  if [[ ! -s "$log" ]]; then fail "scheduler log is empty"; return; fi
  if grep -q '"msg":"scheduler score"' "$log" && grep -q "$key" "$log"; then
    ok "scheduler emitted 'scheduler score' for annotation_key=$key"
  else
    fail "scheduler log has no TrainCacheScore decision for $key"
  fi
  if grep -Eq '"score":[1-9][0-9]*' "$log"; then
    ok "scheduler observed a non-zero raw score (NormalizeScore path exercised)"
  else
    fail "scheduler never observed a non-zero score"
  fi
  # Both training Pods must appear in the plugin's scoring decisions.
  local p pod_ref
  for p in $(kubectl -n "$TENANT" get pods -l job-name="$JOB" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
    pod_ref="$TENANT/$p"
    grep -q "$pod_ref" "$log" && ok "scheduler scored $pod_ref" || fail "scheduler did not log $pod_ref"
  done
}

assert_node_deviation() {
  step "assert 6: NodeDeviation event (plan criterion 4)"
  local ev=0 actual i
  for i in $(seq 1 60); do
    ev="$(kubectl -n "$TENANT" get events --field-selector reason=NodeDeviation -o name 2>/dev/null | wc -l)"
    (( ev >= 1 )) && break
    sleep 2
  done
  (( ev >= 1 )) && ok "NodeDeviation event observed ($ev)" || fail "no NodeDeviation event was emitted"
  actual="$(kubectl -n "$TENANT" get warmuptask "$TASK" -o json 2>/dev/null \
    | jq '[.status.podNodeReadiness[]? | select(.actualNode != null and .actualNode != "")] | length')"
  (( actual >= 1 )) && ok "PodNodeReadiness.actualNode recorded ($actual row(s))" \
    || fail "PodNodeReadiness.actualNode was not recorded"

  # Refresh artifacts after the event and assert the control-plane log point.
  kubectl -n "$NS" logs deploy/traincache-controller-manager --tail=5000 >"$LOGS/controller-manager.log" 2>&1 || true
  kubectl -n "$TENANT" get events --sort-by=.lastTimestamp >"$RESULTS/events.txt" 2>&1 || true
  grep -qF '"msg":"node deviation detected"' "$LOGS/controller-manager.log" \
    && ok "controller log: node deviation detected" \
    || fail "controller log has no 'node deviation detected'"
  grep -qF '"msg":"preheat: 任务结束"' "$LOGS"/agent-*.log 2>/dev/null \
    && ok "agent log: preheat task finished" \
    || fail "agent logs have no 'preheat: 任务结束'"
}

write_summary() {
  local elapsed=$(( $(date +%s) - RUN_STARTED ))
  local phase gate_state
  phase="$(kubectl -n "$TENANT" get warmuptask "$TASK" -o jsonpath='{.status.phase}' 2>/dev/null || echo unknown)"
  gate_state="$(kubectl -n "$TENANT" get warmuptask "$TASK" -o jsonpath='{.status.gateDecision.state}' 2>/dev/null || echo unknown)"
  {
    printf '# M11 kind e2e summary\n\n'
    printf -- '- cluster: `%s` (kind v0.33.0, node `%s`)\n' "$CLUSTER" "$NODE_IMAGE"
    printf -- '- elapsed: %ss\n' "$elapsed"
    printf -- '- failed assertions: %d\n' "$FAILS"
    printf -- '- WarmupTask phase: `%s`, gate: `%s`\n' "$phase" "$gate_state"
    printf -- '- score key: `%s`\n' "$(score_key "$TENANT" "$TASK")"
    printf -- '- logs: `results/logs/`, state: `results/*.yaml`\n\n'
    printf '## Assertion trail\n\n'
    grep -E '^== |^  \[(OK|FAIL|SKIP|INFO)\]' "$RESULTS/run.log" 2>/dev/null || true
  } >"$RESULTS/summary.md"
  say ""
  say "summary: $RESULTS/summary.md"
}

run_flow() {
  step "workload setup (dataset -> pods gated -> manager enabled)"
  kubectl -n "$TENANT" delete job "$JOB" --ignore-not-found --wait=true >/dev/null 2>&1
  kubectl -n "$TENANT" delete warmuptask "$TASK" --ignore-not-found --wait=true >/dev/null 2>&1
  kubectl -n "$TENANT" delete dataset "$DATASET" --ignore-not-found --wait=true >/dev/null 2>&1

  kubectl apply -f "$E2E/manifests/dataset.yaml" >/dev/null || { fail "apply dataset"; return 1; }
  if ! wait_dataset_ready; then
    fail "DataSet did not become Ready"
    kubectl -n "$TENANT" get dataset "$DATASET" -o yaml | tail -40
    return 1
  fi
  ok "DataSet Ready (snapshot $(kubectl -n "$TENANT" get dataset "$DATASET" -o jsonpath='{.status.snapshotID}'), chunks $(kubectl -n "$TENANT" get dataset "$DATASET" -o jsonpath='{.status.totalChunks}'))"

  # Freeze the controller so the webhook injects the gate into the Job Pods
  # before any preheat can release it. The webhook is an independent process.
  scale_manager 0 || { fail "scale controller-manager to 0"; return 1; }
  ok "controller-manager paused (webhook still serving)"

  kubectl apply -f "$E2E/manifests/warmuptask.yaml" >/dev/null || { fail "apply warmuptask"; return 1; }
  # Let the webhook's informer observe the new WarmupTask before Pod creation,
  # otherwise admission runs against a stale cache and skips injection.
  sleep 5
  kubectl apply -f "$E2E/manifests/train-job.yaml" >/dev/null || { fail "apply job"; return 1; }
  if ! wait_workload_pods; then
    info "injection not observed; recreating the Job once after cache warm-up"
    kubectl -n "$TENANT" delete job "$JOB" --ignore-not-found --wait=true >/dev/null 2>&1
    sleep 5
    kubectl apply -f "$E2E/manifests/train-job.yaml" >/dev/null 2>&1
  fi
  if ! wait_workload_pods; then
    fail "training Pods were not created and injected"
    kubectl -n "$TENANT" get pods -l job-name="$JOB" -o wide
    return 1
  fi
  ok "2 training Pods created, gated by webhook"
  assert_webhook_injection

  scale_manager 1 || { fail "scale controller-manager to 1"; return 1; }
  ok "controller-manager resumed"

  if ! wait_task_phase Released 300; then
    fail "WarmupTask did not reach Released"
    kubectl -n "$TENANT" get warmuptask "$TASK" -o yaml | tail -60
    return 1
  fi
  ok "WarmupTask Released"

  if ! wait_gates_removed 120; then fail "gates were not removed from all Pods"; else ok "gates removed"; fi
  if ! wait_pods_running 180; then
    fail "Pods did not reach Running"
    kubectl -n "$TENANT" get pods -l job-name="$JOB" -o wide
  else
    ok "training Pods Running"
  fi
}

assert_all() {
  assert_preheat_and_release
  assert_gates_and_scheduling
  assert_node_scores
  collect_artifacts
  assert_scheduler_scored
  assert_node_deviation
  write_summary
}

main() {
  start_logging
  say "TrainCache M11 e2e (mode=$MODE cluster=$CLUSTER)"
  case "$MODE" in
    clean) clean ;;
    setup) setup ;;
    run) setup && run_flow && assert_all ;;
    assert) assert_all ;;
    *) say "usage: $0 [clean|setup|run|assert]"; exit 2 ;;
  esac
  if [[ "$FAILS" -gt 0 ]]; then
    say ""
    say "RESULT: FAIL ($FAILS assertion/step failure(s)); see $RESULTS/run.log and $LOGS/"
    exit 1
  fi
  say ""
  say "RESULT: PASS"
  exit 0
}

main

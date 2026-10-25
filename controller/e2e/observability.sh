#!/usr/bin/env bash
# observability.sh — repeatable acceptance checks for the TrainCache logging
# and metrics chain (docs/9.2 P1) on the existing kind cluster.
#
# Usage:
#   ./e2e/observability.sh [up|verify|down]   (default: verify)
#
#   up      apply controller/deploy/observability, restart the workloads so
#           ConfigMap changes take effect, and wait for all four to be Ready
#   verify  assert workloads, JSON logging, Loki labels/queries, log survival
#           after Pod deletion, Prometheus targets, Grafana data sources and
#           the kind.yaml log-rotation budget (creates/deletes a probe Pod)
#   down    delete the observability resources (including the namespace)
#
# The script never creates or deletes the kind cluster. Images are loaded from
# the local Docker daemon when present (kind nodes cannot always reach the
# registry mirror); set SKIP_IMAGE_LOAD=1 to rely on kubelet pulls instead.
#
# Environment overrides:
#   CLUSTER=traincache-e2e  OBS_NS=observability  PROBE_NS=default
#   READY_TIMEOUT=300  DELETE_WAIT=20  SKIP_IMAGE_LOAD=1
#   LOKI_LOCAL_PORT=13100  PROM_LOCAL_PORT=19090  GRAFANA_LOCAL_PORT=13000
#   GRAFANA_PASSWORD=admin
#
# Exit codes: 0 all assertions passed; 1 an assertion/step failed;
#             2 usage or missing tool.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
KUST="$REPO_ROOT/controller/deploy/observability"
KIND_YAML="$REPO_ROOT/controller/e2e/kind.yaml"

CLUSTER="${CLUSTER:-traincache-e2e}"
CTX="kind-$CLUSTER"
OBS_NS="${OBS_NS:-observability}"
PROBE_NS="${PROBE_NS:-default}"
PROBE_NAME="${PROBE_NAME:-observability-probe}"
PROBE_COMPONENT="${PROBE_COMPONENT:-observability-probe}"

REGISTRY="${REGISTRY:-dgl3i4ucowsgvvdwty.xuanyuan.run}"
IMG_LOKI="${IMG_LOKI:-$REGISTRY/grafana/loki:3.6.9}"
IMG_GRAFANA="${IMG_GRAFANA:-$REGISTRY/grafana/grafana:12.4.13}"
IMG_PROMETHEUS="${IMG_PROMETHEUS:-$REGISTRY/prom/prometheus:v3.5.5}"
IMG_FLUENTBIT="${IMG_FLUENTBIT:-$REGISTRY/fluent/fluent-bit:3.1.8}"
IMG_BUSYBOX="${IMG_BUSYBOX:-$REGISTRY/library/busybox:1.36.1}"

READY_TIMEOUT="${READY_TIMEOUT:-300}"
DELETE_WAIT="${DELETE_WAIT:-20}"
LOKI_LOCAL_PORT="${LOKI_LOCAL_PORT:-13100}"
PROM_LOCAL_PORT="${PROM_LOCAL_PORT:-19090}"
GRAFANA_LOCAL_PORT="${GRAFANA_LOCAL_PORT:-13000}"
GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-admin}"

MODE="${1:-verify}"
RUN_ID="$(date +%s)-$$"
REQ_ID="req-$RUN_ID"
POD_ID="pod-$RUN_ID"
TASK_ID="task-$RUN_ID"
PATH_ID="/probe/$RUN_ID/payload"
PROBE_JSON=""
PROBE_NODE=""
PROBE_CREATED=0
START_NS=0
PF_PIDS=()

say()  { printf '%s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
ok()   { printf '  [OK]   %s\n' "$*"; }
info() { printf '  [INFO] %s\n' "$*"; }
warn() { printf '  [WARN] %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; exit 1; }

cleanup() {
  set +eu
  if [[ "$PROBE_CREATED" == "1" ]]; then
    kubectl -n "$PROBE_NS" delete pod "$PROBE_NAME" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  local pid
  for pid in ${PF_PIDS[@]+"${PF_PIDS[@]}"}; do
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

require_tools() {
  local missing=0 t
  for t in kubectl jq curl kind docker; do
    if ! command -v "$t" >/dev/null 2>&1; then
      say "FATAL: missing required tool: $t"
      missing=1
    fi
  done
  [[ "$missing" -eq 0 ]] || exit 2
}

use_context() {
  if ! kubectl config get-contexts -o name | grep -qx "$CTX"; then
    fail "kubectl context $CTX not found; create the kind cluster with controller/e2e/run.sh setup"
  fi
  kubectl config use-context "$CTX" >/dev/null
  kubectl cluster-info >/dev/null 2>&1 || fail "cluster $CLUSTER is unreachable"
  ok "context $CTX reachable"
}

load_images() {
  if [[ "${SKIP_IMAGE_LOAD:-0}" == "1" ]]; then
    info "SKIP_IMAGE_LOAD=1; relying on kubelet image pulls"
    return 0
  fi
  local img
  for img in "$IMG_LOKI" "$IMG_GRAFANA" "$IMG_PROMETHEUS" "$IMG_FLUENTBIT" "$IMG_BUSYBOX"; do
    if ! docker image inspect "$img" >/dev/null 2>&1; then
      warn "image $img is not in the local Docker daemon; kubelet will try the registry mirror"
      continue
    fi
    if kind load docker-image --name "$CLUSTER" "$img" >/dev/null 2>&1; then
      ok "loaded $img into kind nodes"
    else
      warn "kind load failed for $img; kubelet will try the registry mirror"
    fi
  done
}

start_pf() {
  local svc="$1" local_port="$2" remote_port="$3" health_url="$4"
  local log pid i
  log="$(mktemp "${TMPDIR:-/tmp}/traincache-obs-pf.XXXXXX")"
  kubectl -n "$OBS_NS" port-forward "svc/$svc" "$local_port:$remote_port" >"$log" 2>&1 &
  pid=$!
  PF_PIDS+=("$pid")
  for i in $(seq 1 60); do
    if curl -fsS --max-time 2 -o /dev/null "$health_url" 2>/dev/null; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      fail "port-forward svc/$svc exited: $(tail -1 "$log" 2>/dev/null)"
    fi
    sleep 0.5
  done
  fail "port-forward svc/$svc did not become healthy at $health_url"
}

# ---------------------------------------------------------------- assertions

check_workloads() {
  step "assert 1: four observability workloads Ready"
  local j ready desired

  if ! j="$(kubectl -n "$OBS_NS" get deploy grafana -o json 2>/dev/null)"; then
    fail "deployment/grafana not found in namespace $OBS_NS"
  fi
  ready="$(jq -r '.status.readyReplicas // 0' <<<"$j")"
  desired="$(jq -r '.spec.replicas // 1' <<<"$j")"
  [[ "$ready" -ge "$desired" ]] || fail "deployment/grafana ready=$ready want=$desired"
  ok "deployment/grafana ready=$ready"

  if ! j="$(kubectl -n "$OBS_NS" get deploy prometheus -o json 2>/dev/null)"; then
    fail "deployment/prometheus not found in namespace $OBS_NS (is prometheus in the root kustomization?)"
  fi
  ready="$(jq -r '.status.readyReplicas // 0' <<<"$j")"
  desired="$(jq -r '.spec.replicas // 1' <<<"$j")"
  [[ "$ready" -ge "$desired" ]] || fail "deployment/prometheus ready=$ready want=$desired"
  ok "deployment/prometheus ready=$ready"

  if ! j="$(kubectl -n "$OBS_NS" get statefulset loki -o json 2>/dev/null)"; then
    fail "statefulset/loki not found in namespace $OBS_NS"
  fi
  ready="$(jq -r '.status.readyReplicas // 0' <<<"$j")"
  desired="$(jq -r '.spec.replicas // 1' <<<"$j")"
  [[ "$ready" -ge "$desired" ]] || fail "statefulset/loki ready=$ready want=$desired"
  ok "statefulset/loki ready=$ready"

  if ! j="$(kubectl -n "$OBS_NS" get daemonset fluent-bit -o json 2>/dev/null)"; then
    fail "daemonset/fluent-bit not found in namespace $OBS_NS"
  fi
  desired="$(jq -r '.status.desiredNumberScheduled // 0' <<<"$j")"
  ready="$(jq -r '.status.numberReady // 0' <<<"$j")"
  [[ "$desired" -gt 0 ]] || fail "daemonset/fluent-bit scheduled on 0 nodes"
  [[ "$ready" -ge "$desired" ]] || fail "daemonset/fluent-bit ready=$ready want=$desired"
  ok "daemonset/fluent-bit ready=$ready/$desired"
}

create_probe() {
  step "probe Pod: single-line JSON log with sentinel fields"
  PROBE_JSON="$(jq -cn \
    --arg v "v9" --arg c "$PROBE_COMPONENT" --arg l "INFO" \
    --arg req "$REQ_ID" --arg pod "$POD_ID" --arg task "$TASK_ID" \
    --arg path "$PATH_ID" \
    '{log_schema_version:$v,component:$c,level:$l,request_id:$req,pod:$pod,task_id:$task,path:$path,msg:"observability probe"}')"
  jq -e . >/dev/null <<<"$PROBE_JSON" || fail "generated probe line is not valid JSON"

  kubectl -n "$PROBE_NS" delete pod "$PROBE_NAME" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  START_NS="$(( $(date +%s) * 1000000000 ))"
  if ! kubectl -n "$PROBE_NS" run "$PROBE_NAME" \
      --image="$IMG_BUSYBOX" --image-pull-policy=IfNotPresent --restart=Never \
      --env "PROBE_LINE=$PROBE_JSON" \
      --command -- /bin/sh -c 'printf "%s\n" "$PROBE_LINE"; sleep 600' >/dev/null; then
    fail "could not create probe Pod $PROBE_NS/$PROBE_NAME"
  fi
  PROBE_CREATED=1
  if ! kubectl -n "$PROBE_NS" wait --for=condition=Ready "pod/$PROBE_NAME" --timeout=90s >/dev/null; then
    fail "probe Pod did not become Ready"
  fi

  local line="" i
  for i in $(seq 1 60); do
    line="$(kubectl -n "$PROBE_NS" logs "$PROBE_NAME" --tail=1 2>/dev/null || true)"
    [[ -n "$line" ]] && break
    sleep 1
  done
  [[ -n "$line" ]] || fail "probe Pod produced no log line"
  PROBE_NODE="$(kubectl -n "$PROBE_NS" get pod "$PROBE_NAME" -o jsonpath='{.spec.nodeName}')"
  [[ -n "$PROBE_NODE" ]] || fail "probe Pod has no nodeName"
  ok "probe running on $PROBE_NODE"

  local nonempty
  nonempty="$(printf '%s\n' "$line" | grep -c . || true)"
  [[ "$nonempty" -eq 1 ]] || fail "kubectl logs returned $nonempty non-empty lines, want exactly 1"
  if ! jq -e . >/dev/null <<<"$line"; then
    fail "probe log line is not valid JSON: $line"
  fi
  ok "kubectl logs is one valid JSON line"

  if ! jq -e \
      --arg c "$PROBE_COMPONENT" --arg req "$REQ_ID" --arg pod "$POD_ID" \
      --arg task "$TASK_ID" --arg path "$PATH_ID" \
      '.component == $c and .level == "INFO" and
       (.log_schema_version | type == "string" and length > 0) and
       .request_id == $req and .pod == $pod and .task_id == $task and .path == $path' \
      >/dev/null <<<"$line"; then
    fail "probe JSON lacks component/level/log_schema_version or a sentinel field"
  fi
  ok "JSON carries component/level/log_schema_version and four sentinels"
}

loki_query_range() {
  curl -fsS -G "http://127.0.0.1:$LOKI_LOCAL_PORT/loki/api/v1/query_range" \
    --data-urlencode "query=$1" \
    --data-urlencode "start=$START_NS" \
    --data-urlencode "limit=50" \
    --data-urlencode "direction=backward"
}

# loki_wait_hit QUERY — poll until the query returns at least one line, then
# print the response body.
loki_wait_hit() {
  local query="$1" resp="" n=0 i
  for i in $(seq 1 60); do
    resp="$(loki_query_range "$query" 2>/dev/null || true)"
    n="$(jq -r '[.data.result[]?.values[]?] | length' <<<"$resp" 2>/dev/null || echo 0)"
    [[ "$n" -gt 0 ]] && { printf '%s' "$resp"; return 0; }
    sleep 2
  done
  return 1
}

check_loki() {
  step "assert 2: Loki query by namespace/component/node and bounded labels"
  start_pf loki "$LOKI_LOCAL_PORT" 3100 "http://127.0.0.1:$LOKI_LOCAL_PORT/ready"
  ok "Loki reachable via port-forward"

  local selector query resp
  selector="{namespace=\"$PROBE_NS\",component=\"$PROBE_COMPONENT\",node=\"$PROBE_NODE\"}"
  query="$selector |= \"$REQ_ID\""
  if ! resp="$(loki_wait_hit "$query")"; then
    fail "Loki query returned no lines for $query within 120s"
  fi
  ok "Loki query hit for namespace=$PROBE_NS component=$PROBE_COMPONENT node=$PROBE_NODE"

  # The probe stream must carry exactly the six frozen labels, all non-empty,
  # and none of the high-cardinality fields. Only the stream label set is
  # checked: Loki 3.6's detected_level is computed structured metadata and is
  # not a stored label, so it may appear elsewhere in the response.
  if ! jq -e '
      [.data.result[].stream] as $s |
      ($s | length) > 0 and
      all($s[]; ((del(.detected_level) | keys | sort) == ["cluster","component","level","namespace","node","stream"])) and
      all($s[]; (del(.detected_level) | all(.[]; (. != null and . != "")))) and
      all($s[]; ((has("request_id") or has("pod") or has("task_id") or has("path") or has("service_name")) | not))
    ' >/dev/null <<<"$resp"; then
    local got
    got="$(jq -c '[.data.result[].stream] | unique' <<<"$resp" 2>/dev/null || true)"
    fail "probe stream labels are not exactly the six bounded labels: $got"
  fi
  ok "probe stream labels are exactly cluster/namespace/component/level/node/stream, all non-empty"
  info "stream: $(jq -c '.data.result[0].stream' <<<"$resp")"

  local body
  body="$(jq -r '[.data.result[].values[][1]] | join("\n")' <<<"$resp")"
  local sentinel
  for sentinel in "$REQ_ID" "$POD_ID" "$TASK_ID" "$PATH_ID"; do
    grep -qF "$sentinel" <<<"$body" || fail "Loki body does not contain sentinel $sentinel"
  done
  ok "request_id/pod/task_id/path remain in the JSON body, not in labels"

  # Global label index must stay inside the six-label budget.
  local global
  if ! global="$(curl -fsS -G "http://127.0.0.1:$LOKI_LOCAL_PORT/loki/api/v1/labels" \
      --data-urlencode "start=$START_NS" 2>/dev/null)"; then
    fail "Loki /labels request failed"
  fi
  local glabels
  glabels="$(jq -r '.data[]?' <<<"$global" | sort)"
  grep -qx 'service_name' <<<"$glabels" \
    && fail "Loki stored label service_name although discover_service_name is disabled"
  [[ "$glabels" == "$(printf '%s\n' cluster component level namespace node stream | sort)" ]] \
    || fail "Loki label index is outside the six-label budget: $(tr '\n' ',' <<<"$glabels")"
  ok "Loki label index is exactly the six bounded labels (no service_name)"
}

check_loki_after_delete() {
  step "assert 3: log survives probe Pod deletion (already in the central store)"
  local selector query resp
  selector="{namespace=\"$PROBE_NS\",component=\"$PROBE_COMPONENT\",node=\"$PROBE_NODE\"}"
  query="$selector |= \"$REQ_ID\""

  if ! kubectl -n "$PROBE_NS" delete pod "$PROBE_NAME" --wait=true >/dev/null 2>&1; then
    fail "could not delete probe Pod $PROBE_NS/$PROBE_NAME"
  fi
  PROBE_CREATED=0
  ok "probe Pod deleted"
  info "waiting ${DELETE_WAIT}s before re-querying the central store"
  sleep "$DELETE_WAIT"

  if ! resp="$(loki_wait_hit "$query")"; then
    fail "Loki no longer returns the probe line after Pod deletion: $query"
  fi
  ok "Loki still returns the probe line after Pod deletion"
}

check_prometheus() {
  step "assert 4: Prometheus ready and project targets up"
  start_pf prometheus "$PROM_LOCAL_PORT" 9090 "http://127.0.0.1:$PROM_LOCAL_PORT/-/ready"
  local ready
  ready="$(curl -fsS --max-time 10 "http://127.0.0.1:$PROM_LOCAL_PORT/-/ready" 2>/dev/null || true)"
  grep -qi 'ready' <<<"$ready" || fail "Prometheus /-/ready did not report ready: $ready"
  ok "Prometheus /-/ready: $ready"

  local resp="" total=0 down=0 i
  for i in $(seq 1 40); do
    resp="$(curl -fsS --max-time 10 "http://127.0.0.1:$PROM_LOCAL_PORT/api/v1/targets" 2>/dev/null || true)"
    total="$(jq -r '[.data.activeTargets[]? | select(.labels.job == "traincache-components")] | length' <<<"$resp" 2>/dev/null || echo 0)"
    down="$(jq -r '[.data.activeTargets[]? | select(.labels.job == "traincache-components") | select(.health != "up")] | length' <<<"$resp" 2>/dev/null || echo 0)"
    [[ "$total" -gt 0 && "$down" -eq 0 ]] && break
    sleep 3
  done

  if [[ "$total" -gt 0 ]]; then
    [[ "$down" -eq 0 ]] || fail "Prometheus has $down/$total down targets for job traincache-components"
    if ! jq -e '
        [.data.activeTargets[] | select(.labels.job == "traincache-components")] as $t |
        all($t[]; (.labels.component | length > 0) and
                   (.labels.namespace | length > 0) and
                   (.labels.node | length > 0) and
                   (.labels.instance | length > 0))
      ' >/dev/null <<<"$resp"; then
      fail "a project target is missing relabelled component/namespace/node/instance"
    fi
    ok "all $total project targets up with component/namespace/node/instance labels"
    info "targets: $(jq -r '[.data.activeTargets[] | select(.labels.job == "traincache-components") | "\(.labels.component)@\(.labels.node)"] | sort | join(", ")' <<<"$resp")"
    return 0
  fi

  # No business Pods: prove the job exists and pod SD + relabel rules fired.
  info "no active business targets; checking the job config and dropped discoveries"
  local cfg
  cfg="$(curl -fsS --max-time 10 "http://127.0.0.1:$PROM_LOCAL_PORT/api/v1/status/config" 2>/dev/null || true)"
  jq -r '.data.yaml // ""' <<<"$cfg" | grep -q 'job_name: traincache-components' \
    || fail "Prometheus config has no job traincache-components"
  local dropped
  dropped="$(jq -r '.data.droppedTargets | length' <<<"$resp" 2>/dev/null || echo 0)"
  [[ "$dropped" -gt 0 ]] \
    || fail "job traincache-components exists but pod discovery produced no targets or dropped targets"
  ok "job traincache-components configured; pod SD discovered $dropped dropped target(s)"
  info "explanation: no TrainCache business Pods are running, so active targets are empty; the relabel keep rules were exercised against discovered Pods"
}

check_grafana() {
  step "assert 5: Grafana has both Loki and Prometheus data sources"
  start_pf grafana "$GRAFANA_LOCAL_PORT" 3000 "http://127.0.0.1:$GRAFANA_LOCAL_PORT/api/health"
  local resp types
  resp="$(curl -fsS --max-time 10 -u "admin:$GRAFANA_PASSWORD" \
    "http://127.0.0.1:$GRAFANA_LOCAL_PORT/api/datasources" 2>/dev/null || true)"
  [[ -n "$resp" ]] || fail "Grafana /api/datasources returned nothing (check GRAFANA_PASSWORD)"
  types="$(jq -r '.[].type' <<<"$resp" 2>/dev/null || true)"
  grep -qx 'loki' <<<"$types" || fail "Grafana has no Loki data source"
  grep -qx 'prometheus' <<<"$types" || fail "Grafana has no Prometheus data source"
  ok "Grafana data sources: $(jq -r '[.[] | "\(.name)(\(.type))"] | join(", ")' <<<"$resp")"
}

check_kind_rotation() {
  step "assert 6: kind.yaml log rotation budget (50Mi / 3)"
  local size_count files_count
  size_count="$(grep -c 'containerLogMaxSize: 50Mi' "$KIND_YAML" || true)"
  files_count="$(grep -c 'containerLogMaxFiles: 3' "$KIND_YAML" || true)"
  [[ "$size_count" -ge 1 ]] || fail "kind.yaml has no 'containerLogMaxSize: 50Mi'"
  [[ "$files_count" -ge 1 ]] || fail "kind.yaml has no 'containerLogMaxFiles: 3'"
  [[ "$size_count" -eq "$files_count" ]] \
    || fail "kind.yaml rotation settings differ per node (size=$size_count files=$files_count)"
  ok "kind.yaml: $size_count node(s) with containerLogMaxSize=50Mi and containerLogMaxFiles=3"
}

# --------------------------------------------------------------------- modes

up() {
  require_tools
  use_context
  step "apply observability stack from $KUST"
  load_images
  kubectl apply -k "$KUST" >/dev/null || fail "kubectl apply -k $KUST"
  ok "manifests applied"
  # ConfigMap content does not roll Pods automatically; restart so Grafana
  # re-provisions data sources and fluent-bit re-reads its pipeline.
  kubectl -n "$OBS_NS" rollout restart deploy/grafana deploy/prometheus >/dev/null
  kubectl -n "$OBS_NS" rollout restart statefulset/loki >/dev/null
  kubectl -n "$OBS_NS" rollout restart daemonset/fluent-bit >/dev/null
  if ! kubectl -n "$OBS_NS" rollout status deploy/grafana --timeout="${READY_TIMEOUT}s" >/dev/null; then
    fail "deployment/grafana did not become Ready within ${READY_TIMEOUT}s"
  fi
  if ! kubectl -n "$OBS_NS" rollout status deploy/prometheus --timeout="${READY_TIMEOUT}s" >/dev/null; then
    fail "deployment/prometheus did not become Ready within ${READY_TIMEOUT}s (image $IMG_PROMETHEUS available?)"
  fi
  if ! kubectl -n "$OBS_NS" rollout status statefulset/loki --timeout="${READY_TIMEOUT}s" >/dev/null; then
    fail "statefulset/loki did not become Ready within ${READY_TIMEOUT}s (image $IMG_LOKI available?)"
  fi
  if ! kubectl -n "$OBS_NS" rollout status daemonset/fluent-bit --timeout="${READY_TIMEOUT}s" >/dev/null; then
    fail "daemonset/fluent-bit did not become Ready within ${READY_TIMEOUT}s"
  fi
  check_workloads
}

verify() {
  require_tools
  use_context
  check_workloads
  create_probe
  check_loki
  check_loki_after_delete
  check_prometheus
  check_grafana
  check_kind_rotation
}

down() {
  require_tools
  use_context
  step "delete observability resources"
  if ! kubectl delete -k "$KUST" --ignore-not-found --wait=true --timeout=180s >/dev/null 2>&1; then
    fail "kubectl delete -k $KUST failed"
  fi
  if kubectl get namespace "$OBS_NS" >/dev/null 2>&1; then
    if ! kubectl wait --for=delete "namespace/$OBS_NS" --timeout=180s >/dev/null 2>&1; then
      fail "namespace $OBS_NS is still present after deletion"
    fi
  fi
  ok "namespace $OBS_NS and its resources removed"
}

case "$MODE" in
  up|verify|down) ;;
  *)
    say "usage: $0 [up|verify|down]   (default: verify)"
    exit 2
    ;;
esac

say "TrainCache observability acceptance (mode=$MODE cluster=$CLUSTER)"
case "$MODE" in
  up) up ;;
  verify) verify ;;
  down) down ;;
esac

say ""
say "RESULT: PASS"
exit 0

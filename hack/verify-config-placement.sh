#!/usr/bin/env bash
# 配置落位门禁:防止五份工作负载清单里再混进字面量 env,也防止 ConfigMap 与清单
# 引用脱钩。判据正文见 docs/PARAMETER-CONFIGURATION.md 的「落位判据」一节。
#
#   1. 凭证 -> Secret;2. 节点身份 -> fieldRef;3. 换集群/换轮次要改 -> ConfigMap;
#   4. 偏离代码默认 -> ConfigMap;5. 等于代码默认 -> 哪里都不写。
#
# 引用方向还有一条 Kubernetes 硬约束:configMapKeyRef 只在 Pod 自己的 namespace
# 里找对象。scheduler 跑在 kube-system,引用 traincache 的 traincache-config 会让
# Pod 停在 CreateContainerConfigError;清单哈希没变过就不执行 apply,这个缺陷在一直
# 复用的热集群上不会露头。第 2 步因此按 namespace 校验每一条引用。
#
# 不需要 Go 工具链,也不需要集群:直接读源文件。
set -uo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIGMAPS=(
  controller/deploy/agent/configmap.yaml
  controller/deploy/scheduler/kube-scheduler-config.yaml
  controller/deploy/webhook/checkpoint-image.yaml
)
WORKLOADS=(
  controller/deploy/agent/daemonset.yaml
  controller/deploy/manager/deployment.yaml
  controller/deploy/webhook/deployment.yaml
  controller/deploy/scheduler/deployment.yaml
  controller/deploy/gateway/deployment.yaml
)

fail=0

# 1. 工作负载清单里不得出现任何字面量 env 值。
for f in "${WORKLOADS[@]}"; do
  hits="$(grep -n '^[[:space:]]*value:' "$f" || true)"
  if [ -n "$hits" ]; then
    echo "FAIL $f: 出现字面量 env 值,配置落位判据要求改走 ConfigMap / Secret / fieldRef 或删除(等于代码默认):"
    echo "$hits"
    fail=1
  fi
done

# 2 & 3 & 4. 每条引用的 namespace 与键都要对得上;ConfigMap 里不许留没人读的键。
python3 - "$(IFS=,; echo "${CONFIGMAPS[*]}")" "${WORKLOADS[@]}" <<'PY' || fail=1
import re
import sys

cm_paths = [p for p in sys.argv[1].split(",") if p]
workloads = sys.argv[2:]


def docs(text):
    return re.split(r"^---\s*$", text, flags=re.M)


def metadata_field(doc, field):
    """metadata 下的单层字段(name/namespace):缩进两格,不会撞上 spec 里的同名字段。"""
    m = re.search(rf"^  {field}:\s*(\S+)\s*$", doc, re.M)
    return m.group(1) if m else None


def data_keys(doc):
    """data: 段的顶层键;内嵌 YAML(scheduler.yaml)的 4 空格缩进行不算配置项。"""
    m = re.search(r"^data:\s*$", doc, re.M)
    if not m:
        return set()
    return set(re.findall(r"^  ([A-Za-z0-9_]+):", doc[m.end():], re.M))


def refs(doc):
    """按出现顺序取 (kind, name, key);kind 是 configMapKeyRef 或 secretKeyRef。"""
    out, pending = [], None
    for line in doc.splitlines():
        stripped = line.strip()
        if stripped in ("configMapKeyRef:", "secretKeyRef:"):
            pending = {"kind": stripped[:-1], "name": None, "key": None}
            continue
        if pending is None:
            continue
        m = re.match(r"(name|key):\s*(\S+)", stripped)
        if m:
            pending[m.group(1)] = m.group(2)
            if pending["name"] and pending["key"]:
                out.append((pending["kind"], pending["name"], pending["key"]))
                pending = None
            continue
        if stripped and not stripped.startswith("#"):
            pending = None
    return out


cms = {}
for path in cm_paths:
    for d in docs(open(path, encoding="utf-8").read()):
        if not re.search(r"^kind:\s*ConfigMap\s*$", d, re.M):
            continue
        name = metadata_field(d, "name")
        ns = metadata_field(d, "namespace")
        if name is None or ns is None:
            print(f"FAIL {path}: ConfigMap 缺 metadata.name 或 metadata.namespace,门禁判不了引用落点")
            sys.exit(1)
        cms[(name, ns)] = (path, data_keys(d))

cm_refs = {}
secret_refs = set()
problems = []

for path in workloads:
    for d in docs(open(path, encoding="utf-8").read()):
        if not re.search(r"^kind:\s*(Deployment|DaemonSet)\s*$", d, re.M):
            continue
        w_name = metadata_field(d, "name")
        w_ns = metadata_field(d, "namespace")
        if w_ns is None:
            problems.append(f"{path}: 工作负载 {w_name} 没写 namespace")
            continue
        for kind, cm_name, key in refs(d):
            if kind == "secretKeyRef":
                secret_refs.add(key)
                continue
            entry = cms.get((cm_name, w_ns))
            if entry is None:
                home = [ns for (n, ns) in cms if n == cm_name]
                hint = f",它只存在于 {home[0]}" if home else ",仓库里没有这个 ConfigMap"
                problems.append(
                    f"{path}: {w_ns}/{w_name} 引用 {cm_name}/{key}{hint};"
                    " configMapKeyRef 只在本 namespace 找对象,Pod 会停在 CreateContainerConfigError")
                continue
            if key not in entry[1]:
                problems.append(
                    f"{path}: {w_ns}/{w_name} 引用 {cm_name}/{key},该 ConfigMap 里没有这一项")
                continue
            cm_refs.setdefault((cm_name, w_ns), set()).add(key)

for (cm_name, ns), (path, keys) in sorted(cms.items()):
    orphan = sorted(keys - cm_refs.get((cm_name, ns), set()))
    if orphan:
        problems.append(f"{path}: {ns}/{cm_name} 里没有任何清单引用的配置项(进程读不到): {orphan}")

if problems:
    for p in problems:
        print(f"FAIL {p}")
if secret_refs != {"accessKey", "secretKey"}:
    print(f"FAIL Secret 引用异常(期望 accessKey、secretKey): {sorted(secret_refs)}")
sys.exit(1 if (problems or secret_refs != {"accessKey", "secretKey"}) else 0)
PY

if [ "$fail" -ne 0 ]; then
  echo "配置落位检查未通过:按 docs/PARAMETER-CONFIGURATION.md 的「落位判据」五问重新落位。"
  exit 1
fi
for f in "${CONFIGMAPS[@]}"; do
  n="$(awk 'f{if(/^  [A-Za-z0-9_]+:/)c++} /^data:$/{f=1} END{print c+0}' "$f")"
  printf 'OK 配置落位: %s 共 %s 项,与同 namespace 的清单引用一一对应;工作负载零字面量 env;Secret 引用 accessKey/secretKey\n' "$f" "$n"
done

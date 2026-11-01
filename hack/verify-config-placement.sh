#!/usr/bin/env bash
# 配置落位门禁:防止四份工作负载清单里再混进字面量 env,也防止 ConfigMap 与清单
# 引用脱钩。判据正文见 docs/PARAMETER-CONFIGURATION.md 的「落位判据」一节。
#
#   1. 凭证 -> Secret;2. 节点身份 -> fieldRef;3. 换集群/换轮次要改 -> ConfigMap;
#   4. 偏离代码默认 -> ConfigMap;5. 等于代码默认 -> 哪里都不写。
#
# 不需要 Go 工具链,也不需要集群:kubectl 只在能渲染时用,这里直接读源文件。
set -uo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CM=controller/deploy/agent/configmap.yaml
WORKLOADS=(
  controller/deploy/agent/daemonset.yaml
  controller/deploy/manager/deployment.yaml
  controller/deploy/webhook/deployment.yaml
  controller/deploy/scheduler/deployment.yaml
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

# 2 & 3 & 4. ConfigMap 配置项与清单引用一一对应;Secret 引用固定两个键。
python3 - "$CM" "${WORKLOADS[@]}" <<'PY' || fail=1
import re, sys

cm_path, workloads = sys.argv[1], sys.argv[2:]

body = open(cm_path, encoding="utf-8").read().split("\ndata:\n", 1)
if len(body) != 2:
    print(f"FAIL {cm_path}: 找不到 data: 段")
    sys.exit(1)
cm_keys = set(re.findall(r"^  ([A-Za-z0-9_]+):", body[1], re.M))

cm_refs, secret_refs = set(), set()
for path in workloads:
    pending = None
    for line in open(path, encoding="utf-8"):
        stripped = line.strip()
        if stripped in ("configMapKeyRef:", "secretKeyRef:"):
            pending = {"kind": stripped[:-1], "name": None, "key": None}
            continue
        if pending is not None:
            m = re.match(r"(name|key):\s*(\S+)", stripped)
            if m:
                pending[m.group(1)] = m.group(2)
                if pending["name"] and pending["key"]:
                    if pending["kind"] == "configMapKeyRef":
                        if pending["name"] != "traincache-config":
                            print(f"FAIL {path}: 引用了 {pending['name']},本门禁只覆盖 traincache-config")
                            sys.exit(1)
                        cm_refs.add(pending["key"])
                    else:
                        secret_refs.add(pending["key"])
                    pending = None
                continue
            if stripped and not stripped.startswith("#"):
                pending = None

missing = sorted(cm_refs - cm_keys)
orphan = sorted(cm_keys - cm_refs)
if missing:
    print(f"FAIL 清单引用了但 {cm_path} 里没有的配置项(Pod 会停在 CreateContainerConfigError): {missing}")
if orphan:
    print(f"FAIL {cm_path} 里有但没有任何清单引用的配置项(进程读不到): {orphan}")
if secret_refs != {"accessKey", "secretKey"}:
    print(f"FAIL Secret 引用异常(期望 accessKey、secretKey): {sorted(secret_refs)}")
sys.exit(1 if (missing or orphan or secret_refs != {"accessKey", "secretKey"}) else 0)
PY

if [ "$fail" -ne 0 ]; then
  echo "配置落位检查未通过:按 docs/PARAMETER-CONFIGURATION.md 的「落位判据」五问重新落位。"
  exit 1
fi
cm_count="$(awk 'f{if(/^  [A-Za-z0-9_]+:/)c++} /^data:$/{f=1} END{print c+0}' "$CM")"
echo "OK 配置落位:清单零字面量 env、traincache-config 的 ${cm_count} 项与清单引用一一对应、Secret 引用 accessKey/secretKey"

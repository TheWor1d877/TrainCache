#!/usr/bin/env bash
# verify-chart.sh — Helm chart 与 controller/deploy 的 kustomize 清单同源。
#
# 默认 values 下 chart 渲染出的每个对象要与 kubectl kustomize 的输出逐字段
# 相等。chart 不含 Namespace 对象，安装走 --create-namespace，所以比较时
# 跳过它。chart 也不注入 Helm 自有标签，否则两边无法对齐。
set -uo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" || exit 2

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
bad=0

helm lint chart/traincache >/dev/null || bad=1

helm template traincache chart/traincache -n traincache --include-crds >"$tmp/helm.yaml" || exit 1
kubectl kustomize controller/deploy >"$tmp/kustomize.yaml" || exit 1

python3 - "$tmp/kustomize.yaml" "$tmp/helm.yaml" <<'PY' || bad=1
import sys
import yaml


def load(path):
    out = {}
    for doc in yaml.safe_load_all(open(path, encoding="utf-8")):
        if not doc:
            continue
        meta = doc.get("metadata", {})
        out[(doc["kind"], meta.get("namespace", ""), meta.get("name"))] = doc
    return out


kustomize = load(sys.argv[1])
helm = load(sys.argv[2])
kustomize.pop(("Namespace", "", "traincache"), None)

problems = []


def walk(a, b, path):
    if type(a) is not type(b):
        problems.append("%s: 类型不同" % path)
        return
    if isinstance(a, dict):
        for key in sorted(set(a) | set(b)):
            if key not in a:
                problems.append("%s/%s: chart 多出该字段" % (path, key))
            elif key not in b:
                problems.append("%s/%s: chart 缺少该字段" % (path, key))
            else:
                walk(a[key], b[key], "%s/%s" % (path, key))
    elif isinstance(a, list):
        if len(a) != len(b):
            problems.append("%s: 列表长度 %d != %d" % (path, len(a), len(b)))
        else:
            for i, (x, y) in enumerate(zip(a, b)):
                walk(x, y, "%s[%d]" % (path, i))
    elif a != b:
        problems.append("%s: %r != %r" % (path, a, b))


for key in sorted(set(kustomize) - set(helm)):
    problems.append("chart 缺少对象 %s" % (key,))
for key in sorted(set(helm) - set(kustomize)):
    problems.append("chart 多出对象 %s" % (key,))
for key in sorted(set(kustomize) & set(helm)):
    walk(kustomize[key], helm[key], "kind=%s name=%s" % (key[0], key[2]))

if problems:
    print("FAIL chart 与 kustomize 渲染结果不一致:")
    for p in problems[:30]:
        print("  " + p)
    if len(problems) > 30:
        print("  还有 %d 处未列出" % (len(problems) - 30))
    sys.exit(1)
PY

# digest 覆盖后镜像引用必须换成 @sha256。
digest="sha256:$(printf '0%.0s' $(seq 1 64))"
if ! helm template traincache chart/traincache -n traincache \
      --set images.webhook.digest="$digest" \
      | grep -q "ghcr.io/thewor1d877/traincache/webhook@$digest"; then
  echo "FAIL chart 的 digest 覆盖没有生效" >&2
  bad=1
fi

# caBundle 必须同时写进两条 webhook 规则。
helm template traincache chart/traincache -n traincache \
  --set webhook.caBundle=dGVzdC1jYQ== >"$tmp/ca.yaml" || exit 1
python3 - "$tmp/ca.yaml" <<'PY' || bad=1
import sys
import yaml

docs = [d for d in yaml.safe_load_all(open(sys.argv[1], encoding="utf-8")) if d]
for doc in docs:
    if doc["kind"] != "MutatingWebhookConfiguration":
        continue
    bundles = [w["clientConfig"]["caBundle"] for w in doc["webhooks"]]
    if bundles != ["dGVzdC1jYQ==", "dGVzdC1jYQ=="]:
        print("FAIL caBundle 没有写进两条规则: %r" % (bundles,))
        sys.exit(1)
    break
else:
    print("FAIL 渲染结果里没有 MutatingWebhookConfiguration")
    sys.exit(1)
PY

if [ "$bad" -ne 0 ]; then
  echo "chart 校验未通过: chart/traincache 与 controller/deploy 必须同源。" >&2
  exit 1
fi

echo "OK chart: 与 kustomize 同源，digest 覆盖与 caBundle 注入均生效"
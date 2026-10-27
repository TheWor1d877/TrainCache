#!/usr/bin/env bash
# verify-release-assets.sh — 校验一个 Release 的发布物完整性。
#
# 查三件事：五项资产齐全、SHA256SUMS 逐一对得上、install.yaml 里的四个
# GHCR digest 匿名可解析（docker manifest inspect，只看 manifest 不下载层；
# 包不是 Public 这里就报 not pullable，层数据的真拉由 release-smoke 的装机段负责）。
#
# 用法:
#   TAG=v0.1.0 ./hack/verify-release-assets.sh <下载目录>
#
# 依赖: GH_TOKEN（contents: read）、gh、curl、sha256sum、docker。
# draft Release 用列表接口解析，按 tag 查单条的接口不保证返回草稿。
set -uo pipefail

DIR="${1:-.release}"
: "${TAG:?TAG=vX.Y.Z required}"
: "${GH_TOKEN:?GH_TOKEN required}"

API="repos/${GITHUB_REPOSITORY:?}/releases"
REG='ghcr.io/thewor1d877/traincache'

FAILS=0
ok()   { printf '  [OK]   %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; FAILS=$((FAILS + 1)); }

for t in gh curl sha256sum docker awk sort; do
  command -v "$t" >/dev/null 2>&1 || { echo "missing tool: $t" >&2; exit 2; }
done

printf '\n== locate release %s\n' "$TAG"
rid="$(gh api "$API" --paginate --jq '.[] | "\(.id) \(.tag_name)"' \
  | awk -v tag="$TAG" '$2 == tag { print $1; exit }')"
if [[ -z "$rid" ]]; then
  fail "no release for tag $TAG"
  printf '\nrelease-assets %s: %d failure(s)\n' "$TAG" "$FAILS"
  exit 1
fi
ok "release id $rid"

printf '\n== asset list\n'
mkdir -p "$DIR"
gh api "${API}/${rid}/assets" --paginate --jq '.[] | "\(.name)\t\(.browser_download_url)"' \
  >"$DIR/assets.tsv" || fail "list assets"

# 期望五项：清单、镜像 tar、两个 CLI、校验和文件本身。
{
  printf '%s\n' install.yaml
  printf '%s\n' "traincache-${TAG}-images.tar"
  printf '%s\n' traincache-linux-amd64
  printf '%s\n' trainpublish-linux-amd64
  printf '%s\n' SHA256SUMS
} | sort >"$DIR/expected.txt"
cut -f1 "$DIR/assets.tsv" | sort >"$DIR/got.txt"
if diff -q "$DIR/expected.txt" "$DIR/got.txt" >/dev/null 2>&1; then
  ok "5 assets present"
else
  fail "asset list differs from the expected 5"
  diff "$DIR/expected.txt" "$DIR/got.txt" || true
fi

printf '\n== download assets\n'
while IFS=$'\t' read -r name url; do
  [[ -n "$name" && -n "$url" ]] || continue
  if curl -fsSL -H "Authorization: Bearer $GH_TOKEN" -o "$DIR/$name" "$url"; then
    ok "$name"
  else
    fail "download $name"
  fi
done <"$DIR/assets.tsv"

printf '\n== checksums\n'
if (cd "$DIR" && sha256sum -c SHA256SUMS); then
  ok "SHA256SUMS matches all listed files"
else
  fail "SHA256SUMS mismatch"
fi

printf '\n== image digests\n'
mapfile -t refs < <(grep -oE "${REG}/[a-z0-9._-]+@sha256:[0-9a-f]{64}" "$DIR/install.yaml" | sort -u)
if [[ "${#refs[@]}" -eq 4 ]]; then
  ok "4 digest references in install.yaml"
else
  fail "expected 4 digest references, got ${#refs[@]}"
fi
for ref in "${refs[@]}"; do
  if docker manifest inspect "$ref" >/dev/null 2>&1; then
    ok "pullable: $ref"
  else
    fail "not pullable: $ref"
  fi
done

printf '\nrelease-assets %s: %d failure(s)\n' "$TAG" "$FAILS"
exit $((FAILS > 0))

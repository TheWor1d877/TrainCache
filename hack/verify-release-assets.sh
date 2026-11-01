#!/usr/bin/env bash
# verify-release-assets.sh — 校验一个候选产物目录的完整性。
#
# 查四件事：install.yaml 里恰好五个 digest 引用、SHA256SUMS 逐项对得上、
# 目录内每个文件都被 SHA256SUMS 覆盖、五个 GHCR digest 匿名可解析
# （docker manifest inspect，只看 manifest 不下载层；包不是 Public 这里就报
# not pullable，层数据的真拉由 release-smoke 的装机段负责）。
#
# 用法:
#   ./hack/verify-release-assets.sh <候选产物目录>
#
# 依赖: sha256sum、docker、grep、awk、find、sort。
set -uo pipefail

DIR="${1:-.candidate}"
REG='ghcr.io/thewor1d877/traincache'

FAILS=0
ok()   { printf '  [OK]   %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; FAILS=$((FAILS + 1)); }

for t in sha256sum docker grep awk find sort jq; do
  command -v "$t" >/dev/null 2>&1 || { echo "missing tool: $t" >&2; exit 2; }
done
[[ -d "$DIR" ]] || { echo "目录不存在: $DIR" >&2; exit 2; }
[[ -f "$DIR/install.yaml" ]] || { echo "缺少 $DIR/install.yaml" >&2; exit 2; }
[[ -f "$DIR/SHA256SUMS" ]] || { echo "缺少 $DIR/SHA256SUMS" >&2; exit 2; }

printf '\n== image digests in install.yaml\n'
mapfile -t refs < <(grep -oE "${REG}/[a-z0-9._-]+@sha256:[0-9a-f]{64}" "$DIR/install.yaml" | sort -u)
if [[ "${#refs[@]}" -eq 5 ]]; then
  ok "5 digest references"
else
  fail "expected 5 digest references, got ${#refs[@]}"
fi

if [[ -f "$DIR/candidate.json" && "${#refs[@]}" -eq 5 ]]; then
  printf '\n== candidate digests match install.yaml\n'
  mapfile -t cand < <(jq -r '.images[]' "$DIR/candidate.json" | sed 's/^sha256://' | sort)
  mapfile -t have < <(printf '%s\n' "${refs[@]}" | sed 's/.*@//; s/^sha256://' | sort)
  if [[ "${#cand[@]}" -eq 5 && "${cand[*]}" == "${have[*]}" ]]; then
    ok "install.yaml 的五个 digest 与 candidate.json 一致"
  else
    fail "install.yaml 的 digest 与 candidate.json 不一致"
    diff <(printf '%s\n' "${cand[@]}") <(printf '%s\n' "${have[@]}") || true
  fi
fi

printf '\n== checksums\n'
if (cd "$DIR" && sha256sum -c SHA256SUMS); then
  ok "SHA256SUMS matches all listed files"
else
  fail "SHA256SUMS mismatch"
fi

printf '\n== coverage\n'
mapfile -t listed < <(awk '{print $2}' "$DIR/SHA256SUMS" | sed 's/^\*//' | sort -u)
while IFS= read -r f; do
  [[ -n "$f" ]] || continue
  if printf '%s\n' "${listed[@]}" | grep -qxF "$f"; then
    ok "checksum covers $f"
  else
    fail "checksum does not cover $f"
  fi
done < <(find "$DIR" -maxdepth 1 -type f ! -name SHA256SUMS -printf '%f\n' | sort)

printf '\n== registry reachability\n'
for ref in "${refs[@]}"; do
  if docker manifest inspect "$ref" >/dev/null 2>&1; then
    ok "pullable: $ref"
  else
    fail "not pullable: $ref"
  fi
done

printf '\ncandidate-assets %s: %d failure(s)\n' "$DIR" "$FAILS"
exit $((FAILS > 0))

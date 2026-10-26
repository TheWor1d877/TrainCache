#!/usr/bin/env bash
# verify-doc-refs.sh — 文档里的相对 Markdown 链接必须指向真实存在的文件。
# 只看 ](target) 形式的链接；反引号里的路径常是模块相对写法（如 svc/preheat.go），
# 无法唯一解析，不在此检查。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 2

bad=0
fail() { printf '  [FAIL] %s -> %s\n' "$1" "$2"; bad=1; }

resolve() { # 先按文档目录解析，再按仓库根解析
  local file="$1" ref="$2"
  [[ -e "$(dirname "$file")/$ref" || -e "$ROOT/$ref" ]]
}

files=(README.md CHANGELOG.md CLAUDE.md)
while IFS= read -r f; do files+=("$f"); done < <(find docs -name '*.md' | sort)

for f in "${files[@]}"; do
  [[ -f "$f" ]] || continue
  while IFS= read -r ref; do
    case "$ref" in
      http://*|https://*|mailto:*|\#*|/*|~*|*'<'*|*'$'*|*'{'*) continue ;;
    esac
    ref="${ref%%#*}"
    ref="${ref%\"}"   # 去掉可能的 title 引号
    case "$ref" in
      */*|*.md|*.go|*.yaml|*.yml|*.sh|*.py|*.json|*.txt) ;;
      *) continue ;;  # 不像路径，是正文里的括号
    esac
    if resolve "$f" "$ref"; then
      continue
    elif resolve "$f" "${ref//%20/ }"; then
      continue
    elif [[ "$ref" == *' '* ]]; then
      continue  # 含空格的链接无法可靠分词，可能是正文
    fi
    fail "$f" "$ref"
  done < <(grep -oP '\]\(\K[^)]+' "$f" 2>/dev/null)
done

exit "$bad"

// Package logging 提供日志字段脱敏，供 controller 与 agent 共用。
//
// 本包只依赖标准库，不依赖 core 其他包；core 其他包也不依赖本包。
// 脱敏契约见 5.1 design §5.6 与 5.3 decisions D32。
package logging

import (
	"net/url"
	"strings"
)

// replacement 是脱敏后替入的哨兵文本。
const replacement = "***"

// sensitiveKeys 为小写比较的敏感键集合。
var sensitiveKeys = map[string]struct{}{
	"accesskey":            {},
	"access_key":           {},
	"secretkey":            {},
	"secret_key":           {},
	"token":                {},
	"authorization":        {},
	"password":             {},
	"secret":               {},
	"credential":           {},
	"signature":            {},
	"expires":              {},
	"x-amz-signature":      {},
	"x-amz-credential":     {},
	"x-amz-security-token": {},
}

// Redact 返回 s 的副本，其中命中敏感键的取值被替换为 "***"。
func Redact(s string) string {
	if s == "" {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if next, ok := redactAt(s, i, &b); ok {
			i = next
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

// RedactURL 返回 URL 的安全形式：去掉 userinfo，只保留 scheme://host 与 path，
// 敏感查询参数的值替换为 "***"，丢弃 fragment。解析失败返回 "***"。
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return replacement
	}
	u.User = nil
	u.Fragment = ""
	u.RawFragment = ""
	u.RawQuery = redactRawQuery(u.RawQuery)
	return u.String()
}

// redactAt 尝试从 s[i] 起匹配一个敏感片段，命中时写入 b 并返回片段之后的下标。
func redactAt(s string, i int, b *strings.Builder) (int, bool) {
	j := i
	quotedKey := false
	if s[j] == '"' {
		quotedKey = true
		j++
	} else if i > 0 && isKeyChar(s[i-1]) {
		// 未加引号的键必须从词边界开始，避免匹配 my_accessKey 之类的后缀。
		return i, false
	}

	keyStart := j
	for j < len(s) && isKeyChar(s[j]) {
		j++
	}
	if j == keyStart || !isSensitiveKey(s[keyStart:j]) {
		return i, false
	}

	colonSep := false
	if quotedKey {
		if j >= len(s) || s[j] != '"' {
			return i, false
		}
		j++
		for j < len(s) && isSpace(s[j]) {
			j++
		}
		if j >= len(s) || s[j] != ':' {
			return i, false
		}
		colonSep = true
		j++
	} else {
		for j < len(s) && isSpace(s[j]) {
			j++
		}
		if j >= len(s) || (s[j] != '=' && s[j] != ':') {
			return i, false
		}
		colonSep = s[j] == ':'
		j++
	}
	for j < len(s) && isSpace(s[j]) {
		j++
	}

	if j < len(s) && s[j] == '"' {
		k := j + 1
		for k < len(s) {
			if s[k] == '\\' {
				k += 2
				continue
			}
			if s[k] == '"' {
				break
			}
			k++
		}
		b.WriteString(s[i : j+1])
		b.WriteString(replacement)
		if k < len(s) {
			b.WriteByte('"')
			return k + 1, true
		}
		// 引号未闭合：吞掉剩余内容，避免敏感值泄漏。
		return len(s), true
	}

	if j >= len(s) {
		b.WriteString(s[i:j])
		b.WriteString(replacement)
		return j, true
	}

	k := j
	for k < len(s) {
		c := s[k]
		if c == '\n' || c == '\r' || c == '&' || c == ',' || c == ';' ||
			c == '}' || c == ']' || c == ')' || c == '"' || c == '\'' {
			break
		}
		if !colonSep && isSpace(c) {
			break
		}
		k++
	}
	b.WriteString(s[i:j])
	b.WriteString(replacement)
	return k, true
}

// redactRawQuery 保持参数顺序与其他参数原文，仅替换敏感键的值。
func redactRawQuery(raw string) string {
	if raw == "" {
		return raw
	}
	parts := strings.Split(raw, "&")
	for idx, part := range parts {
		if part == "" {
			continue
		}
		key := part
		if eq := strings.IndexByte(part, '='); eq >= 0 {
			key = part[:eq]
		}
		decoded, err := url.QueryUnescape(key)
		if err != nil {
			decoded = key
		}
		if isSensitiveKey(decoded) {
			parts[idx] = key + "=" + replacement
		}
	}
	return strings.Join(parts, "&")
}

func isSensitiveKey(key string) bool {
	_, ok := sensitiveKeys[strings.ToLower(key)]
	return ok
}

func isKeyChar(c byte) bool {
	return c >= 'a' && c <= 'z' ||
		c >= 'A' && c <= 'Z' ||
		c >= '0' && c <= '9' ||
		c == '_' || c == '-'
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t'
}

package logging

import (
	"context"
	"log/slog"
	"strings"
)

// NewRedactingHandler 包装 next，写入前对消息与属性脱敏。
// 字符串与 error 值过 Redact；值整体或片段是 http(s) URL 时再过 RedactURL。
//
// 两端 logger 在构造时包这一层，业务日志点不需要逐处调用 Redact。
func NewRedactingHandler(next slog.Handler) slog.Handler {
	if next == nil {
		return nil
	}
	return &redactingHandler{next: next}
}

type redactingHandler struct {
	next slog.Handler
}

func (h *redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *redactingHandler) Handle(ctx context.Context, r slog.Record) error {
	out := slog.NewRecord(r.Time, r.Level, redactText(r.Message), r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(redactAttr(a))
		return true
	})
	return h.next.Handle(ctx, out)
}

func (h *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, len(attrs))
	for i := range attrs {
		redacted[i] = redactAttr(attrs[i])
	}
	return &redactingHandler{next: h.next.WithAttrs(redacted)}
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{next: h.next.WithGroup(name)}
}

// redactAttr 按值的种类脱敏；group 递归处理。
func redactAttr(a slog.Attr) slog.Attr {
	// 属性名本身是敏感键时，值整体替换，不依赖值里是否出现 key=value。
	if isSensitiveKey(strings.ToLower(a.Key)) {
		a.Value = slog.StringValue(replacement)
		return a
	}
	a.Value = a.Value.Resolve()
	switch a.Value.Kind() {
	case slog.KindString:
		a.Value = slog.StringValue(redactText(a.Value.String()))
	case slog.KindAny:
		if err, ok := a.Value.Any().(error); ok {
			a.Value = slog.StringValue(redactText(err.Error()))
		}
	case slog.KindGroup:
		group := a.Value.Group()
		out := make([]slog.Attr, len(group))
		for i := range group {
			out[i] = redactAttr(group[i])
		}
		a.Value = slog.GroupValue(out...)
	}
	return a
}

// redactText 先按敏感键脱敏，再把文本里嵌的 http(s) URL 交给 RedactURL。
func redactText(s string) string {
	if s == "" {
		return s
	}
	return redactURLs(Redact(s))
}

// redactURLs 替换 s 里所有 http(s) URL；URL 在空白、引号或尖括号处截断。
func redactURLs(s string) string {
	var b strings.Builder
	for {
		start := indexURLScheme(s)
		if start < 0 {
			b.WriteString(s)
			return b.String()
		}
		end := start
		for end < len(s) && !isURLTerminator(s[end]) {
			end++
		}
		raw := s[start:end]
		trimmed := strings.TrimRight(raw, ".,;:!?)]}'\"")
		b.WriteString(s[:start])
		b.WriteString(RedactURL(trimmed))
		b.WriteString(raw[len(trimmed):])
		s = s[end:]
	}
}

// indexURLScheme 返回 s 中最靠前的 http:// 或 https:// 下标，没有则返回 -1。
func indexURLScheme(s string) int {
	lower := strings.ToLower(s)
	http := strings.Index(lower, "http://")
	https := strings.Index(lower, "https://")
	switch {
	case http < 0:
		return https
	case https < 0:
		return http
	case http < https:
		return http
	default:
		return https
	}
}

func isURLTerminator(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '"', '\'', '`', '<', '>':
		return true
	}
	return false
}

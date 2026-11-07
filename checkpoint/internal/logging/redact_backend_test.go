package logging

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"traincache/checkpoint/internal/obs"
)

// 本文件覆盖 10.3 覆盖矩阵里的「日志字段与脱敏」一行：已有用例只断言敏感键被替换，这里注入
// 后端返回体——一条把两把凭证与预签名 URL 原样回显给 checkpointd 的 S3 错误。
// 脱敏仍按 10.1「可观测出口」交给 core/logging 的处理器，业务代码不逐处调用 Redact，
// 所以用例从 logger 出发，断言的是「写出去的那一行里查不到凭证」。

// 哨兵取值是对象存储文档里的示例键，不是可用凭证。
const (
	sentinelAccessKey  = "AKIAIOSFODNN7EXAMPLE"
	sentinelSecretKey  = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	sentinelSignature  = "1f9c2b7ad0e44a6f9b3c8e5d2a7f1b0c"
	sentinelUserinfo   = "traincache:SuperSecretPass"
	sensitiveObjectKey = "checkpoints/llm-7b-job-0/step-1200/rank-0/model.bin"
)

// presignedURL 是后端返回体里常见的预签名地址：userinfo 与查询串都带凭证。
var presignedURL = fmt.Sprintf(
	"https://%s@minio.traincache.svc:9000/%s"+
		"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%s%%2F20261105%%2Fcn-north-1%%2Fs3%%2Faws4_request"+
		"&X-Amz-Date=20261105T090000Z&X-Amz-Expires=900&X-Amz-Signature=%s",
	sentinelUserinfo, sensitiveObjectKey, sentinelAccessKey, sentinelSignature)

// backendBody 模拟后端错误返回体：键名走 JSON，凭证与预签名 URL 混在同一个
// 字符串里。checkpointd 只能整条记进 err 字段，逐字段清洗不在契约里（10.1「可观测出口」）。
var backendBody = fmt.Sprintf(
	`plugin: UNAVAILABLE: s3 请求失败 403 {"code":"AccessDenied","message":"The security token included in the request is expired",`+
		`"accessKey":"%s","secret_key":"%s","request_url":"%s"}`,
	sentinelAccessKey, sentinelSecretKey, presignedURL)

// leakedCredentials 是任何一条日志行都不该出现的取值。
var leakedCredentials = []string{sentinelAccessKey, sentinelSecretKey, sentinelSignature, "SuperSecretPass"}

// saveFailAttrs 按 10.1「可观测出口」的写法拼出「保存或恢复失败」这条 ERROR 的字段：
// task_id、step、retry 加成对的 err 与 err_class。
func saveFailAttrs(err error, extra ...slog.Attr) []any {
	attrs := make([]any, 0, len(extra)+4)
	attrs = append(attrs, obs.Task("llm-7b-job-0"), obs.Step(1200), obs.Retry(3))
	for _, a := range extra {
		attrs = append(attrs, a)
	}
	return append(attrs, obs.Err(err)...)
}

// TestBackendBodyCredentialsRedactedInEveryLane 覆盖凭证进日志的三条路径：
// obs.Err 的 error 值、消息文本、结构化字段。三条都必须被处理器脱敏。
func TestBackendBodyCredentialsRedactedInEveryLane(t *testing.T) {
	backendErr := errors.New(backendBody)

	cases := []struct {
		name string
		emit func(*slog.Logger)
	}{
		{
			// 生产日志点的实际写法：错误体经 obs.Err 落成 err 字段。
			name: "err 属性里的后端返回体",
			emit: func(l *slog.Logger) { l.Error("保存失败", saveFailAttrs(backendErr)...) },
		},
		{
			// 消息文本本身带返回体：redactText 同样覆盖消息（core handler.go）。
			name: "消息文本里的后端返回体",
			emit: func(l *slog.Logger) {
				l.Error("后端返回 "+backendBody, obs.Err(backendErr)...)
			},
		},
		{
			// 原样转交的 backend 配置或响应体被写成普通字符串字段。
			name: "结构化字段里的凭证键值",
			emit: func(l *slog.Logger) {
				l.Error("上传失败", saveFailAttrs(backendErr,
					slog.String("response_body", backendBody),
					slog.String("presigned", presignedURL))...)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureOutput(t)
			oldDefault := slog.Default()
			t.Cleanup(func() { slog.SetDefault(oldDefault) })

			tc.emit(New("checkpointd", "dev", "info"))

			out := buf.String()
			for _, leak := range leakedCredentials {
				if strings.Contains(out, leak) {
					t.Fatalf("日志泄漏凭证 %q:\n%s", leak, out)
				}
			}
			if !strings.Contains(out, "***") {
				t.Fatalf("日志里没有脱敏哨兵，说明凭证片段未被替换:\n%s", out)
			}
			// 脱敏不能把排障要用的信息一起抹掉：错误码与对象键仍要在行里。
			if !strings.Contains(out, "AccessDenied") {
				t.Fatalf("后端错误码被一并抹掉，排障信息丢失:\n%s", out)
			}
			if !strings.Contains(out, sensitiveObjectKey) {
				t.Fatalf("预签名 URL 的对象键应保留:\n%s", out)
			}

			got := decodeOne(t, buf)
			if got["level"] != "ERROR" {
				t.Fatalf("level = %v, want ERROR", got["level"])
			}
			// err 与 err_class 成对（docs/8.1 §3.1），脱敏只改值不改字段结构。
			errText, ok := got["err"].(string)
			if !ok {
				t.Fatalf("缺少 err 字段: %v", got["err"])
			}
			if !strings.Contains(errText, "***") {
				t.Fatalf("err 字段未脱敏: %q", errText)
			}
			if got["err_class"] == nil || got["err_class"] == "" {
				t.Fatalf("err_class 应与 err 成对出现: %v", got)
			}
			if got["component"] != "checkpointd" {
				t.Fatalf("component = %v, want checkpointd", got["component"])
			}
		})
	}
}

// TestBackendBodyRedactionKeepsStepFields 确认脱敏后逐步事件的可统计字段仍在：
// step、task_id、retry 是 10.1「可观测出口」里替代指标的三个查询维度。
func TestBackendBodyRedactionKeepsStepFields(t *testing.T) {
	buf := captureOutput(t)
	oldDefault := slog.Default()
	t.Cleanup(func() { slog.SetDefault(oldDefault) })

	log := New("checkpointd", "dev", "info")
	log.Error("保存失败", saveFailAttrs(errors.New(backendBody), obs.StateAttr(obs.StateUploading))...)

	got := decodeOne(t, buf)
	if got["task_id"] != "llm-7b-job-0" || got["step"] != float64(1200) || got["retry"] != float64(3) {
		t.Fatalf("step/task_id/retry 字段应原样保留: %v", got)
	}
	if got["state"] != "uploading" {
		t.Fatalf("state = %v, want uploading", got["state"])
	}
	out := buf.String()
	for _, leak := range leakedCredentials {
		if strings.Contains(out, leak) {
			t.Fatalf("日志泄漏凭证 %q:\n%s", leak, out)
		}
	}
}

package server

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"

	"traincache/gateway/internal/origin"
)

// s3ErrorXML 是响应体解析目标（只关心 Code/Message）。
type s3ErrorXML struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func decodeS3Error(t *testing.T, body []byte) s3ErrorXML {
	t.Helper()
	var got s3ErrorXML
	if err := xml.Unmarshal(body, &got); err != nil {
		t.Fatalf("响应体不是合法 S3 XML: %v: %q", err, body)
	}
	return got
}

func errorCode(t *testing.T, body []byte) string {
	t.Helper()
	return decodeS3Error(t, body).Code
}

// TestOriginErrorPassthrough 覆盖源站 4xx/5xx：原状态码、原 Code、无网关响应头。
func TestOriginErrorPassthrough(t *testing.T) {
	for _, tc := range []struct {
		name       string
		resp       minio.ErrorResponse
		wantStatus int
		wantCode   string
	}{
		{
			name:       "403 AccessDenied",
			resp:       minio.ErrorResponse{StatusCode: http.StatusForbidden, Code: "AccessDenied", Message: "denied"},
			wantStatus: http.StatusForbidden,
			wantCode:   "AccessDenied",
		},
		{
			name:       "404 NoSuchKey",
			resp:       minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: "NoSuchKey", Message: "missing"},
			wantStatus: http.StatusNotFound,
			wantCode:   "NoSuchKey",
		},
		{
			name:       "500 InternalError",
			resp:       minio.ErrorResponse{StatusCode: http.StatusInternalServerError, Code: "InternalError", Message: "boom"},
			wantStatus: http.StatusInternalServerError,
			wantCode:   "InternalError",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(&fakeOrigin{
				statFn: func(context.Context, string) (origin.ObjectInfo, error) {
					return origin.ObjectInfo{}, tc.resp
				},
			})

			rec := serve(s, http.MethodGet, "/traincache/ds/CURRENT")
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if got := errorCode(t, rec.Body.Bytes()); got != tc.wantCode {
				t.Fatalf("Code = %q, want %q", got, tc.wantCode)
			}
			if got := rec.Header().Get(GatewayHeaderName); got != "" {
				t.Fatalf("%s = %q, 源站错误不应带网关响应头", GatewayHeaderName, got)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/xml" {
				t.Fatalf("Content-Type = %q, want application/xml", ct)
			}
		})
	}
}

// TestOriginUnavailableReturns504 覆盖连不上/TLS/超时：StatusCode==0 → 504。
func TestOriginUnavailableReturns504(t *testing.T) {
	s := newTestServer(&fakeOrigin{
		statFn: func(context.Context, string) (origin.ObjectInfo, error) {
			return origin.ObjectInfo{}, errors.New("dial tcp 127.0.0.1:9000: connect: connection refused")
		},
	})

	rec := serve(s, http.MethodGet, "/traincache/ds/CURRENT")
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", rec.Code)
	}
	if got := errorCode(t, rec.Body.Bytes()); got != GatewayCodeOriginUnavailable {
		t.Fatalf("Code = %q, want %q", got, GatewayCodeOriginUnavailable)
	}
	if got := rec.Header().Get(GatewayHeaderName); got != "" {
		t.Fatalf("%s = %q, 源站不可用不应带网关响应头", GatewayHeaderName, got)
	}
}

// TestGatewayPanicReturns503 覆盖网关自身失败：panic 恢复为 503 + unavailable 头。
func TestGatewayPanicReturns503(t *testing.T) {
	s := newTestServer(&fakeOrigin{
		statFn: func(context.Context, string) (origin.ObjectInfo, error) {
			panic("boom")
		},
	})

	rec := serve(s, http.MethodGet, "/traincache/ds/CURRENT")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := errorCode(t, rec.Body.Bytes()); got != GatewayCodeUnavailable {
		t.Fatalf("Code = %q, want %q", got, GatewayCodeUnavailable)
	}
	if got := rec.Header().Get(GatewayHeaderName); got != GatewayHeaderUnavailable {
		t.Fatalf("%s = %q, want %q", GatewayHeaderName, got, GatewayHeaderUnavailable)
	}
}

// TestErrorBodyEscapesXML 覆盖 <>& 转义与 XML 往返一致性。
func TestErrorBodyEscapesXML(t *testing.T) {
	const (
		code    = "Bad<&Code"
		message = "a<b>&c"
	)
	s := newTestServer(&fakeOrigin{
		statFn: func(context.Context, string) (origin.ObjectInfo, error) {
			return origin.ObjectInfo{}, minio.ErrorResponse{
				StatusCode: http.StatusBadRequest,
				Code:       code,
				Message:    message,
			}
		},
	})

	rec := serve(s, http.MethodGet, "/traincache/ds/CURRENT")
	body := rec.Body.String()
	if !strings.HasPrefix(body, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>`) {
		t.Fatalf("响应体前缀不符: %q", body)
	}
	for _, escaped := range []string{"Bad&lt;&amp;Code", "a&lt;b&gt;&amp;c"} {
		if !strings.Contains(body, escaped) {
			t.Fatalf("响应体缺转义片段 %q: %q", escaped, body)
		}
	}
	if strings.Contains(body, "a<b>") {
		t.Fatalf("响应体未转义: %q", body)
	}

	got := decodeS3Error(t, rec.Body.Bytes())
	if got.Code != code || got.Message != message {
		t.Fatalf("XML 往返 = %+v, want Code=%q Message=%q", got, code, message)
	}
}

package agentclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// stubCandidateResolver 替代 Kubernetes 解析器,返回预置候选或错误。
type stubCandidateResolver struct {
	addrs []string
	err   error
}

func (s stubCandidateResolver) ResolveCandidates(_ context.Context, _ string) ([]string, error) {
	return s.addrs, s.err
}

func TestResolveHandlerReturnsCandidateArray(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/resolve?node=n1", nil)
	handler := ResolveHandler(stubCandidateResolver{addrs: []string{"10.0.0.5:9410", "192.168.1.20:9410"}})

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 期望 200(%s)", rec.Code, rec.Body.String())
	}
	var got []string
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应不是 JSON 数组: %v (%s)", err, rec.Body.String())
	}
	if len(got) != 2 || got[0] != "10.0.0.5:9410" || got[1] != "192.168.1.20:9410" {
		t.Fatalf("候选 = %v", got)
	}
}

func TestResolveHandlerRequiresNodeParameter(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/resolve", nil)
	handler := ResolveHandler(stubCandidateResolver{addrs: []string{"10.0.0.5:9410"}})

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d, 期望 400(%s)", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil || payload["error"] == "" {
		t.Fatalf("错误响应体 = %s", rec.Body.String())
	}
}

func TestResolveHandlerStatusCodes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"没有 Agent 地址", fmt.Errorf("%w: %w", ErrAgentUnavailable, ErrNoAgentAddress), http.StatusNotFound},
		{"查询失败", errors.New("api down"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/resolve?node=n1", nil)
			handler := ResolveHandler(stubCandidateResolver{err: tc.err})

			handler.ServeHTTP(rec, req)

			if rec.Code != tc.want {
				t.Fatalf("状态码 = %d, 期望 %d(%s)", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestResolveHandlerRejectsNonGET(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/resolve?node=n1", nil)
	handler := ResolveHandler(stubCandidateResolver{addrs: []string{"10.0.0.5:9410"}})

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("状态码 = %d, 期望 405", rec.Code)
	}
}

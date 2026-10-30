package agentclient

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corelog "traincache/core/logging"
)

// TestErrClass 断言调用侧错误到 err_class 的映射。
func TestErrClass(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"unavailable", ErrAgentUnavailable, corelog.ErrClassUnavailable},
		{"wrapped unavailable", fmt.Errorf("%w: dial node n1: refused", ErrAgentUnavailable), corelog.ErrClassUnavailable},
		{"task not found", ErrTaskNotFound, corelog.ErrClassNotFound},
		{"permanent", ErrPermanent, corelog.ErrClassPermanent},
		{"context deadline", fmt.Errorf("progress: %w", context.DeadlineExceeded), corelog.ErrClassTimeout},
		{"grpc deadline", status.Error(codes.DeadlineExceeded, "deadline"), corelog.ErrClassTimeout},
		{"context canceled", context.Canceled, corelog.ErrClassUnavailable},
		{"other", errors.New("decode response"), corelog.ErrClassIO},
		{"nil", nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ErrClass(c.err); got != c.want {
				t.Fatalf("ErrClass(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

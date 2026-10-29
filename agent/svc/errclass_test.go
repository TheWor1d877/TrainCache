package svc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"traincache/core/cas"
	corelog "traincache/core/logging"
	"traincache/core/objectstore"
)

// TestErrClass 断言 svc 的错误到 err_class 映射:坏块、缺失、超时与 io 兜底。
func TestErrClass(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"corrupt", cas.ErrCorrupt, corelog.ErrClassCorrupt},
		{"wrapped corrupt", fmt.Errorf("open block: %w", cas.ErrCorrupt), corelog.ErrClassCorrupt},
		{"cas not found", cas.ErrNotFound, corelog.ErrClassNotFound},
		{"object not found", objectstore.ErrNotFound, corelog.ErrClassNotFound},
		{"wrapped object not found", fmt.Errorf("fetch: %w", objectstore.ErrNotFound), corelog.ErrClassNotFound},
		{"deadline", fmt.Errorf("repair: %w", context.DeadlineExceeded), corelog.ErrClassTimeout},
		{"canceled", context.Canceled, corelog.ErrClassUnavailable},
		{"io", io.ErrUnexpectedEOF, corelog.ErrClassIO},
		{"nil", nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := errClass(c.err); got != c.want {
				t.Fatalf("errClass(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

// TestErrClassIgnoresNonSentinel 断言普通错误不会被误判为 not_found。
func TestErrClassIgnoresNonSentinel(t *testing.T) {
	if got := errClass(errors.New("rename quarantine: permission denied")); got != corelog.ErrClassIO {
		t.Fatalf("普通 I/O 错误 errClass = %q, want %q", got, corelog.ErrClassIO)
	}
}

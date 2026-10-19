package objectstore

import (
	"context"
	"errors"
	"io"
	"testing"
)

// alwaysSucceedPutIfMatchStore 模拟忽略条件写前置条件的后端：PutIfMatch 永远成功。
type alwaysSucceedPutIfMatchStore struct {
	*Mock
}

func (s alwaysSucceedPutIfMatchStore) PutIfMatch(ctx context.Context, key string, r io.Reader, size int64, expectedETag string) (string, error) {
	return s.Put(ctx, key, r, size)
}

// assertNoObject 断言对象存储为空（探测临时键已清理）。
func assertNoObject(t *testing.T, s *Mock) {
	t.Helper()
	keys, err := s.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("探测后仍有对象：%v", keys)
	}
}

// TestProbeConditionalWriteSupported：支持条件写的后端（Mock）→ nil，且临时键被清理。
func TestProbeConditionalWriteSupported(t *testing.T) {
	m := NewMock()
	if err := ProbeConditionalWrite(context.Background(), m); err != nil {
		t.Fatalf("ProbeConditionalWrite(支持条件写) = %v，期望 nil", err)
	}
	assertNoObject(t, m)
}

// TestProbeConditionalWriteUnsupported：忽略条件写、总是成功的后端 →
// ErrConditionalWriteUnsupported，且临时键被清理。
func TestProbeConditionalWriteUnsupported(t *testing.T) {
	m := NewMock()
	s := alwaysSucceedPutIfMatchStore{Mock: m}
	err := ProbeConditionalWrite(context.Background(), s)
	if !errors.Is(err, ErrConditionalWriteUnsupported) {
		t.Fatalf("ProbeConditionalWrite(忽略条件写) = %v，期望 ErrConditionalWriteUnsupported", err)
	}
	assertNoObject(t, m)
}

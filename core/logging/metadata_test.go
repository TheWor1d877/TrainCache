package logging

import (
	"context"
	"testing"
)

// TestWithRequestMetaLayers 验证分层补充：后写的空字段不会抹掉先写的值。
func TestWithRequestMetaLayers(t *testing.T) {
	ctx := WithRequestMeta(context.Background(), "rid-1", "", 0)
	ctx = WithRequestMeta(ctx, "", "train/t/p0", 3)

	got := RequestMetaFrom(ctx)
	if got.RequestID != "rid-1" || got.TaskID != "train/t/p0" || got.Generation != 3 {
		t.Fatalf("RequestMetaFrom = %+v, want rid-1/train/t/p0/3", got)
	}
}

// TestRequestMetaFromAbsent 验证没有写入时返回零值，不 panic。
func TestRequestMetaFromAbsent(t *testing.T) {
	if got := RequestMetaFrom(nil); got != (RequestMeta{}) {
		t.Fatalf("RequestMetaFrom(nil) = %+v, want zero", got)
	}
	if got := RequestMetaFrom(context.Background()); got != (RequestMeta{}) {
		t.Fatalf("RequestMetaFrom(background) = %+v, want zero", got)
	}
}

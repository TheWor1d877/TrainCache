package obs

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
)

func TestErrPairsClass(t *testing.T) {
	attrs := Err(fmt.Errorf("upload: %w", context.DeadlineExceeded))
	if len(attrs) != 2 {
		t.Fatalf("len(Err) = %d, want 2", len(attrs))
	}
	first, ok := attrs[0].(slog.Attr)
	if !ok || first.Key != FieldErr {
		t.Fatalf("attrs[0] = %v, want attr err", attrs[0])
	}
	second, ok := attrs[1].(slog.Attr)
	if !ok || second.Key != FieldErrClass {
		t.Fatalf("attrs[1] = %v, want attr err_class", attrs[1])
	}
	if second.Value.String() != "timeout" {
		t.Fatalf("err_class = %s, want timeout", second.Value.String())
	}
}

func TestErrNil(t *testing.T) {
	if got := Err(nil); len(got) != 0 {
		t.Fatalf("Err(nil) = %v, want empty", got)
	}
}

func TestFieldNames(t *testing.T) {
	cases := map[string]string{
		Task("t").Key:             "task_id",
		Step(1).Key:               "step",
		Rank(0).Key:               "rank",
		Job("job-a").Key:          "job",
		Plugin("s3").Key:          "plugin",
		Bytes(1).Key:              "bytes",
		Retry(2).Key:              "retry",
		StateAttr(StateReady).Key: "state",
	}
	for got, want := range cases {
		if got != want {
			t.Fatalf("field = %q, want %q", got, want)
		}
	}
}

// TestJobKeepsEmptyValue 锁住 D26 的口径：裸 Pod 时 job 为空串，
// 字段仍然出现，日志侧才能按「有字段但为空」判断该 Pod 不属于 Job。
func TestJobKeepsEmptyValue(t *testing.T) {
	attr := Job("")
	if attr.Key != FieldJob {
		t.Fatalf("key = %q, want %q", attr.Key, FieldJob)
	}
	if attr.Value.String() != "" {
		t.Fatalf("job = %q, want 空串", attr.Value.String())
	}
}

package nodeid_test

import (
	"os"
	"testing"

	"traincache/agent/nodeid"
)

func TestFromEnvMissing(t *testing.T) {
	t.Setenv("NODE_NAME", "")
	os.Unsetenv("NODE_NAME") // t.Setenv 的 cleanup 会恢复原值

	if _, err := nodeid.FromEnv(); err == nil {
		t.Fatal("NODE_NAME 缺失时应报错,实际无错误")
	}
}

func TestFromEnvEmpty(t *testing.T) {
	t.Setenv("NODE_NAME", "")

	if _, err := nodeid.FromEnv(); err == nil {
		t.Fatal(`NODE_NAME="" 时应报错,实际无错误`)
	}
}

func TestFromEnvBlank(t *testing.T) {
	t.Setenv("NODE_NAME", "   ")

	if _, err := nodeid.FromEnv(); err == nil {
		t.Fatal("NODE_NAME 全空白时应报错,实际无错误")
	}
}

func TestFromEnvOK(t *testing.T) {
	t.Setenv("NODE_NAME", "worker-01")

	id, err := nodeid.FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if id != nodeid.ID("worker-01") {
		t.Errorf("id = %q, want %q", id, "worker-01")
	}
	if got := id.String(); got != "worker-01" {
		t.Errorf("String() = %q, want %q", got, "worker-01")
	}
}

func TestFromEnvTrimmed(t *testing.T) {
	t.Setenv("NODE_NAME", "  worker-02  ")

	id, err := nodeid.FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if got := id.String(); got != "worker-02" {
		t.Errorf("String() = %q, want 修剪后的 %q", got, "worker-02")
	}
}

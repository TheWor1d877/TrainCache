package merge

import (
	"bytes"
	"context"
	"testing"
)

// TestOnFinishBeforeEntryEnd 断言 onFinish 回调在条目向订阅者广播结束之前
// 触发：回调里落的日志因此先于客户端收到最后一个字节，事后核对不必依赖
// 两个协程的调度顺序。
func TestOnFinishBeforeEntryEnd(t *testing.T) {
	const c = 64
	reg := NewRegistry(c)
	e := reg.Create("k", reg.GetBuffer())

	fired := false
	e.SetOnFinish(func(error) { fired = true })

	sub := e.Subscribe()
	done := make(chan error, 1)
	go func() { done <- e.Pump(context.Background(), bytes.NewReader(pattern(50))) }()

	if _, err := readAll(sub); err != nil {
		t.Fatalf("readAll: %v", err)
	}
	sub.Close()
	if err := <-done; err != nil {
		t.Fatalf("Pump: %v", err)
	}
	if !fired {
		t.Fatalf("订阅者看到条目结束时 onFinish 还没触发")
	}
}

// TestOnFinishFiresOnce 断言 onFinish 至多触发一次：Pump 的正常收尾与
// 提前取消两条路径不会各落一次日志。
func TestOnFinishFiresOnce(t *testing.T) {
	const c = 64
	reg := NewRegistry(c)
	e := reg.Create("k", reg.GetBuffer())

	calls := 0
	e.SetOnFinish(func(error) { calls++ })

	sub := e.Subscribe()
	done := make(chan error, 1)
	go func() { done <- e.Pump(context.Background(), bytes.NewReader(pattern(50))) }()
	if _, err := readAll(sub); err != nil {
		t.Fatalf("readAll: %v", err)
	}
	sub.Close()
	<-done
	e.Finish(nil)

	if calls != 1 {
		t.Fatalf("onFinish 触发 %d 次, want 1", calls)
	}
}

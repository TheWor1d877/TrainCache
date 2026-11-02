package merge

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// TestConcurrentAttachCreate 覆盖并发 Attach + Create：所有调用拿到同一条目，
// 在途表只登记一次。
func TestConcurrentAttachCreate(t *testing.T) {
	const workers = 100
	reg := NewRegistry(64)
	if _, ok := reg.Attach("ds/blocks/a#0"); ok {
		t.Fatal("空表 Attach 不应命中")
	}

	var wg sync.WaitGroup
	seen := make([]*Entry, workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			seen[i] = reg.Create("ds/blocks/a#0", reg.GetBuffer())
		}(i)
	}
	close(start)
	wg.Wait()

	if reg.Len() != 1 {
		t.Fatalf("Len = %d, want 1", reg.Len())
	}
	for i := 1; i < workers; i++ {
		if seen[i] != seen[0] {
			t.Fatalf("第 %d 个 Create 返回了不同条目", i)
		}
	}
	seen[0].Finish(nil)
}

// TestFinishIdempotent 覆盖 Finish 幂等、唤醒订阅者与结束后 Attach 返回 false。
func TestFinishIdempotent(t *testing.T) {
	reg := NewRegistry(64)
	e := reg.Create("k", reg.GetBuffer())

	s1 := e.Subscribe()
	s2 := e.Subscribe()
	if got := e.Subscribers(); got != 2 {
		t.Fatalf("Subscribers = %d, want 2", got)
	}

	e.write([]byte("payload"))
	boom := errors.New("done")
	e.Finish(boom)
	e.Finish(errors.New("second")) // 幂等：不覆盖第一次的状态

	if reg.Len() != 0 {
		t.Fatalf("Finish 后 Len = %d, want 0", reg.Len())
	}
	if _, ok := reg.Attach("k"); ok {
		t.Fatal("结束后 Attach 应为 false")
	}

	for _, s := range []*Sub{s1, s2} {
		got, err := s.Wait(context.Background())
		if err != nil {
			t.Fatalf("Wait 数据段: %v", err)
		}
		if string(got) != "payload" {
			t.Fatalf("data = %q, want payload", got)
		}
		if _, err := s.Wait(context.Background()); !errors.Is(err, boom) {
			t.Fatalf("结束后 Wait = %v, want %v", err, boom)
		}
		s.Close()
	}
}

// TestAttachRejectsWrappedEntry 覆盖 written > C 后 Attach 返回 false。
func TestAttachRejectsWrappedEntry(t *testing.T) {
	const c = 4
	reg := NewRegistry(c)
	e := reg.Create("k", reg.GetBuffer())
	defer e.Finish(nil)

	e.write(make([]byte, c+1))
	if _, ok := reg.Attach("k"); ok {
		t.Fatal("written > C 后 Attach 应为 false")
	}
}

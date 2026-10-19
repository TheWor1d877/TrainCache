package cas

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// observerRecorder 是观察者调用的并发安全记录器。
type observerRecorder struct {
	mu  sync.Mutex
	got []Fingerprint
}

func (r *observerRecorder) record(fp Fingerprint) {
	r.mu.Lock()
	r.got = append(r.got, fp)
	r.mu.Unlock()
}

func (r *observerRecorder) snapshot() []Fingerprint {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Fingerprint(nil), r.got...)
}

// TestOpenObserverFiresForHitAndMissing：命中块与缺失块都在 Open 前触发观察者，
// 传 nil 清除后不再触发。
func TestOpenObserverFiresForHitAndMissing(t *testing.T) {
	s, _ := newTestStore(t, VerifyXXH3)
	fp := putBlock(t, s, []byte("observer-hit"))

	rec := &observerRecorder{}
	s.SetOpenObserver(rec.record)

	r, err := s.Open(fp)
	if err != nil {
		t.Fatalf("Open(命中块) 失败: %v", err)
	}
	_ = r.Close()

	var missing Fingerprint
	for i := range missing {
		missing[i] = 0xEE
	}
	if _, err := s.Open(missing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Open(缺失块) err = %v, want ErrNotFound", err)
	}

	got := rec.snapshot()
	if len(got) != 2 || got[0] != fp || got[1] != missing {
		t.Fatalf("观察者调用 = %v, want [%s %s]", got, fp, missing)
	}

	// nil 清除：后续 Open 不再触发。
	s.SetOpenObserver(nil)
	if _, err := s.Open(fp); err != nil {
		t.Fatalf("清除后 Open(命中块) 失败: %v", err)
	}
	if _, err := s.Open(missing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("清除后 Open(缺失块) err = %v, want ErrNotFound", err)
	}
	if got := rec.snapshot(); len(got) != 2 {
		t.Fatalf("清除后观察者仍被调用, 累计 %d 次", len(got))
	}
}

// TestOpenObserverRunsBeforeOSOpen：观察者在 os.Open 之前触发——缺失块由观察者
// 现场补齐后，同一次 Open 能读到块。
func TestOpenObserverRunsBeforeOSOpen(t *testing.T) {
	s, _ := newTestStore(t, VerifyXXH3)
	content := []byte("observer-recreate")
	fp := putBlock(t, s, content)
	if err := s.Remove(fp); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := s.Open(fp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("装置前置:Open err = %v, want ErrNotFound", err)
	}

	var called atomic.Int32
	s.SetOpenObserver(func(seen Fingerprint) {
		if seen != fp {
			return
		}
		called.Add(1)
		// 观察者内补块：证明本次 Open 的 os.Open 尚未发生。
		if _, _, err := s.Put(context.Background(), bytes.NewReader(content)); err != nil {
			t.Errorf("观察者内 Put: %v", err)
		}
	})

	r, err := s.Open(fp)
	if err != nil {
		t.Fatalf("Open(观察者补块后) 失败: %v", err)
	}
	_ = r.Close()
	if got := called.Load(); got != 1 {
		t.Fatalf("观察者调用次数 = %d, want 1", got)
	}
}

// TestSetOpenObserverConcurrentWithOpen：并发 SetOpenObserver 与 Open 在 -race 下
// 无数据竞争；并发写入的回调只用原子量。
func TestSetOpenObserverConcurrentWithOpen(t *testing.T) {
	s, _ := newTestStore(t, VerifyXXH3)
	fp := putBlock(t, s, []byte("observer-concurrent"))

	var (
		calls  atomic.Int64
		stop   = make(chan struct{})
		openWG sync.WaitGroup
		setWG  sync.WaitGroup
	)
	fn := func(Fingerprint) { calls.Add(1) }

	setWG.Add(1)
	go func() {
		defer setWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.SetOpenObserver(fn)
			s.SetOpenObserver(nil)
		}
	}()

	const workers = 8
	const iterations = 50
	for i := 0; i < workers; i++ {
		openWG.Add(1)
		go func(i int) {
			defer openWG.Done()
			for j := 0; j < iterations; j++ {
				var want Fingerprint
				if (i+j)%2 == 0 {
					want = fp
				} else {
					want[0] = 0xAA
				}
				r, err := s.Open(want)
				if err == nil {
					_ = r.Close()
					continue
				}
				if !errors.Is(err, ErrNotFound) {
					t.Errorf("Open err = %v, want nil 或 ErrNotFound", err)
				}
			}
		}(i)
	}

	openWG.Wait()
	close(stop)
	setWG.Wait()
}

package merge

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
)

// readAll 持续 Wait 到条目结束，返回拼接后的字节。
func readAll(s *Sub) ([]byte, error) {
	var got []byte
	for {
		b, err := s.Wait(context.Background())
		got = append(got, b...)
		if err == nil {
			continue
		}
		if errors.Is(err, ErrClosed) {
			return got, nil
		}
		return got, err
	}
}

// TestFanoutConsistent 覆盖三个订阅者收到完全一致的字节。
func TestFanoutConsistent(t *testing.T) {
	const c = 64
	src := pattern(50)
	reg := NewRegistry(c)
	e := reg.Create("k", reg.GetBuffer())

	subs := []*Sub{e.Subscribe(), e.Subscribe(), e.Subscribe()}
	done := make(chan error, 1)
	go func() { done <- e.Pump(context.Background(), bytes.NewReader(src)) }()

	var wg sync.WaitGroup
	got := make([][]byte, len(subs))
	for i, s := range subs {
		wg.Add(1)
		go func(i int, s *Sub) {
			defer wg.Done()
			defer s.Close()
			data, err := readAll(s)
			if err != nil {
				t.Errorf("订阅者 %d Wait: %v", i, err)
				return
			}
			got[i] = data
		}(i, s)
	}
	wg.Wait()
	if err := <-done; err != nil {
		t.Fatalf("Pump: %v", err)
	}
	for i := range got {
		if !bytes.Equal(got[i], src) {
			t.Fatalf("订阅者 %d 收到 %d 字节，want %d（内容不一致）", i, len(got[i]), len(src))
		}
	}
}

// TestBlockedSubscriberDoesNotBlockOthers 覆盖第一个订阅者阻塞时其他订阅者仍收完。
func TestBlockedSubscriberDoesNotBlockOthers(t *testing.T) {
	const c = 64
	src := pattern(40)
	reg := NewRegistry(c)
	e := reg.Create("k", reg.GetBuffer())

	slow := e.Subscribe()
	fast1 := e.Subscribe()
	fast2 := e.Subscribe()

	done := make(chan error, 1)
	go func() { done <- e.Pump(context.Background(), bytes.NewReader(src)) }()

	var wg sync.WaitGroup
	for _, s := range []*Sub{fast1, fast2} {
		wg.Add(1)
		go func(s *Sub) {
			defer wg.Done()
			defer s.Close()
			data, err := readAll(s)
			if err != nil {
				t.Errorf("快速订阅者 Wait: %v", err)
				return
			}
			if !bytes.Equal(data, src) {
				t.Errorf("快速订阅者收到 %d 字节，want %d", len(data), len(src))
			}
		}(s)
	}
	wg.Wait()
	if err := <-done; err != nil {
		t.Fatalf("Pump: %v", err)
	}

	// 阻塞的订阅者此时仍能读到完整数据（数据量小于 C，未被踢）。
	got, err := readAll(slow)
	if err != nil {
		t.Fatalf("阻塞订阅者 Wait: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("阻塞订阅者收到 %d 字节，want %d", len(got), len(src))
	}
	slow.Close()
}

// TestSlowSubscriberKicked 覆盖慢订阅者被踢且其他订阅者不受影响。
func TestSlowSubscriberKicked(t *testing.T) {
	const c = 8
	reg := NewRegistry(c)
	e := reg.Create("k", reg.GetBuffer())

	fast := e.Subscribe()
	slow := e.Subscribe()

	// 先写满一个容量，让 fast 追上进度。
	e.write(pattern(c))
	first, err := fast.Wait(context.Background())
	if err != nil {
		t.Fatalf("fast Wait: %v", err)
	}
	if len(first) != c {
		t.Fatalf("fast 首段 = %d 字节, want %d", len(first), c)
	}

	// 再多写 1 字节：slow 的 written-pos 变成 C+1，必须被踢。
	e.write([]byte{0xFF})
	if _, err := slow.Wait(context.Background()); !errors.Is(err, ErrKicked) {
		t.Fatalf("slow Wait = %v, want ErrKicked", err)
	}
	if got := e.Subscribers(); got != 1 {
		t.Fatalf("踢出后 Subscribers = %d, want 1", got)
	}

	rest, err := fast.Wait(context.Background())
	if err != nil {
		t.Fatalf("fast 后续 Wait: %v", err)
	}
	if len(rest) != 1 || rest[0] != 0xFF {
		t.Fatalf("fast 后续 = %v, want [0xFF]", rest)
	}

	e.Finish(nil)
	if _, err := fast.Wait(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("fast 结束 = %v, want ErrClosed", err)
	}
	fast.Close()
	slow.Close()
}

// TestNoAttachAfterOverflow 覆盖 written > C 后新订阅者不加入。
func TestNoAttachAfterOverflow(t *testing.T) {
	const c = 4
	reg := NewRegistry(c)
	e := reg.Create("k", reg.GetBuffer())
	defer e.Finish(nil)

	e.write(make([]byte, c+1))
	if _, ok := reg.Attach("k"); ok {
		t.Fatal("written > C 后 Attach 应返回 false")
	}
}

// TestTrySubscribeAfterFinish 覆盖「Attach 成功后、订阅前 Finish」的窗口：
// 此时缓冲已归还，TrySubscribe 必须返回 false 且不加入订阅者集合。
func TestTrySubscribeAfterFinish(t *testing.T) {
	reg := NewRegistry(64)
	e := reg.Create("k", reg.GetBuffer())
	if _, ok := reg.Attach("k"); !ok {
		t.Fatal("Attach 应命中刚创建的条目")
	}

	e.Finish(nil) // 无订阅者，缓冲立即归还

	sub, ok := e.TrySubscribe()
	if ok {
		t.Fatalf("Finish 后 TrySubscribe = true, want false")
	}
	if sub != nil {
		t.Fatalf("失败时应返回 nil 订阅者, got %v", sub)
	}
	if got := e.Subscribers(); got != 0 {
		t.Fatalf("Subscribers = %d, want 0（失败的订阅不应加入集合）", got)
	}
}

// TestWaitChunkLimit 覆盖单次 Wait 的交付上限：容量 1MiB 时每次最多
// maxChunk，拼接后与源序列逐字节一致，pos 只按实际交付推进。
func TestWaitChunkLimit(t *testing.T) {
	const c = 1 << 20
	src := pattern(c)
	reg := NewRegistry(c)
	e := reg.Create("k", reg.GetBuffer())
	sub := e.Subscribe()
	defer sub.Close()

	e.write(src)
	e.Finish(nil)

	var (
		got     []byte
		lastPos int64
		chunks  int
	)
	for {
		data, err := sub.Wait(context.Background())
		if len(data) > maxChunk {
			t.Fatalf("第 %d 次 Wait 交付 %d 字节, want <= %d", chunks+1, len(data), maxChunk)
		}
		if pos, want := sub.Pos(), lastPos+int64(len(data)); pos != want {
			t.Fatalf("pos = %d, want %d（应单调推进且等于已交付字节数）", pos, want)
		} else {
			lastPos = pos
		}
		if len(data) > 0 {
			chunks++
			got = append(got, data...)
		}
		if err != nil {
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("结束 Wait = %v, want ErrClosed", err)
			}
			break
		}
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("拼接后 %d 字节与源不一致", len(got))
	}
	if want := (c + maxChunk - 1) / maxChunk; chunks != want {
		t.Fatalf("Wait 次数 = %d, want %d", chunks, want)
	}
}

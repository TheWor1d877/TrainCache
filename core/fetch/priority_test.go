package fetch

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// TestLimiterBackgroundQuota：后台额度取全场的一半（任务槽与在途字节各自折半，至少 1）。
func TestLimiterBackgroundQuota(t *testing.T) {
	cases := []struct {
		name      string
		maxBytes  int64
		maxTasks  int
		wantBytes int64
		wantTasks int
	}{
		{"默认 1GiB/8", defaultMaxInflightBytes, defaultMaxTasks, defaultMaxInflightBytes / 2, defaultMaxTasks / 2},
		{"任务数 1 至少 1 槽", 1 << 20, 1, 1 << 19, 1},
		{"字节 1 至少 1 字节", 1, 4, 1, 2},
		{"奇数任务数向下取整", 1 << 20, 5, 1 << 19, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := newLimiter(Options{MaxInflightBytes: c.maxBytes, MaxTasks: c.maxTasks}.withDefaults())
			if got := l.bgBytes.cap; got != c.wantBytes {
				t.Fatalf("bgBytes.cap = %d, want %d", got, c.wantBytes)
			}
			if got := l.bgTasks.max; got != c.wantTasks {
				t.Fatalf("bgTasks.max = %d, want %d", got, c.wantTasks)
			}
		})
	}
}

// TestForegroundGateWake：前台在途归零时唤醒后台等待者；promoted 关闭同样立即放行。
func TestForegroundGateWake(t *testing.T) {
	g := newForegroundGate()
	g.enter()
	g.enter()

	promoted := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- g.wait(context.Background(), promoted) }()

	select {
	case <-done:
		t.Fatalf("gate.wait returned while %d foreground downloads in flight", g.activeCount())
	case <-time.After(50 * time.Millisecond):
	}
	g.exit()
	select {
	case <-done:
		t.Fatalf("gate.wait returned while 1 foreground download in flight")
	case <-time.After(50 * time.Millisecond):
	}
	g.exit()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("gate.wait err = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("gate.wait did not return after foreground idle")
	}
	if got := g.activeCount(); got != 0 {
		t.Fatalf("activeCount = %d, want 0", got)
	}

	g.enter()
	close(promoted)
	if err := g.wait(context.Background(), promoted); err != nil {
		t.Fatalf("gate.wait after promote err = %v, want nil", err)
	}
	g.exit()
}

// TestFetcherBackgroundTaskQuota：后台并发任务数不超过半额任务槽（MaxTasks=4 -> 后台 2）。
func TestFetcherBackgroundTaskQuota(t *testing.T) {
	const (
		nKeys   = 5
		maxTask = 4
		chunk   = 4 << 10
	)
	ctx := context.Background()

	ms := newFnStore(nil)
	release := make(chan struct{})
	entered := make(chan struct{}, nKeys)
	ms.get = func(ctx context.Context, k string, w io.Writer) error {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return ms.Mock.Get(ctx, k, w)
	}
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = keyName(i)
		seed(t, ms.Mock, keys[i], make([]byte, chunk))
	}

	cs, _ := newTestCAS(t)
	f := New(ms, cs, Options{MaxTasks: maxTask, MaxConnsPerHost: 64})

	results := make(chan fetchResult, nKeys)
	for i := 0; i < nKeys; i++ {
		go func(i int) {
			fp, err := f.FetchWithPriority(ctx, keys[i], chunk, PriorityBackground)
			results <- fetchResult{fp, err}
		}(i)
	}

	want := maxTask / 2
	for i := 0; i < want; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d/%d background tasks entered store.Get", i, want)
		}
	}
	select {
	case <-entered:
		t.Fatalf("background task beyond half quota (%d) entered store.Get", want)
	case <-time.After(100 * time.Millisecond):
	}
	if got := f.lim.bgTasks.current(); got != want {
		t.Fatalf("bgTasks.current() = %d, want %d", got, want)
	}

	close(release)
	for i := 0; i < nKeys; i++ {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("background Fetch: %v", r.err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("only %d/%d background Fetch returned", i, nKeys)
		}
	}
	if got := f.lim.bgTasks.current(); got != 0 {
		t.Fatalf("bgTasks.current() after completion = %d, want 0", got)
	}
}

// TestFetcherBackgroundYieldsToForeground：前台在途时后台下载不接入，前台结束后才放行。
func TestFetcherBackgroundYieldsToForeground(t *testing.T) {
	const chunk = 4 << 10
	ctx := context.Background()

	release := make(chan struct{})
	entered := make(chan string, 4)
	ms := newFnStore(func(ctx context.Context, k string, w io.Writer) error {
		entered <- k
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
	seed(t, ms.Mock, "obj/fg", make([]byte, chunk))
	seed(t, ms.Mock, "obj/bg", make([]byte, chunk))

	cs, _ := newTestCAS(t)
	f := New(ms, cs, Options{MaxTasks: 8, MaxConnsPerHost: 64})

	fgDone := make(chan fetchResult, 1)
	go func() {
		fp, err := f.Fetch(ctx, "obj/fg", chunk)
		fgDone <- fetchResult{fp, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("foreground fetch did not enter store.Get")
	}

	bgDone := make(chan fetchResult, 1)
	go func() {
		fp, err := f.FetchWithPriority(ctx, "obj/bg", chunk, PriorityBackground)
		bgDone <- fetchResult{fp, err}
	}()
	select {
	case k := <-entered:
		t.Fatalf("background fetch (%s) entered store.Get while foreground in flight", k)
	case <-time.After(200 * time.Millisecond):
	}
	if got := f.lim.gate.activeCount(); got != 1 {
		t.Fatalf("gate.activeCount() = %d, want 1", got)
	}

	close(release)
	for i, ch := range []chan fetchResult{fgDone, bgDone} {
		select {
		case r := <-ch:
			if r.err != nil {
				t.Fatalf("fetch %d: %v", i, r.err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("fetch %d did not return", i)
		}
	}
	if got := f.lim.gate.activeCount(); got != 0 {
		t.Fatalf("gate.activeCount() after completion = %d, want 0", got)
	}
}

// TestFetcherForegroundBypassesBackgroundQuota：后台占满半额任务槽时，前台仍按全场额度接入。
func TestFetcherForegroundBypassesBackgroundQuota(t *testing.T) {
	const chunk = 4 << 10
	ctx := context.Background()

	release := make(chan struct{})
	entered := make(chan string, 8)
	ms := newFnStore(func(ctx context.Context, k string, w io.Writer) error {
		entered <- k
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
	for _, k := range []string{"obj/bg0", "obj/bg1", "obj/fg"} {
		seed(t, ms.Mock, k, make([]byte, chunk))
	}

	cs, _ := newTestCAS(t)
	f := New(ms, cs, Options{MaxTasks: 4, MaxConnsPerHost: 64}) // 后台只有 2 槽

	results := make(chan fetchResult, 3)
	for _, k := range []string{"obj/bg0", "obj/bg1"} {
		go func(k string) {
			fp, err := f.FetchWithPriority(ctx, k, chunk, PriorityBackground)
			results <- fetchResult{fp, err}
		}(k)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d/2 background tasks entered store.Get", i)
		}
	}

	go func() {
		fp, err := f.Fetch(ctx, "obj/fg", chunk)
		results <- fetchResult{fp, err}
	}()
	select {
	case k := <-entered:
		if k != "obj/fg" {
			t.Fatalf("third store.Get key = %s, want obj/fg（前台不应排在后台后面）", k)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("foreground fetch did not enter store.Get while background quota full")
	}

	close(release)
	for i := 0; i < 3; i++ {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("fetch: %v", r.err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("only %d/3 Fetch returned", i)
		}
	}
}

// TestFetcherBackgroundPromotedByForegroundMerge：后台下载在让路期间被前台同 key 请求合并时，
// 立即改按前台准入，前台等待者不必等让路门开启。
func TestFetcherBackgroundPromotedByForegroundMerge(t *testing.T) {
	const chunk = 4 << 10
	ctx := context.Background()

	release := make(chan struct{})
	entered := make(chan string, 4)
	ms := newFnStore(func(ctx context.Context, k string, w io.Writer) error {
		entered <- k
		if k == "obj/slow" {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	for _, k := range []string{"obj/slow", "obj/merged"} {
		seed(t, ms.Mock, k, make([]byte, chunk))
	}

	cs, _ := newTestCAS(t)
	f := New(ms, cs, Options{MaxTasks: 8, MaxConnsPerHost: 64})

	slow := make(chan fetchResult, 1)
	go func() {
		fp, err := f.Fetch(ctx, "obj/slow", chunk)
		slow <- fetchResult{fp, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("foreground slow fetch did not enter store.Get")
	}

	bg := make(chan fetchResult, 1)
	go func() {
		fp, err := f.FetchWithPriority(ctx, "obj/merged", chunk, PriorityBackground)
		bg <- fetchResult{fp, err}
	}()
	select {
	case k := <-entered:
		t.Fatalf("background fetch (%s) entered store.Get while foreground in flight", k)
	case <-time.After(200 * time.Millisecond):
	}

	// 前台请求同一个 key：合并到后台 owner 并把它提升为前台准入，应立刻进入下载。
	merged := make(chan fetchResult, 1)
	go func() {
		fp, err := f.Fetch(ctx, "obj/merged", chunk)
		merged <- fetchResult{fp, err}
	}()
	select {
	case k := <-entered:
		if k != "obj/merged" {
			t.Fatalf("entered key = %s, want obj/merged", k)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("merged background download did not bypass the foreground gate")
	}
	select {
	case r := <-merged:
		if r.err != nil {
			t.Fatalf("merged foreground Fetch: %v", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("merged foreground Fetch did not return")
	}
	select {
	case r := <-bg:
		if r.err != nil {
			t.Fatalf("background Fetch: %v", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("background Fetch did not return")
	}
	select {
	case <-slow:
		t.Fatal("foreground slow fetch returned before release")
	default:
	}

	close(release)
	select {
	case r := <-slow:
		if r.err != nil {
			t.Fatalf("foreground slow Fetch: %v", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("foreground slow Fetch did not return after release")
	}
}

// TestFetcherBackgroundGateCtxCancel：后台在让路门等待时 ctx 取消立即返回，不占任何额度。
func TestFetcherBackgroundGateCtxCancel(t *testing.T) {
	const chunk = 4 << 10

	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	ms := newFnStore(func(ctx context.Context, k string, w io.Writer) error {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
	seed(t, ms.Mock, "obj/fg", make([]byte, chunk))
	seed(t, ms.Mock, "obj/bg", make([]byte, chunk))

	cs, _ := newTestCAS(t)
	f := New(ms, cs, Options{MaxTasks: 8, MaxConnsPerHost: 64})

	slow := make(chan error, 1)
	go func() {
		_, err := f.Fetch(context.Background(), "obj/fg", chunk)
		slow <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("foreground fetch did not enter store.Get")
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	bgDone := make(chan error, 1)
	go func() {
		_, err := f.FetchWithPriority(bgCtx, "obj/bg", chunk, PriorityBackground)
		bgDone <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-bgDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("gated background Fetch err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("gated background Fetch did not return after ctx cancel")
	}
	select {
	case <-entered:
		t.Fatal("canceled background fetch entered store.Get")
	default:
	}
	if got := f.lim.bgTasks.current(); got != 0 {
		t.Fatalf("bgTasks.current() = %d, want 0（取消不占额度）", got)
	}
	if got := f.lim.bgBytes.inflight(); got != 0 {
		t.Fatalf("bgBytes.inflight() = %d, want 0", got)
	}

	close(release)
	if err := <-slow; err != nil {
		t.Fatalf("foreground Fetch: %v", err)
	}
}

// keyName 返回第 i 个测试对象键。
func keyName(i int) string { return "obj/bg" + string(rune('0'+i)) }

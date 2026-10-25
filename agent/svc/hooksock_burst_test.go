package svc

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
)

// hooksock_burst_test.go exercises the recv side under a one-shot burst from
// many senders: no goroutine leak, exact format-error counters, and no loss of
// well-formed frames (docs/7.4 后续覆盖计划).

const burstTimeout = 10 * time.Second

func TestHookSockConcurrentBurst(t *testing.T) {
	m := newHookMetrics()
	path := hookPath(t)

	var advances, opens, stages, misses, stalls atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	s := New(path, m, HookHandlers{
		Advance:     func(ordersource.ChunkRef) { advances.Add(1) },
		Open:        func(ordersource.ChunkRef) { opens.Add(1) },
		StageSwitch: func(string, int64) { stages.Add(1) },
		Miss:        func(ordersource.ChunkRef, string) { misses.Add(1) },
		Stall:       func(ordersource.ChunkRef, int64, time.Time) { stalls.Add(1) },
	})
	if err := s.Start(ctx); err != nil {
		cancel()
		t.Fatalf("HookSock.Start: %v", err)
	}
	baseline := runtime.NumGoroutine()

	const (
		senders        = 8
		perSender      = 250 // 2000 well-formed frames, 400 per type
		badFrames      = 40  // invalid JSON
		unknownFrames  = 5   // well-formed JSON, unknown type
		oversizeFrames = 6   // one datagram above 4 KiB
	)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func(sender int) {
			defer wg.Done()
			c, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
			if err != nil {
				t.Errorf("sender %d dial: %v", sender, err)
				return
			}
			defer func() { _ = c.Close() }()
			<-start
			for j := 0; j < perSender; j++ {
				var frame string
				switch (sender*perSender + j) % 5 {
				case 0:
					frame = fmt.Sprintf(`{"type":"advance","path":"shard-%02d.tar","chunk":%d}`, sender, j)
				case 1:
					frame = fmt.Sprintf(`{"type":"open_report","path":"shard-%02d.tar","chunk":%d}`, sender, j)
				case 2:
					frame = fmt.Sprintf(`{"type":"stage_switch","stage_id":"epoch-%d","shuffle_seed":%d}`, sender, j)
				case 3:
					frame = fmt.Sprintf(`{"type":"read_miss","path":"shard-%02d.tar","chunk":%d,"reason":"corrupt"}`, sender, j)
				case 4:
					frame = fmt.Sprintf(`{"type":"read_stall","path":"shard-%02d.tar","chunk":%d,"wait_ms":%d}`, sender, j, j)
				}
				// A full receiver queue blocks here instead of dropping, so
				// every frame below is accounted for.
				if _, err := c.Write([]byte(frame)); err != nil {
					t.Errorf("sender %d write frame %d: %v", sender, j, err)
					return
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	c := dialHook(t, path)
	for i := 0; i < badFrames; i++ {
		sendHook(t, c, []byte(`{"type":"advance",`))
	}
	for i := 0; i < unknownFrames; i++ {
		sendHook(t, c, []byte(`{"type":"bogus"}`))
	}
	oversize := bytes.Repeat([]byte("x"), 5000)
	for i := 0; i < oversizeFrames; i++ {
		sendHook(t, c, oversize)
	}

	wantFrames := int64(senders * perSender)
	wantPerType := wantFrames / 5
	wantParse := float64(badFrames + unknownFrames)
	wantSize := float64(oversizeFrames)
	deadline := time.Now().Add(burstTimeout)
	for {
		gotFrames := advances.Load() + opens.Load() + stages.Load() + misses.Load() + stalls.Load()
		gotParse := testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonParse))
		gotSize := testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonSize))
		if gotFrames == wantFrames && gotParse == wantParse && gotSize == wantSize {
			if advances.Load() != wantPerType || opens.Load() != wantPerType ||
				stages.Load() != wantPerType || misses.Load() != wantPerType || stalls.Load() != wantPerType {
				t.Fatalf("per-type counts = %d/%d/%d/%d/%d, want %d each",
					advances.Load(), opens.Load(), stages.Load(), misses.Load(), stalls.Load(), wantPerType)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("burst incomplete: frames %d/%d parse %v/%v size %v/%v",
				gotFrames, wantFrames, gotParse, wantParse, gotSize, wantSize)
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	if err := s.Close(); err != nil {
		t.Errorf("HookSock.Close: %v", err)
	}
	deadline = time.Now().Add(burstTimeout)
	for {
		if n := runtime.NumGoroutine(); n <= baseline {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: %d goroutines, baseline %d", runtime.NumGoroutine(), baseline)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

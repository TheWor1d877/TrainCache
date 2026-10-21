package agentclient

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	agentpb "traincache/agent/api/agentpb"
)

const bufTarget = "passthrough:///bufnet"

type fakePreheatServer struct {
	agentpb.UnimplementedPreheatServiceServer

	mu       sync.Mutex
	err      error
	delay    time.Duration
	submits  map[string]*agentpb.SubmitPreheatRequest
	updates  map[string]*agentpb.UpdatePreheatRequest
	cancels  map[string]*agentpb.CancelPreheatRequest
	statuses int
}

func newFakePreheatServer() *fakePreheatServer {
	return &fakePreheatServer{
		submits: map[string]*agentpb.SubmitPreheatRequest{},
		updates: map[string]*agentpb.UpdatePreheatRequest{},
		cancels: map[string]*agentpb.CancelPreheatRequest{},
	}
}

func (s *fakePreheatServer) respond(ctx context.Context) error {
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *fakePreheatServer) SubmitPreheat(ctx context.Context, req *agentpb.SubmitPreheatRequest) (*agentpb.SubmitPreheatResponse, error) {
	if err := s.respond(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.submits[req.TaskId] = req
	s.mu.Unlock()
	return &agentpb.SubmitPreheatResponse{Accepted: true, TaskId: req.TaskId}, nil
}

func (s *fakePreheatServer) UpdatePreheat(ctx context.Context, req *agentpb.UpdatePreheatRequest) (*agentpb.UpdatePreheatResponse, error) {
	if err := s.respond(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.updates[req.TaskId] = req
	s.mu.Unlock()
	return &agentpb.UpdatePreheatResponse{Accepted: true}, nil
}

func (s *fakePreheatServer) CancelPreheat(ctx context.Context, req *agentpb.CancelPreheatRequest) (*agentpb.CancelPreheatResponse, error) {
	if err := s.respond(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.cancels[req.TaskId] = req
	s.mu.Unlock()
	return &agentpb.CancelPreheatResponse{Accepted: true}, nil
}

func (s *fakePreheatServer) GetPreheatStatus(ctx context.Context, req *agentpb.GetPreheatStatusRequest) (*agentpb.PreheatStatus, error) {
	if err := s.respond(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.statuses++
	s.mu.Unlock()
	return &agentpb.PreheatStatus{TaskId: req.TaskId, Generation: 7, Seq: 3}, nil
}

func startFakeServer(t *testing.T) (*fakePreheatServer, *bufconn.Listener) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	fake := newFakePreheatServer()
	agentpb.RegisterPreheatServiceServer(srv, fake)
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return fake, lis
}

func bufDialer(lis *bufconn.Listener, calls *atomic.Int32) grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		if calls != nil {
			calls.Add(1)
		}
		return lis.DialContext(ctx)
	})
}

func testPool(lis *bufconn.Listener, timeout time.Duration, calls *atomic.Int32) *GRPCPool {
	return NewPool(StaticResolver{"n1": bufTarget}, timeout, WithDialOptions(bufDialer(lis, calls)))
}

func TestPoolSubmitUpdateCancelProgress(t *testing.T) {
	fake, lis := startFakeServer(t)
	pool := testPool(lis, time.Second, nil)
	defer pool.Close()
	ctx := context.Background()

	submit := &agentpb.SubmitPreheatRequest{TaskId: "train/t/p0", Node: "n1", Generation: 2}
	if err := pool.Submit(ctx, "n1", submit); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := pool.Update(ctx, "n1", &agentpb.UpdatePreheatRequest{TaskId: "train/t/p0", Node: "n1", Generation: 3}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := pool.Cancel(ctx, "n1", &agentpb.CancelPreheatRequest{TaskId: "train/t/p0", Node: "n1", Generation: 4}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	progress, err := pool.Progress(ctx, "n1", "train/t/p0", 2)
	if err != nil || progress == nil {
		t.Fatalf("Progress = (%v, %v)", progress, err)
	}
	if progress.Generation != 7 || progress.Seq != 3 {
		t.Fatalf("progress = %#v", progress)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.submits["train/t/p0"] == nil || fake.updates["train/t/p0"] == nil || fake.cancels["train/t/p0"] == nil {
		t.Fatalf("rpc calls not recorded: %#v %#v %#v", fake.submits, fake.updates, fake.cancels)
	}
	if fake.statuses != 1 {
		t.Fatalf("status calls = %d", fake.statuses)
	}
}

func TestPoolUnavailableThenRecovers(t *testing.T) {
	fake, lis := startFakeServer(t)
	pool := testPool(lis, time.Second, nil)
	defer pool.Close()
	ctx := context.Background()

	fake.mu.Lock()
	fake.err = status.Error(codes.Unavailable, "agent down")
	fake.mu.Unlock()
	if err := pool.Submit(ctx, "n1", &agentpb.SubmitPreheatRequest{TaskId: "t"}); !errors.Is(err, ErrAgentUnavailable) {
		t.Fatalf("Submit error = %v, want ErrAgentUnavailable", err)
	}

	fake.mu.Lock()
	fake.err = nil
	fake.mu.Unlock()
	if err := pool.Submit(ctx, "n1", &agentpb.SubmitPreheatRequest{TaskId: "t"}); err != nil {
		t.Fatalf("Submit after recovery: %v", err)
	}
}

func TestPoolPermanentErrors(t *testing.T) {
	fake, lis := startFakeServer(t)
	pool := testPool(lis, time.Second, nil)
	defer pool.Close()
	ctx := context.Background()

	for _, code := range []codes.Code{codes.InvalidArgument, codes.FailedPrecondition} {
		fake.mu.Lock()
		fake.err = status.Error(code, "rejected")
		fake.mu.Unlock()
		if err := pool.Submit(ctx, "n1", &agentpb.SubmitPreheatRequest{TaskId: "t"}); !errors.Is(err, ErrPermanent) {
			t.Fatalf("code %s error = %v, want ErrPermanent", code, err)
		}
		if errors.Is(fake.err, ErrAgentUnavailable) {
			t.Fatalf("permanent error classified as unavailable")
		}
	}
}

func TestPoolTimeoutIsUnavailable(t *testing.T) {
	fake, lis := startFakeServer(t)
	pool := testPool(lis, 20*time.Millisecond, nil)
	defer pool.Close()

	fake.mu.Lock()
	fake.delay = 500 * time.Millisecond
	fake.mu.Unlock()

	start := time.Now()
	err := pool.Submit(context.Background(), "n1", &agentpb.SubmitPreheatRequest{TaskId: "t"})
	if !errors.Is(err, ErrAgentUnavailable) {
		t.Fatalf("Submit error = %v, want ErrAgentUnavailable", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout took %s", elapsed)
	}
}

func TestPoolHonorsCallerCancellation(t *testing.T) {
	_, lis := startFakeServer(t)
	pool := testPool(lis, time.Second, nil)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pool.Submit(ctx, "n1", &agentpb.SubmitPreheatRequest{TaskId: "t"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit error = %v, want context.Canceled", err)
	}
}

func TestPoolReusesConnectionPerNode(t *testing.T) {
	_, lis := startFakeServer(t)
	var dials atomic.Int32
	pool := testPool(lis, time.Second, &dials)
	defer pool.Close()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := pool.Submit(ctx, "n1", &agentpb.SubmitPreheatRequest{TaskId: "t"}); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dial calls = %d, want 1 (connection reused)", got)
	}
}

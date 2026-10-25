package agentclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	agentpb "traincache/agent/api/agentpb"
)

// Error classes separate retryable transport failures from protocol errors
// (docs/7.1 §5.9). Callers retry ErrAgentUnavailable with backoff and surface
// ErrPermanent as a status condition.
var (
	ErrAgentUnavailable = errors.New("agentclient: agent unavailable")
	ErrPermanent        = errors.New("agentclient: permanent agent error")
)

// DefaultRPCTimeout is the per-call timeout from CONTROLLER_AGENT_TIMEOUT.
const DefaultRPCTimeout = 5 * time.Second

// Pool is the frozen Agent client contract from docs/7.1 §5.9. Progress reuses
// GetPreheatStatus; polling itself belongs to M5.
type Pool interface {
	Submit(ctx context.Context, node string, req *agentpb.SubmitPreheatRequest) error
	Update(ctx context.Context, node string, req *agentpb.UpdatePreheatRequest) error
	Cancel(ctx context.Context, node string, req *agentpb.CancelPreheatRequest) error
	Progress(ctx context.Context, node string, taskID string, generation uint64) (*agentpb.PreheatStatus, error)
}

// GRPCPool keeps one gRPC ClientConn per Node and resolves addresses lazily.
// gRPC ClientConns already multiplex and reconnect, so no third-party pool is
// used (plan M4).
type GRPCPool struct {
	resolver    AddressResolver
	timeout     time.Duration
	dialOptions []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// PoolOption customizes a GRPCPool (tests inject bufconn dialers).
type PoolOption func(*GRPCPool)

// WithDialOptions appends gRPC dial options after the insecure default.
func WithDialOptions(opts ...grpc.DialOption) PoolOption {
	return func(p *GRPCPool) {
		p.dialOptions = append(p.dialOptions, opts...)
	}
}

// NewPool builds a pool using resolver for Node -> endpoint discovery.
func NewPool(resolver AddressResolver, timeout time.Duration, opts ...PoolOption) *GRPCPool {
	if timeout <= 0 {
		timeout = DefaultRPCTimeout
	}
	p := &GRPCPool{
		resolver: resolver,
		timeout:  timeout,
		conns:    map[string]*grpc.ClientConn{},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Submit implements Pool.
func (p *GRPCPool) Submit(ctx context.Context, node string, req *agentpb.SubmitPreheatRequest) error {
	return p.call(ctx, node, func(ctx context.Context, c agentpb.PreheatServiceClient) error {
		_, err := c.SubmitPreheat(ctx, req)
		return err
	})
}

// Update implements Pool.
func (p *GRPCPool) Update(ctx context.Context, node string, req *agentpb.UpdatePreheatRequest) error {
	return p.call(ctx, node, func(ctx context.Context, c agentpb.PreheatServiceClient) error {
		_, err := c.UpdatePreheat(ctx, req)
		return err
	})
}

// Cancel implements Pool.
func (p *GRPCPool) Cancel(ctx context.Context, node string, req *agentpb.CancelPreheatRequest) error {
	return p.call(ctx, node, func(ctx context.Context, c agentpb.PreheatServiceClient) error {
		_, err := c.CancelPreheat(ctx, req)
		return err
	})
}

// Progress implements Pool by reusing GetPreheatStatus (M5 polling consumes it).
func (p *GRPCPool) Progress(ctx context.Context, node string, taskID string, generation uint64) (*agentpb.PreheatStatus, error) {
	_ = generation // stale-generation handling is M5's responsibility.
	var out *agentpb.PreheatStatus
	err := p.call(ctx, node, func(ctx context.Context, c agentpb.PreheatServiceClient) error {
		resp, err := c.GetPreheatStatus(ctx, &agentpb.GetPreheatStatusRequest{TaskId: taskID})
		if err == nil {
			out = resp
		}
		return err
	})
	return out, err
}

// Close closes every cached connection. It is safe to call more than once.
func (p *GRPCPool) Close() error {
	p.mu.Lock()
	conns := p.conns
	p.conns = map[string]*grpc.ClientConn{}
	p.mu.Unlock()

	var err error
	for _, conn := range conns {
		if cerr := conn.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

func (p *GRPCPool) call(ctx context.Context, node string, fn func(context.Context, agentpb.PreheatServiceClient) error) error {
	conn, err := p.conn(ctx, node)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	classified := classify(ctx, fn(callCtx, agentpb.NewPreheatServiceClient(conn)))
	if errors.Is(classified, ErrAgentUnavailable) {
		// Force a fresh resolve/dial on the next call; the Agent Pod may have
		// moved to a new IP while the old ClientConn still targets it.
		p.drop(node)
	}
	return classified
}

func (p *GRPCPool) conn(ctx context.Context, node string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	if conn, ok := p.conns[node]; ok {
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	if p.resolver == nil {
		return nil, fmt.Errorf("%w: no address resolver for node %s", ErrAgentUnavailable, node)
	}
	target, err := p.resolver.Resolve(ctx, node)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[node]; ok {
		return conn, nil
	}
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, p.dialOptions...)
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: dial node %s: %v", ErrAgentUnavailable, node, err)
	}
	p.conns[node] = conn
	return conn, nil
}

func (p *GRPCPool) drop(node string) {
	p.mu.Lock()
	conn := p.conns[node]
	delete(p.conns, node)
	p.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// classify maps transport failures to ErrAgentUnavailable and protocol
// rejections to ErrPermanent. Caller cancellation is returned unchanged so it
// can be distinguished from a timeout.
func classify(parent context.Context, err error) error {
	if err == nil {
		return nil
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("%w: %v", ErrAgentUnavailable, err)
	}
	switch st.Code() {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.Unimplemented,
		codes.NotFound, codes.PermissionDenied, codes.Unauthenticated, codes.OutOfRange:
		return fmt.Errorf("%w: %s: %s", ErrPermanent, st.Code(), st.Message())
	default:
		// Unavailable, DeadlineExceeded, ResourceExhausted, Unknown,
		// Aborted, Internal, Canceled (without parent cancellation) are all
		// treated as retryable transport-level failures.
		return fmt.Errorf("%w: %s: %s", ErrAgentUnavailable, st.Code(), st.Message())
	}
}

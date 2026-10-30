package grpcmeta

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"traincache/agent/internal/logging"
	corelog "traincache/core/logging"
)

// captureHealth 记录入站 metadata，并用 ctx 里的 logger 写一条日志供断言。
type captureHealth struct {
	healthpb.UnimplementedHealthServer

	mu sync.Mutex
	md metadata.MD
}

func (h *captureHealth) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		h.mu.Lock()
		h.md = md.Copy()
		h.mu.Unlock()
	}
	logging.From(ctx).Info("meta probe")
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (h *captureHealth) incoming() metadata.MD {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.md.Copy()
}

// startBufconn 起一个带拦截器的 health 服务，返回客户端连接。
func startBufconn(t *testing.T, h healthpb.HealthServer) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(UnaryServerInterceptor))
	healthpb.RegisterHealthServer(gs, h)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// captureLog 把默认 logger 换成写 buffer 的 JSON handler，供 handler 内的
// logging.From(ctx) 落盘。
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

func decodeFields(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatal("no log line captured")
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("log line is not JSON: %v: %q", err, line)
	}
	return got
}

// TestUnaryServerInterceptorPropagatesMetadata 断言三键到达 Agent，并写进
// ctx logger。
func TestUnaryServerInterceptorPropagatesMetadata(t *testing.T) {
	buf := captureLog(t)
	srv := &captureHealth{}
	conn := startBufconn(t, srv)

	ctx := metadata.AppendToOutgoingContext(context.Background(),
		corelog.MetadataKeyRequestID, "rid-1",
		corelog.MetadataKeyTask, "train/t/p0",
		corelog.MetadataKeyGeneration, "3",
	)
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("Check: %v", err)
	}

	md := srv.incoming()
	for key, want := range map[string]string{
		corelog.MetadataKeyRequestID:  "rid-1",
		corelog.MetadataKeyTask:       "train/t/p0",
		corelog.MetadataKeyGeneration: "3",
	} {
		if got := md.Get(key); len(got) != 1 || got[0] != want {
			t.Fatalf("metadata %s = %v, want %q", key, got, want)
		}
	}

	fields := decodeFields(t, buf)
	if fields["request_id"] != "rid-1" || fields["task_id"] != "train/t/p0" || fields["generation"] != float64(3) {
		t.Fatalf("ctx logger fields = %v, want request_id/task_id/generation", fields)
	}
}

// TestUnaryServerInterceptorMissingMetadata 断言缺键时 RPC 正常，日志不带字段。
func TestUnaryServerInterceptorMissingMetadata(t *testing.T) {
	buf := captureLog(t)
	srv := &captureHealth{}
	conn := startBufconn(t, srv)

	if _, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("Check: %v", err)
	}
	if md := srv.incoming(); len(md.Get(corelog.MetadataKeyRequestID)) != 0 {
		t.Fatalf("unexpected request_id metadata: %v", md)
	}

	fields := decodeFields(t, buf)
	for _, key := range []string{"request_id", "task_id", "generation"} {
		if _, ok := fields[key]; ok {
			t.Fatalf("field %s should be absent when metadata is missing: %v", key, fields)
		}
	}
}

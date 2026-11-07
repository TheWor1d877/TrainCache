package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// controllerStub 起一个只回候选地址的控制面替身。
func controllerStub(t *testing.T, addrs []string, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("node") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(addrs)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestAgentCandidatesExplicitEndpointWins(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	addrs, probe, err := agentCandidates(context.Background(), agentTarget{
		Endpoint: "10.0.0.9:9410",
		Node:     "n1",
	})
	if err != nil {
		t.Fatalf("agentCandidates: %v", err)
	}
	if len(addrs) != 1 || addrs[0] != "10.0.0.9:9410" {
		t.Fatalf("addrs = %v", addrs)
	}
	if probe {
		t.Fatal("显式 --endpoint 不该试连")
	}
}

func TestAgentCandidatesLocalNode(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	addrs, probe, err := agentCandidates(context.Background(), agentTarget{Node: "n1"})
	if err != nil {
		t.Fatalf("agentCandidates: %v", err)
	}
	if len(addrs) != 1 || addrs[0] != defaultEndpoint {
		t.Fatalf("addrs = %v, 期望本机地址", addrs)
	}
	if probe {
		t.Fatal("本机 Agent 不该试连")
	}
}

func TestAgentCandidatesWithoutNode(t *testing.T) {
	t.Setenv("NODE_NAME", "n1")
	addrs, probe, err := agentCandidates(context.Background(), agentTarget{})
	if err != nil {
		t.Fatalf("agentCandidates: %v", err)
	}
	if len(addrs) != 1 || addrs[0] != defaultEndpoint {
		t.Fatalf("addrs = %v, 期望本机地址", addrs)
	}
	if probe {
		t.Fatal("没给节点名时不该试连")
	}
}

func TestAgentCandidatesFromController(t *testing.T) {
	t.Setenv("NODE_NAME", "")
	srv := controllerStub(t, []string{"10.0.0.5:9410", "192.168.1.20:9410"}, http.StatusOK)

	addrs, probe, err := agentCandidates(context.Background(), agentTarget{
		Controller: srv.URL,
		Node:       "n1",
	})
	if err != nil {
		t.Fatalf("agentCandidates: %v", err)
	}
	if len(addrs) != 2 || addrs[0] != "10.0.0.5:9410" || addrs[1] != "192.168.1.20:9410" {
		t.Fatalf("addrs = %v", addrs)
	}
	if !probe {
		t.Fatal("控制面给出的候选要试连")
	}
}

func TestControllerCandidatesReportsErrorStatus(t *testing.T) {
	srv := controllerStub(t, nil, http.StatusNotFound)
	_, err := controllerCandidates(context.Background(), srv.URL, "n1")
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("err = %v, 期望含 404", err)
	}
}

// TestControllerCandidatesUsesResolvePath 控制面把 /resolve 注册在 metrics 服务器上,
// 请求必须落到这个路径上;基址只给 host:port 时请求路径为空会打到 / 上,
// 控制面返回 404,按节点名拨号就永远连不上。
func TestControllerCandidatesUsesResolvePath(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]string{"10.0.0.5:9410"})
	}))
	defer srv.Close()

	if _, err := controllerCandidates(context.Background(), srv.URL, "n1"); err != nil {
		t.Fatalf("controllerCandidates: %v", err)
	}
	if gotPath != resolvePath {
		t.Fatalf("请求路径 = %q, 期望 %q", gotPath, resolvePath)
	}
}

// TestControllerCandidatesKeepsBasePath 基址里带路径时,查询路径接在基址路径后面,
// 不覆盖基址自带的路径前缀。
func TestControllerCandidatesKeepsBasePath(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]string{"10.0.0.5:9410"})
	}))
	defer srv.Close()

	if _, err := controllerCandidates(context.Background(), srv.URL+"/base", "n1"); err != nil {
		t.Fatalf("controllerCandidates: %v", err)
	}
	if gotPath != "/base"+resolvePath {
		t.Fatalf("请求路径 = %q, 期望 %q", gotPath, "/base"+resolvePath)
	}
}

func TestControllerCandidatesRejectsBadBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()

	if _, err := controllerCandidates(context.Background(), srv.URL, "n1"); err == nil {
		t.Fatal("响应体不是 JSON 数组时应报错")
	}
}

func TestControllerBaseAddsScheme(t *testing.T) {
	if got := controllerBase("127.0.0.1:9412"); got != "http://127.0.0.1:9412" {
		t.Fatalf("controllerBase = %q", got)
	}
	if got := controllerBase("https://ctrl:9412/"); got != "https://ctrl:9412" {
		t.Fatalf("controllerBase = %q", got)
	}
}

func TestResolveCommandPrintsCandidates(t *testing.T) {
	t.Setenv("NODE_NAME", "")
	srv := controllerStub(t, []string{"10.0.0.5:9410", "192.168.1.20:9410"}, http.StatusOK)

	var stdout, stderr bytes.Buffer
	code := resolveCommand([]string{"--node", "n1", "--controller", srv.URL}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("退出码 = %d, 期望 %d(stderr=%q)", code, exitOK, stderr.String())
	}
	want := "10.0.0.5:9410\n192.168.1.20:9410\n"
	if stdout.String() != want {
		t.Fatalf("输出 = %q, 期望 %q", stdout.String(), want)
	}
}

func TestResolveCommandRequiresNode(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := resolveCommand([]string{}, io.Discard, &stderr); code != exitUsage {
		t.Fatalf("退出码 = %d, 期望 %d", code, exitUsage)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, 期望为空", stdout.String())
	}
}

func TestResolveCommandReportsControllerError(t *testing.T) {
	t.Setenv("NODE_NAME", "")
	srv := controllerStub(t, nil, http.StatusNotFound)

	var stdout, stderr bytes.Buffer
	code := resolveCommand([]string{"--node", "n1", "--controller", srv.URL}, &stdout, &stderr)
	if code != exitError {
		t.Fatalf("退出码 = %d, 期望 %d", code, exitError)
	}
	if !strings.Contains(stderr.String(), "解析节点地址失败") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, 期望为空", stdout.String())
	}
}

func TestDefaultControllerEndpointReadsEnv(t *testing.T) {
	t.Setenv(envControllerEndpoint, "ctrl.example:9412")
	if got := defaultControllerEndpoint(); got != "ctrl.example:9412" {
		t.Fatalf("defaultControllerEndpoint = %q", got)
	}
	t.Setenv(envControllerEndpoint, "")
	if got := defaultControllerEndpoint(); got != inClusterControllerEndpoint {
		t.Fatalf("defaultControllerEndpoint = %q", got)
	}
}

// closedAddr 返回一个没人监听的本地地址。
func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

func TestProbeAgentFailsOnClosedPort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	conn, err := probeAgent(ctx, closedAddr(t))
	if err == nil {
		_ = conn.Close()
		t.Fatal("关闭的端口不该连上")
	}
}

func TestDialAgentPicksFirstReachableCandidate(t *testing.T) {
	t.Setenv("NODE_NAME", "")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)

	srv2 := controllerStub(t, []string{closedAddr(t), listener.Addr().String()}, http.StatusOK)

	conn, err := dialAgent(context.Background(), agentTarget{
		Controller: srv2.URL,
		Node:       "n1",
	})
	if err != nil {
		t.Fatalf("dialAgent: %v", err)
	}
	defer conn.Close()
	if got := conn.Target(); got != listener.Addr().String() {
		t.Fatalf("target = %q, 期望 %q", got, listener.Addr().String())
	}
}

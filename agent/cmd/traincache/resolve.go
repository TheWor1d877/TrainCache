// resolve.go:按节点名解析 Agent 地址(docs/7.1-design.md《地址解析端点》,
// docs/CLI-INTERFACE.md《地址解析顺序》)。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const (
	// inClusterControllerEndpoint 是集群内 controller-manager Service 的 DNS 名。
	inClusterControllerEndpoint = "traincache-controller-manager.traincache.svc:9412"
	// envControllerEndpoint 是控制面地址的环境变量名。
	envControllerEndpoint = "TRAINCACHE_CONTROLLER_ENDPOINT"
	// resolvePath 是控制面上按节点名查 Agent 地址的 HTTP 路径。
	resolvePath = "/resolve"
	// resolveTimeout 是查控制面与逐个试连的总时限。
	resolveTimeout = 10 * time.Second
	// probeTimeout 是单个候选地址的试连时限。
	probeTimeout = 2 * time.Second
	// maxResolveBody 是控制面响应体的读取上限。
	maxResolveBody = 1 << 20
)

// agentTarget 是一次寻址的输入。
type agentTarget struct {
	// Endpoint 是显式给定的 Agent 地址,非空时直接用它。
	Endpoint string
	// Controller 是控制面地址,用于按节点名解析。
	Controller string
	// Node 是目标节点名。
	Node string
}

// defaultControllerEndpoint 返回控制面地址:环境变量 TRAINCACHE_CONTROLLER_ENDPOINT
// 优先,未设置时用集群内 Service 的 DNS 名。
func defaultControllerEndpoint() string {
	if v := strings.TrimSpace(os.Getenv(envControllerEndpoint)); v != "" {
		return v
	}
	return inClusterControllerEndpoint
}

// agentCandidates 按优先顺序给出候选 Agent 地址。第二个返回值表示这些候选
// 是否需要试连:显式 --endpoint 与本机 Agent 直接拨号,控制面给出的候选要
// 逐个试连,取第一个连上的。
func agentCandidates(ctx context.Context, t agentTarget) ([]string, bool, error) {
	switch {
	case t.Endpoint != "":
		return []string{t.Endpoint}, false, nil
	case t.Node == "":
		return []string{defaultEndpoint}, false, nil
	case os.Getenv("NODE_NAME") == t.Node:
		// Agent DaemonSet 用 Downward API 注入 NODE_NAME,本机就是目标节点。
		return []string{defaultEndpoint}, false, nil
	default:
		addrs, err := controllerCandidates(ctx, t.Controller, t.Node)
		if err != nil {
			return nil, false, err
		}
		return addrs, true, nil
	}
}

// dialAgent 连上要操作的 Agent。控制面给出的候选按顺序试连,取第一个连上的;
// 一个都连不上时报错。
func dialAgent(ctx context.Context, t agentTarget) (*grpc.ClientConn, error) {
	candidates, probe, err := agentCandidates(ctx, t)
	if err != nil {
		return nil, err
	}
	if !probe {
		return dial(candidates[0])
	}
	var lastErr error
	for _, addr := range candidates {
		probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		conn, err := probeAgent(probeCtx, addr)
		cancel()
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("节点 %s 的 Agent 地址都连不上(%s): %w",
		t.Node, strings.Join(candidates, ", "), lastErr)
}

// probeAgent 连一次 Agent,等到 gRPC 连接进入 Ready 才算连上;超时不候。
func probeAgent(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	conn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	conn.Connect()
	for {
		switch state := conn.GetState(); state {
		case connectivity.Ready:
			return conn, nil
		case connectivity.Shutdown:
			_ = conn.Close()
			return nil, fmt.Errorf("连接 %s 已关闭", addr)
		default:
			if !conn.WaitForStateChange(ctx, state) {
				_ = conn.Close()
				return nil, fmt.Errorf("连接 %s 超时: %w", addr, ctx.Err())
			}
		}
	}
}

// controllerCandidates 向控制面查一个节点的候选 Agent 地址。
func controllerCandidates(ctx context.Context, endpoint, node string) ([]string, error) {
	target, err := url.Parse(controllerBase(endpoint))
	if err != nil {
		return nil, fmt.Errorf("控制器地址 %q 非法: %w", endpoint, err)
	}
	// 查询路径挂在控制面基址后面:控制面把 /resolve 注册在 metrics 服务器上,
	// 基址只给 host:port 时请求要落到 / 而不是基址本身。
	target.Path = strings.TrimRight(target.Path, "/") + resolvePath
	query := target.Query()
	query.Set("node", node)
	target.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("构造请求失败: %w", err)
	}
	client := &http.Client{Timeout: resolveTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("访问控制器 %s 失败: %w", target, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResolveBody))
	if err != nil {
		return nil, fmt.Errorf("读取控制器响应失败: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("控制器返回 %d: %s", resp.StatusCode, resolveErrorText(body))
	}
	var addrs []string
	if err := json.Unmarshal(body, &addrs); err != nil {
		return nil, fmt.Errorf("解析控制器响应失败: %w", err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("节点 %s 没有 Agent 地址", node)
	}
	return addrs, nil
}

// controllerBase 把控制面地址补成 URL;不带 scheme 时按 http 处理。
func controllerBase(endpoint string) string {
	base := strings.TrimSpace(endpoint)
	if base == "" {
		base = inClusterControllerEndpoint
	}
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	return strings.TrimRight(base, "/")
}

// resolveErrorText 从错误响应体里取出说明文字。
func resolveErrorText(body []byte) string {
	var payload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && payload.Error != "" {
		return payload.Error
	}
	return strings.TrimSpace(string(body))
}

// runResolve 实现 `traincache resolve`:按节点名解析 Agent 地址并打印。
func runResolve(args []string) int {
	return resolveCommand(args, os.Stdout, os.Stderr)
}

// resolveCommand 是 runResolve 的可测形式:输出流由调用方注入。
func resolveCommand(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("traincache resolve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	controller := fs.String("controller", defaultControllerEndpoint(), "控制面地址(全局选项)")
	node := fs.String("node", "", "目标节点(必填)")
	if err := fs.Parse(args); err != nil {
		return parseExitCode(err)
	}
	if fs.NArg() > 0 {
		return usageError(fs, "resolve: 不接受位置参数 %q", fs.Args())
	}
	if *node == "" {
		return usageError(fs, "resolve: --node 必填")
	}

	ctx, cancel := context.WithTimeout(context.Background(), resolveTimeout)
	defer cancel()

	candidates, _, err := agentCandidates(ctx, agentTarget{
		Controller: *controller,
		Node:       *node,
	})
	if err != nil {
		fmt.Fprintf(stderr, "traincache: 解析节点地址失败: %v\n", err)
		return exitError
	}
	for _, addr := range candidates {
		fmt.Fprintln(stdout, addr)
	}
	return exitOK
}

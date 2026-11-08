package agentclient

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// DefaultMetricsPort 是 Agent /metrics HTTP 端口（agent 默认 :9411，见 agent/svc/config.go D5）。
const DefaultMetricsPort = 9411

// NodeMetrics 是 controller 从一个 Agent 上能读到的窄指标切片。
// 字段都是零值表示"不可用或未上报"——调用方据此跳过带宽判断。
type NodeMetrics struct {
	// FetchBytesTotal 是 traincache_fetch_bytes_total{result="success",reason="prefetch|demand|repair|other"}
	// 所有 label 组合的累加值。只取 result="success" 标签；reason 不限。
	FetchBytesTotal float64
	// ScrapeAt 是本次抓取时间，供 controller 计算 observedThroughput。
	ScrapeAt time.Time
}

// MetricsFetcher 通过 agent AddressResolver 在 node 的 9411 端口抓 /metrics
// 并解析 Prometheus exposition 格式。零值即可用（DefaultPort+10s timeout）。
type MetricsFetcher struct {
	Resolver   AddressResolver
	Port       int // 默认 DefaultMetricsPort
	Timeout    time.Duration
	HTTPClient *http.Client // nil 时构造默认 client
}

// FetchNodeMetrics 抓取 node 的 Agent /metrics，返回 NodeMetrics。
// 网络错误返回 error（调用方应降级跳过带宽条件，不阻塞整个 gate）。
// 解析失败或指标缺失不返回 error——NodeMetrics 里对应字段保持零值。
func (f *MetricsFetcher) FetchNodeMetrics(ctx context.Context, node string) (*NodeMetrics, error) {
	if f.Resolver == nil {
		return nil, fmt.Errorf("metrics: no resolver for node %s", node)
	}
	grpcAddr, err := f.Resolver.Resolve(ctx, node)
	if err != nil {
		return nil, fmt.Errorf("metrics: resolve node %s: %w", node, err)
	}
	// gRPC port 默认 9410，metrics 默认 9411：替换端口即可（同主机）。
	metricsPort := f.Port
	if metricsPort <= 0 {
		metricsPort = DefaultMetricsPort
	}
	metricsAddr := replacePort(grpcAddr, metricsPort)

	timeout := f.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	httpClient := f.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: timeout}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("http://%s/metrics", metricsAddr), nil)
	if err != nil {
		return nil, fmt.Errorf("metrics: build request: %w", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("metrics: GET %s: %w", metricsAddr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics: GET %s returned %d", metricsAddr, resp.StatusCode)
	}

	nm := &NodeMetrics{ScrapeAt: time.Now().UTC()}
	parsePrometheus(resp.Body, nm)
	return nm, nil
}

// replacePort 把 host:oldPort 改成 host:newPort。host 可能是 IPv6，
// 用 net.SplitHostPort 正确处理。如果 addr 本身不带端口，直接拼接。
func replacePort(addr string, newPort int) string {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return net.JoinHostPort(host, fmt.Sprint(newPort))
	}
	// 裸 host（无端口）：直接拼接。
	return fmt.Sprintf("%s:%d", addr, newPort)
}

// parsePrometheus 从 exposition 文本里提取 traincache_fetch_bytes_total
// （累加 result="success" 标签）。只认最简单的 `metric_name{labels} value`
// 形式，略过 # 注释和 TYPE/HELP 行。
//
// traincache_fetch_bandwidth_limit_bytes 不解析：agent 侧它是回源在途字节上限
// （core/fetch.MaxInflightBytes），不是网络带宽上限。controller 的带宽上限来自
// 自己的配置 GateNetworkBandwidthBytes。
func parsePrometheus(r io.Reader, out *NodeMetrics) {
	scanner := bufio.NewScanner(r)
	// Prometheus exposition 单行可能很长（含大量 label），给 64KB 缓冲。
	scanner.Buffer(make([]byte, 0, 64*1024), 1*1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, labels, valueStr, ok := splitMetricLine(line)
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			continue
		}
		switch name {
		case "traincache_fetch_bytes_total":
			// 只累加 result="success"。
			if labelValue(labels, "result") == "success" {
				out.FetchBytesTotal += v
			}
		}
	}
}

// splitMetricLine 解析 `name{labels} value` 或 `name value`。
func splitMetricLine(line string) (name string, labels map[string]string, value string, ok bool) {
	// 找第一个 '{' 或空格。
	lb := strings.IndexByte(line, '{')
	rb := strings.IndexByte(line, '}')
	space := strings.IndexByte(line, ' ')

	if lb >= 0 && rb >= 0 && rb > lb && (space < 0 || rb < space) {
		// 有 labels。
		name = strings.TrimSpace(line[:lb])
		labelStr := line[lb+1 : rb]
		rest := strings.TrimSpace(line[rb+1:])
		if idx := strings.IndexByte(rest, ' '); idx >= 0 {
			value = strings.TrimSpace(rest[idx:])
		} else {
			value = rest
		}
		labels = parseLabels(labelStr)
	} else if space > 0 {
		name = strings.TrimSpace(line[:space])
		value = strings.TrimSpace(line[space+1:])
	} else {
		return "", nil, "", false
	}
	if name == "" || value == "" {
		return "", nil, "", false
	}
	return name, labels, value, true
}

// parseLabels 解析 `k="v",k2="v2"` 格式。
func parseLabels(s string) map[string]string {
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		idx := strings.IndexByte(pair, '=')
		if idx <= 0 {
			continue
		}
		k := strings.TrimSpace(pair[:idx])
		v := strings.TrimSpace(pair[idx+1:])
		// 去掉引号。
		v = strings.Trim(v, "\"")
		out[k] = v
	}
	return out
}

// labelValue 取 labels 里 key 的值，缺失返回空串。
func labelValue(labels map[string]string, key string) string {
	if labels == nil {
		return ""
	}
	return labels[key]
}

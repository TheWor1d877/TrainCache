package agentclient

import (
	"strings"
	"testing"
)

func TestParsePrometheusFetchBytes(t *testing.T) {
	body := `# HELP traincache_fetch_bytes_total fetched bytes total
# TYPE traincache_fetch_bytes_total counter
traincache_fetch_bytes_total{dataset="ds1",result="success",reason="prefetch"} 104857600
traincache_fetch_bytes_total{dataset="ds1",result="success",reason="demand"} 52428800
traincache_fetch_bytes_total{dataset="ds1",result="error",reason="prefetch"} 1024
traincache_fetch_bytes_total{dataset="other",result="success",reason="other"} 2048

# HELP traincache_fetch_bandwidth_limit_bytes bytes per second cap
# TYPE traincache_fetch_bandwidth_limit_bytes gauge
traincache_fetch_bandwidth_limit_bytes{dataset="ds1"} 524288000
traincache_fetch_bandwidth_limit_bytes{dataset="other"} 0
`
	out := &NodeMetrics{}
	parsePrometheus(strings.NewReader(body), out)

	wantBytes := 104857600.0 + 52428800.0 + 2048.0 // error 不计
	if out.FetchBytesTotal != wantBytes {
		t.Errorf("FetchBytesTotal = %f, want %f", out.FetchBytesTotal, wantBytes)
	}
	// traincache_fetch_bandwidth_limit_bytes 不解析：它是 agent 的回源在途字节
	// 上限，不是网络带宽上限，controller 的带宽上限来自自己的配置。
	// 上面的断言已覆盖这一点：正文里带这两行，FetchBytesTotal 不受其影响。
}

func TestParsePrometheusEmpty(t *testing.T) {
	out := &NodeMetrics{}
	parsePrometheus(strings.NewReader("# just comments\n\n"), out)
	if out.FetchBytesTotal != 0 {
		t.Errorf("empty body should leave FetchBytesTotal at zero, got %f", out.FetchBytesTotal)
	}
}

func TestReplacePort(t *testing.T) {
	cases := []struct {
		addr    string
		newPort int
		want    string
	}{
		{"10.0.0.1:9410", 9411, "10.0.0.1:9411"},
		{"[::1]:9410", 9411, "[::1]:9411"},
		{"node.example.com:9410", 9411, "node.example.com:9411"},
		{"10.0.0.1", 9411, "10.0.0.1:9411"}, // 无端口，直接拼接
	}
	for _, c := range cases {
		got := replacePort(c.addr, c.newPort)
		if got != c.want {
			t.Errorf("replacePort(%q, %d) = %q, want %q", c.addr, c.newPort, got, c.want)
		}
	}
}

func TestSplitMetricLine(t *testing.T) {
	cases := []struct {
		line       string
		wantName   string
		wantLabels map[string]string
		wantValue  string
		wantOK     bool
	}{
		{
			"foo{result=\"success\",reason=\"prefetch\"} 123.45",
			"foo",
			map[string]string{"result": "success", "reason": "prefetch"},
			"123.45",
			true,
		},
		{
			"simple 42",
			"simple",
			nil,
			"42",
			true,
		},
		{
			"traincache_fetch_bytes_total{dataset=\"ds1\",result=\"success\",reason=\"prefetch\"} 104857600",
			"traincache_fetch_bytes_total",
			map[string]string{"dataset": "ds1", "result": "success", "reason": "prefetch"},
			"104857600",
			true,
		},
	}
	for _, c := range cases {
		name, labels, value, ok := splitMetricLine(c.line)
		if ok != c.wantOK {
			t.Errorf("splitMetricLine(%q) ok = %v, want %v", c.line, ok, c.wantOK)
			continue
		}
		if name != c.wantName {
			t.Errorf("splitMetricLine(%q) name = %q, want %q", c.line, name, c.wantName)
		}
		if value != c.wantValue {
			t.Errorf("splitMetricLine(%q) value = %q, want %q", c.line, value, c.wantValue)
		}
		for k, want := range c.wantLabels {
			if labels == nil || labels[k] != want {
				t.Errorf("splitMetricLine(%q) labels[%q] = %q, want %q", c.line, k, labels[k], want)
			}
		}
	}
}

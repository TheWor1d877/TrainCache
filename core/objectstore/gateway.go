package objectstore

// gateway.go：S3 合并回源网关的错误契约与客户端支持（10.1 design §4.4）。
//
// 网关与源站共用 S3 协议，但「网关这一跳坏了」必须与「源站坏了」区分开：前者
// 503 + x-traincache-gateway: unavailable，Agent 可据同一次 Get 降级直连；后者
// 原样透传，不算网关失效（10.2 D4/D13）。

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// 网关响应头与 XML 错误码（10.1 §4.2/§4.4；取值与 gateway 模块各写一份，D24）。
const (
	// GatewayHeaderName 是网关自报「这一跳坏了」的响应头名。
	GatewayHeaderName = "x-traincache-gateway"
	// GatewayHeaderUnavailable 是 GatewayHeaderName 的唯一有效取值。
	GatewayHeaderUnavailable = "unavailable"
	// GatewayCodeUnavailable 表示网关自身失败（条目创建失败、拉取协程内部错误、panic 恢复）。
	GatewayCodeUnavailable = "XTraincacheGatewayUnavailable"
	// GatewayCodeSlowDown 表示准入排队超时或队列满。
	GatewayCodeSlowDown = "XTraincacheGatewaySlowDown"
	// GatewayCodeOriginDown 表示网关连不上源站、TLS 失败或超时。
	GatewayCodeOriginDown = "XTraincacheGatewayOriginUnavailable"
)

// 网关失败的类型化表达（Agent 选择器据此决定是否降级与 reason）。
var (
	// ErrGatewayUnavailable 表示网关这一跳坏了：503 且响应头自报 unavailable，
	// 或错误码为 GatewayCodeUnavailable。
	ErrGatewayUnavailable = errors.New("objectstore: gateway unavailable")
	// ErrGatewayRejected 表示网关主动拒绝（准入排队超时/队列满），网关本身仍然健康。
	ErrGatewayRejected = errors.New("objectstore: gateway rejected the request")
)

// TransportError 表示网关路径的连接层失败（连不上、TLS 失败、超时、流中断）。
// 只有安装了响应头探针的网关 Store 才会产出该类型；源站的错误语义保持不变。
type TransportError struct{ Err error }

// Error 实现 error（前缀冻结，便于日志归类）。
func (e *TransportError) Error() string {
	if e.Err == nil {
		return "objectstore: gateway transport: <nil>"
	}
	return "objectstore: gateway transport: " + e.Err.Error()
}

// Unwrap 返回底层错误，供 errors.Is/errors.As 穿透。
func (e *TransportError) Unwrap() error { return e.Err }

// IsGatewayTransport 判断 err 是否是网关连接层失败。
func IsGatewayTransport(err error) bool {
	var te *TransportError
	return errors.As(err, &te)
}

// IsGatewayTimeout 判断 err 是否属于超时（context.DeadlineExceeded 或 net.Error.Timeout）。
func IsGatewayTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// Options 是 S3Store 的可选构造参数。
type Options struct {
	// Transport 是底层 HTTP 传输层；nil 时使用 http.DefaultTransport。
	Transport http.RoundTripper
	// Region 固定 SigV4 签名区域（10.2 D17）。非空时 minio-go 不再请求
	// GET /<bucket>?location=，网关只需处理 GET 与 HEAD 对象请求。
	Region string
	// MaxRetries 覆盖 SDK 的重试次数（>0 时生效；0 沿用 minio-go 全局默认）。
	// 网关的 503 是控制信号（准入拒绝/降级触发），不允许 SDK 放大重试次数
	// （10.1 §4.2/§6.5）。minio-go 的 Minio.Options.MaxRetries 只接受正数，
	// 0 表示沿用全局默认 minio.MaxRetry；本仓库的 v7.3.0 中该值语义为
	// 「总尝试次数」（1 = 不重试）。
	MaxRetries int
}

// gatewayProbeKey 是每请求探针在 ctx 中的键（仅本包内部使用，不跨包）。
type gatewayProbeKey struct{}

// gatewayProbe 记录一次 Get 期间网关是否返回了 unavailable 响应头。
// minio-go 不暴露响应头，只能由 RoundTripper 在收到响应时写入。
type gatewayProbe struct {
	unavailable atomic.Bool
}

func (p *gatewayProbe) markUnavailable()    { p.unavailable.Store(true) }
func (p *gatewayProbe) isUnavailable() bool { return p.unavailable.Load() }

// withGatewayProbe 把新探针挂进 ctx。
func withGatewayProbe(ctx context.Context) (context.Context, *gatewayProbe) {
	p := &gatewayProbe{}
	return context.WithValue(ctx, gatewayProbeKey{}, p), p
}

// gatewayProbeFromContext 取回 ctx 上的探针；没有则返回 nil。
func gatewayProbeFromContext(ctx context.Context) *gatewayProbe {
	if ctx == nil {
		return nil
	}
	p, _ := ctx.Value(gatewayProbeKey{}).(*gatewayProbe)
	return p
}

// gatewayTransport 是网关 Store 专用的 RoundTripper 包装：把
// x-traincache-gateway: unavailable 记入当前请求的探针。
type gatewayTransport struct{ base http.RoundTripper }

func (t *gatewayTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if resp != nil && resp.Header.Get(GatewayHeaderName) == GatewayHeaderUnavailable {
		if p := gatewayProbeFromContext(req.Context()); p != nil {
			p.markUnavailable()
		}
	}
	return resp, err
}

// NewS3StoreWithOptions 构造带网关响应头探针的 S3 客户端（供 Agent 的网关 Store 使用）。
// endpoint/accessKey/secretKey/bucket/secure 语义与 NewS3Store 相同。
func NewS3StoreWithOptions(endpoint, accessKey, secretKey, bucket string, secure bool, opts Options) (*S3Store, error) {
	return newS3Store(endpoint, accessKey, secretKey, bucket, secure, opts, true)
}

// newS3Store 是 S3Store 的唯一构造入口。probe 为 true 时安装响应头探针；
// NewS3Store 走 probe=false，保持既有传输层默认值与错误语义逐字不变。
func newS3Store(endpoint, accessKey, secretKey, bucket string, secure bool, opts Options, probe bool) (*S3Store, error) {
	minioOpts := &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
		Region: opts.Region,
	}
	// >0 才写入：minio-go 的 0 表示不覆盖，此时沿用全局 minio.MaxRetry。
	if opts.MaxRetries > 0 {
		minioOpts.MaxRetries = opts.MaxRetries
	}
	if probe {
		base := opts.Transport
		if base == nil {
			base = http.DefaultTransport
		}
		minioOpts.Transport = &gatewayTransport{base: base}
	} else if opts.Transport != nil {
		minioOpts.Transport = opts.Transport
	}
	client, err := minio.New(endpoint, minioOpts)
	if err != nil {
		return nil, err
	}
	return &S3Store{client: client, bucket: bucket, probe: probe}, nil
}

// classifyGetError 按 10.1 §4.4 的顺序分类 Get 三类出错点（GetObject/Stat/io.Copy）
// 返回的错误。probe 为 nil 表示源站 Store，此时只在对象不存在时改写，其余原样返回。
func classifyGetError(ctx context.Context, probe *gatewayProbe, err error) error {
	// 1. 响应头命中 unavailable：即使响应体不是合法 S3 XML 也成立。
	if probe != nil && probe.isUnavailable() {
		return fmt.Errorf("%w: %v", ErrGatewayUnavailable, err)
	}
	resp := minio.ToErrorResponse(err)
	// 2. 错误码 unavailable（无响应头时由 Code 判定）。
	if resp.Code == GatewayCodeUnavailable {
		return fmt.Errorf("%w: %v", ErrGatewayUnavailable, err)
	}
	// 3. 准入拒绝：网关健康，只是暂时不放行。
	if resp.Code == GatewayCodeSlowDown {
		return fmt.Errorf("%w: %v", ErrGatewayRejected, err)
	}
	// 4. 对象不存在（既有语义）。
	if isNoSuchObject(err) {
		return ErrNotFound
	}
	// 5. 连接层失败：没有 HTTP 状态码且 ctx 未被取消/超时。只有网关 Store 才类型化。
	if resp.StatusCode == 0 && ctx.Err() == nil && probe != nil {
		return &TransportError{Err: err}
	}
	// 6. 其余原样返回（含源站 4xx/5xx）。
	return err
}

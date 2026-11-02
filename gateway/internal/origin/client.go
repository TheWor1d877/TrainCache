// Package origin 封装网关到源站的对象存储客户端（design 10.1 §3、§4.1）。
//
// 只提供两种操作：Stat（HEAD）与 Open（单次 GET，懒发送）。本包不改写
// minio 的错误语义，源站错误与连接层失败都由 server 层按 4.2 的规则映射。
package origin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ObjectInfo 是 HEAD 需要回填到 HTTP 响应头的最小字段集。
type ObjectInfo struct {
	Size         int64
	ETag         string
	LastModified time.Time
	ContentType  string
}

// Client 是绑定单个桶的 minio-go 客户端（v1 单桶，10.2 D18）。
type Client struct {
	client *minio.Client
	bucket string
}

// New 构造源站客户端。endpoint 形如 "127.0.0.1:9000"，不带 scheme；
// secure 为 true 时网关到源站使用 TLS。accessKey/secretKey 为空时按匿名访问处理。
func New(endpoint, accessKey, secretKey, bucket string, secure bool) (*Client, error) {
	transport, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, fmt.Errorf("origin: 构造传输层失败: %w", err)
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:    secure,
		Transport: &metadataTransport{base: transport},
	})
	if err != nil {
		return nil, fmt.Errorf("origin: 构造源站客户端失败: %w", err)
	}
	return &Client{client: client, bucket: bucket}, nil
}

// Stat 对对象做一次 HEAD，返回长度、ETag、修改时间与内容类型。
func (c *Client) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	info, err := c.client.StatObject(ctx, c.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{
		Size:         info.Size,
		ETag:         info.ETag,
		LastModified: info.LastModified,
		ContentType:  info.ContentType,
	}, nil
}

// Open 发起一次 GET。minio-go 的 GetObject 是懒发送：连接与 4xx/5xx 错误
// 在首次 Read 时才暴露，由调用方处理（design 10.1 §6.3）。
func (c *Client) Open(ctx context.Context, key string) (*Object, error) {
	cap := &metadataCapture{}
	obj, err := c.client.GetObject(context.WithValue(ctx, metadataKey{}, cap),
		c.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return &Object{rc: obj, stat: cap.load}, nil
}

// Object 是一次 GET 的读取器：Read/Close 透传底层读取器，Stat 返回元数据。
type Object struct {
	rc   io.ReadCloser
	stat func() (ObjectInfo, error)
}

func (o *Object) Read(p []byte) (int, error) { return o.rc.Read(p) }

func (o *Object) Close() error { return o.rc.Close() }

// Stat 返回对象元数据，不发新请求：值来自 metadataTransport 记录的 GET
// 响应头（design 10.1 §5.2 的元数据来源）。
//
// 不直接调用 minio.Object.Stat：v7.3.0 的 Stat 会把 objectInfo.Size 改写成
// 「剩余字节数」，而 Object.Read 用该值判断 EOF，读流中调用会提前截断响应体。
func (o *Object) Stat() (ObjectInfo, error) { return o.stat() }

// NewObject 用给定读取器与固定元数据构造 Object。生产路径由 Client.Open 使用
// minio.Object；server 的测试替身也用它把普通读取器包成 *Object。
func NewObject(rc io.ReadCloser, info ObjectInfo) *Object {
	return &Object{rc: rc, stat: func() (ObjectInfo, error) { return info, nil }}
}

// metadataKey 是 context 键：Open 用它把一次 GET 的响应头捕获器交给 transport。
type metadataKey struct{}

// metadataCapture 保存一次对象 GET 的响应头元数据，由 transport 写入、
// Object.Stat 读取。
type metadataCapture struct {
	mu   sync.Mutex
	info ObjectInfo
	ok   bool
}

// store 在收到 200 响应头时记录元数据；同一 GET 的重试会覆盖为最后一次。
func (c *metadataCapture) store(resp *http.Response) {
	h := resp.Header
	info := ObjectInfo{
		Size:        resp.ContentLength,
		ETag:        h.Get("ETag"),
		ContentType: h.Get("Content-Type"),
	}
	if t, err := http.ParseTime(h.Get("Last-Modified")); err == nil {
		info.LastModified = t
	}

	c.mu.Lock()
	c.info = info
	c.ok = true
	c.mu.Unlock()
}

// load 返回已记录的元数据；GET 响应头尚未到达时返回错误。
func (c *metadataCapture) load() (ObjectInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.ok {
		return ObjectInfo{}, errors.New("origin: GET 响应头尚未到达")
	}
	return c.info, nil
}

// metadataTransport 包装 minio 的传输层，把对象 GET 的 200 响应头交给该
// 请求 context 中的捕获器。桶位置查询（?location=）不是对象 GET，跳过。
type metadataTransport struct {
	base http.RoundTripper
}

func (t *metadataTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if req.Method == http.MethodGet && resp.StatusCode == http.StatusOK &&
		!req.URL.Query().Has("location") {
		if c, ok := req.Context().Value(metadataKey{}).(*metadataCapture); ok {
			c.store(resp)
		}
	}
	return resp, nil
}

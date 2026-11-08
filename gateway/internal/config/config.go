// Package config 读取并校验网关的运行配置（design 9.1「配置」）。
//
// env 解析 helper 从 agent/svc/config.go 复制而来（envOr/envBool/正整数），
// 不导出、不抽到 core：网关与 Agent 是两个独立模块，按各自的依赖方向
// 保持各自一份，避免为几个字符串引入跨模块依赖。
//
// 配置一次性读取，不做热加载；非法值返回错误，进程退出码 1。
package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// 默认值逐字来自 design 9.1「配置」。
const (
	defaultObjstoreEndpoint  = "127.0.0.1:9000"
	defaultObjstoreBucket    = "traincache"
	defaultListen            = ":9413"
	defaultBucketConcurrency = 64
	defaultBlockBufferBytes  = 10485760
	defaultLogLevel          = "info"
	defaultCacheDatasetBytes = 34359738368 // 32GiB
	defaultCacheTotalBytes   = 68719476736 // 64GiB
)

// env 名（design 9.1「配置」冻结；沿用的是全仓同名键）。
const (
	envObjstoreEndpoint    = "TRAINCACHE_OBJSTORE_ENDPOINT"
	envObjstoreAccessKey   = "TRAINCACHE_OBJSTORE_ACCESS_KEY"
	envObjstoreSecretKey   = "TRAINCACHE_OBJSTORE_SECRET_KEY"
	envObjstoreBucket      = "TRAINCACHE_OBJSTORE_BUCKET"
	envObjstoreSecure      = "TRAINCACHE_OBJSTORE_SECURE"
	envObjstoreCompression = "TRAINCACHE_OBJSTORE_COMPRESSION"
	envListen              = "TRAINCACHE_GATEWAY_LISTEN"
	envBucketConcurrency   = "TRAINCACHE_GATEWAY_BUCKET_CONCURRENCY"
	envBlockBufferBytes    = "TRAINCACHE_GATEWAY_BLOCK_BUFFER_BYTES"
	envLogLevel            = "TRAINCACHE_LOG_LEVEL"
	envCache               = "TRAINCACHE_GATEWAY_CACHE"
	envCacheDatasetBytes   = "TRAINCACHE_GATEWAY_CACHE_DATASET_BYTES"
	envCacheTotalBytes     = "TRAINCACHE_GATEWAY_CACHE_TOTAL_BYTES"
)

// Config 是网关进程的完整配置（design 9.1「配置」）。
type Config struct {
	ObjstoreEndpoint  string // TRAINCACHE_OBJSTORE_ENDPOINT，默认 127.0.0.1:9000（不带 scheme）
	ObjstoreAccessKey string // TRAINCACHE_OBJSTORE_ACCESS_KEY，默认空
	ObjstoreSecretKey string // TRAINCACHE_OBJSTORE_SECRET_KEY，默认空
	Bucket            string // TRAINCACHE_OBJSTORE_BUCKET，默认 traincache（v1 单桶）
	ObjstoreSecure    bool   // TRAINCACHE_OBJSTORE_SECURE，默认 false（"1"/"true"/"yes" 为真）
	// ObjstoreCompression 允许网关向源站声明 gzip，默认 false（压缩默认关闭）。
	// 网关解压后按原始字节扇出，指纹与内容校验不受影响；带 Range 的请求不参与协商。
	ObjstoreCompression bool
	Listen              string // TRAINCACHE_GATEWAY_LISTEN，默认 :9413（S3 与探针共用）
	BucketConcurrency   int    // TRAINCACHE_GATEWAY_BUCKET_CONCURRENCY，默认 64（正整数）
	BlockBufferBytes    int    // TRAINCACHE_GATEWAY_BLOCK_BUFFER_BYTES，默认 10485760（正整数）
	LogLevel            string // TRAINCACHE_LOG_LEVEL，默认 info（未知值退化为 info）

	// 磁盘缓存（D29/D31）。缓存根目录是代码常量，不走 env。
	Cache             bool  // TRAINCACHE_GATEWAY_CACHE，默认 true（未设置即开；"1"/"true"/"yes" 开，其余取值关）
	CacheDatasetBytes int64 // TRAINCACHE_GATEWAY_CACHE_DATASET_BYTES，默认 34359738368（32GiB，正整数）
	CacheTotalBytes   int64 // TRAINCACHE_GATEWAY_CACHE_TOTAL_BYTES，默认 68719476736（64GiB，正整数，且不小于单 dataset 上限）
}

// Load 从环境变量加载配置并校验（design 9.1「配置」）：
// 整数必须 >0，端口必须 1..65535，枚举外的日志级别退化为 info 而不报错。
// 缓存开关取宽松口径：未设置即开，只有 "1"/"true"/"yes" 为真，其余取值一律当作关（D29）。
func Load() (*Config, error) {
	listen, err := loadListen()
	if err != nil {
		return nil, err
	}
	endpoint, err := loadObjstoreEndpoint()
	if err != nil {
		return nil, err
	}
	concurrency, err := envPositiveInt(envBucketConcurrency, defaultBucketConcurrency)
	if err != nil {
		return nil, err
	}
	bufferBytes, err := envPositiveInt(envBlockBufferBytes, defaultBlockBufferBytes)
	if err != nil {
		return nil, err
	}
	cache := loadCache()
	cacheDatasetBytes, err := envPositiveInt64(envCacheDatasetBytes, defaultCacheDatasetBytes)
	if err != nil {
		return nil, err
	}
	cacheTotalBytes, err := envPositiveInt64(envCacheTotalBytes, defaultCacheTotalBytes)
	if err != nil {
		return nil, err
	}
	if cacheDatasetBytes > cacheTotalBytes {
		return nil, fmt.Errorf("config: %s=%d 非法(不得大于 %s=%d)",
			envCacheDatasetBytes, cacheDatasetBytes, envCacheTotalBytes, cacheTotalBytes)
	}

	return &Config{
		ObjstoreEndpoint:    endpoint,
		ObjstoreAccessKey:   envOr(envObjstoreAccessKey, ""),
		ObjstoreSecretKey:   envOr(envObjstoreSecretKey, ""),
		Bucket:              envOr(envObjstoreBucket, defaultObjstoreBucket),
		ObjstoreSecure:      envBool(envObjstoreSecure),
		ObjstoreCompression: envBool(envObjstoreCompression),
		Listen:              listen,
		BucketConcurrency:   concurrency,
		BlockBufferBytes:    bufferBytes,
		LogLevel:            logLevel(os.Getenv(envLogLevel)),
		Cache:               cache,
		CacheDatasetBytes:   cacheDatasetBytes,
		CacheTotalBytes:     cacheTotalBytes,
	}, nil
}

// loadListen 读取 TRAINCACHE_GATEWAY_LISTEN：host 允许为空（":9413"），端口必须合法。
func loadListen() (string, error) {
	v := envOr(envListen, defaultListen)
	if err := validateHostPort(envListen, v, true); err != nil {
		return "", err
	}
	return v, nil
}

// loadObjstoreEndpoint 读取源站地址：不带 scheme，host 必须非空。
func loadObjstoreEndpoint() (string, error) {
	v := envOr(envObjstoreEndpoint, defaultObjstoreEndpoint)
	if err := validateHostPort(envObjstoreEndpoint, v, false); err != nil {
		return "", err
	}
	return v, nil
}

// validateHostPort 用 net.SplitHostPort 校验 host:port 并检查端口范围 1..65535。
// allowEmptyHost 为 true 时接受 ":9413" 这类只给端口的监听地址。
func validateHostPort(key, v string, allowEmptyHost bool) error {
	host, port, err := net.SplitHostPort(v)
	if err != nil {
		return fmt.Errorf("config: %s=%q 非法(需 host:port): %w", key, v, err)
	}
	if !allowEmptyHost && strings.TrimSpace(host) == "" {
		return fmt.Errorf("config: %s=%q 非法(host 不能为空)", key, v)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("config: %s=%q 非法(端口需 1..65535)", key, v)
	}
	return nil
}

// logLevel 归一化日志级别：debug/info/warn/error（大小写不敏感），未知值退化为 info。
func logLevel(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	switch v {
	case "debug", "info", "warn", "error":
		return v
	}
	return defaultLogLevel
}

// envOr 返回环境变量值；未设置或全空白时返回默认值。
func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// envBool 解析布尔 env（design 9.1「配置」口径：TrimSpace 后 "1"/"true"/"yes"，大小写不敏感，为真；
// 未设置或其余取值一律为假，不报错）。
func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes":
		return true
	}
	return false
}

// envPositiveInt 解析正整数 env。
// 未设置或全空白 → 默认值；非法取值或 ≤0 → 错误。
func envPositiveInt(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q 非法(需整数): %w", key, v, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("config: %s=%d 非法(需 > 0)", key, n)
	}
	return n, nil
}

// envPositiveInt64 解析正整数 env（int64 口径，缓存配额用）。
// 未设置或全空白 → 默认值；非法取值或 ≤0 → 错误。
func envPositiveInt64(key string, def int64) (int64, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q 非法(需整数): %w", key, v, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("config: %s=%d 非法(需 > 0)", key, n)
	}
	return n, nil
}

// loadCache 读取 TRAINCACHE_GATEWAY_CACHE（D29：默认开，关闭要显式写 false）。
// 未设置或全空白 → true（未设置即开）；"1"/"true"/"yes" 开；其余取值一律当作关，
// 不报错——与 TRAINCACHE_OBJSTORE_SECURE 同一宽松口径。
func loadCache() bool {
	v := strings.TrimSpace(os.Getenv(envCache))
	if v == "" {
		return true
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes":
		return true
	}
	return false
}

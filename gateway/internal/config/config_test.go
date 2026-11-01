package config

import (
	"strings"
	"testing"
)

// envKeys 是本包读取的全部 env；用例开始前统一清空，隔离宿主环境。
var envKeys = []string{
	envObjstoreEndpoint,
	envObjstoreAccessKey,
	envObjstoreSecretKey,
	envObjstoreBucket,
	envObjstoreSecure,
	envListen,
	envBucketConcurrency,
	envBlockBufferBytes,
	envLogLevel,
}

// clearEnv 把全部相关 env 置空（解析 helper 把空串视同未设置）。
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range envKeys {
		t.Setenv(k, "")
	}
}

// defaultConfig 是 design 10.1 §5.3 的全部默认值。
func defaultConfig() Config {
	return Config{
		ObjstoreEndpoint:  "127.0.0.1:9000",
		ObjstoreAccessKey: "",
		ObjstoreSecretKey: "",
		Bucket:            "traincache",
		ObjstoreSecure:    false,
		Listen:            ":9413",
		BucketConcurrency: 32,
		BlockBufferBytes:  10485760,
		LogLevel:          "info",
	}
}

type loadCase struct {
	name    string
	env     map[string]string
	want    Config
	wantErr string // 非空时断言错误信息包含该片段
}

func TestLoad(t *testing.T) {
	override := defaultConfig()
	override.ObjstoreEndpoint = "minio.example:9443"
	override.ObjstoreAccessKey = "ak"
	override.ObjstoreSecretKey = "sk"
	override.Bucket = "bench"
	override.ObjstoreSecure = true
	override.Listen = "127.0.0.1:19413"
	override.BucketConcurrency = 64
	override.BlockBufferBytes = 20971520
	override.LogLevel = "debug"

	secureYes := defaultConfig()
	secureYes.ObjstoreSecure = true

	secureJunk := defaultConfig() // 非法布尔退化为 false，不报错

	tests := []loadCase{
		{
			name: "缺省",
			want: defaultConfig(),
		},
		{
			name: "空串视同未设置",
			env: map[string]string{
				envObjstoreEndpoint:  "",
				envObjstoreAccessKey: "",
				envObjstoreSecretKey: "",
				envObjstoreBucket:    "",
				envObjstoreSecure:    "",
				envListen:            "",
				envBucketConcurrency: "",
				envBlockBufferBytes:  "",
				envLogLevel:          "",
			},
			want: defaultConfig(),
		},
		{
			name: "完整覆盖",
			env: map[string]string{
				envObjstoreEndpoint:  "minio.example:9443",
				envObjstoreAccessKey: "ak",
				envObjstoreSecretKey: "sk",
				envObjstoreBucket:    "bench",
				envObjstoreSecure:    "1",
				envListen:            "127.0.0.1:19413",
				envBucketConcurrency: "64",
				envBlockBufferBytes:  "20971520",
				envLogLevel:          "debug",
			},
			want: override,
		},
		{
			name: "布尔 yes",
			env:  map[string]string{envObjstoreSecure: "yes"},
			want: secureYes,
		},
		{
			name: "非法布尔退化为 false",
			env:  map[string]string{envObjstoreSecure: "abc"},
			want: secureJunk,
		},
		{
			name:    "并发 0",
			env:     map[string]string{envBucketConcurrency: "0"},
			wantErr: "config: TRAINCACHE_GATEWAY_BUCKET_CONCURRENCY=0 非法(需 > 0)",
		},
		{
			name:    "并发 -1",
			env:     map[string]string{envBucketConcurrency: "-1"},
			wantErr: "TRAINCACHE_GATEWAY_BUCKET_CONCURRENCY=-1 非法(需 > 0)",
		},
		{
			name:    "并发 abc",
			env:     map[string]string{envBucketConcurrency: "abc"},
			wantErr: "TRAINCACHE_GATEWAY_BUCKET_CONCURRENCY=\"abc\" 非法(需整数)",
		},
		{
			name:    "缓冲 0",
			env:     map[string]string{envBlockBufferBytes: "0"},
			wantErr: "TRAINCACHE_GATEWAY_BLOCK_BUFFER_BYTES=0 非法(需 > 0)",
		},
		{
			name:    "缓冲 -1",
			env:     map[string]string{envBlockBufferBytes: "-1"},
			wantErr: "TRAINCACHE_GATEWAY_BLOCK_BUFFER_BYTES=-1 非法(需 > 0)",
		},
		{
			name:    "缓冲 abc",
			env:     map[string]string{envBlockBufferBytes: "abc"},
			wantErr: "TRAINCACHE_GATEWAY_BLOCK_BUFFER_BYTES=\"abc\" 非法(需整数)",
		},
		{
			name:    "监听缺端口",
			env:     map[string]string{envListen: "9413"},
			wantErr: "TRAINCACHE_GATEWAY_LISTEN=\"9413\" 非法(需 host:port)",
		},
		{
			name:    "监听端口 0",
			env:     map[string]string{envListen: "host:0"},
			wantErr: "TRAINCACHE_GATEWAY_LISTEN=\"host:0\" 非法(端口需 1..65535)",
		},
		{
			name:    "监听端口越界",
			env:     map[string]string{envListen: "host:70000"},
			wantErr: "TRAINCACHE_GATEWAY_LISTEN=\"host:70000\" 非法(端口需 1..65535)",
		},
		{
			name:    "源站地址缺端口",
			env:     map[string]string{envObjstoreEndpoint: "127.0.0.1"},
			wantErr: "TRAINCACHE_OBJSTORE_ENDPOINT=\"127.0.0.1\" 非法(需 host:port)",
		},
		{
			name:    "源站地址 host 为空",
			env:     map[string]string{envObjstoreEndpoint: ":9000"},
			wantErr: "TRAINCACHE_OBJSTORE_ENDPOINT=\":9000\" 非法(host 不能为空)",
		},
		{
			name: "日志级别未知退化为 info",
			env:  map[string]string{envLogLevel: "TRACE"},
			want: defaultConfig(),
		},
		{
			name: "日志级别大小写归一",
			env:  map[string]string{envLogLevel: "WARN"},
			want: func() Config {
				c := defaultConfig()
				c.LogLevel = "warn"
				return c
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			got, err := Load()
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("Load() = %+v, 期望错误 %q", *got, tc.wantErr)
				}
				if !strings.HasPrefix(err.Error(), "config: ") {
					t.Fatalf("错误缺 config 前缀: %v", err)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("错误 = %q, 期望包含 %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if *got != tc.want {
				t.Fatalf("Load() = %+v, want %+v", *got, tc.want)
			}
		})
	}
}

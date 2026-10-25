package logging

import (
	"strings"
	"testing"
)

// sentinel 是负向测试用的哨兵 Secret：任何输出里出现它都视为泄漏。
const sentinel = "S3CR3T-SENTINEL-9f3a2b"

func TestRedactSensitiveFragments(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "key=value 形态",
			in:   "accessKey=" + sentinel + " next=1",
			want: "accessKey=*** next=1",
		},
		{
			name: "key: value 形态",
			in:   "secretKey: " + sentinel,
			want: "secretKey: ***",
		},
		{
			name: "JSON 紧凑形态",
			in:   `{"accessKey":"` + sentinel + `","path":"a"}`,
			want: `{"accessKey":"***","path":"a"}`,
		},
		{
			name: "JSON 带空格形态",
			in:   `{"authorization": "Bearer ` + sentinel + `", "x":1}`,
			want: `{"authorization": "***", "x":1}`,
		},
		{
			name: "query 串形态",
			in:   "?token=" + sentinel + "&path=a",
			want: "?token=***&path=a",
		},
		{
			name: "键大小写不敏感",
			in:   "AccessKey=" + sentinel,
			want: "AccessKey=***",
		},
		{
			name: "x-amz 键",
			in:   "X-Amz-Signature=" + sentinel + "&X-Amz-Credential=" + sentinel,
			want: "X-Amz-Signature=***&X-Amz-Credential=***",
		},
		{
			name: "Authorization 头含空格值",
			in:   "authorization: Bearer " + sentinel,
			want: "authorization: ***",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Redact(tc.in)
			if strings.Contains(got, sentinel) {
				t.Fatalf("输出泄漏哨兵 Secret: %q", got)
			}
			if got != tc.want {
				t.Fatalf("Redact(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRedactNonSensitiveUnchanged(t *testing.T) {
	cases := []string{
		"path=/data/train/shard-00001.tar chunk=3 size=67108864",
		"user=bob count=42 status=ok",
		"my_accessKey=" + sentinel, // 键名不是 accessKey，不得按后缀误伤
		"expires_in=5",
		"",
	}
	for _, in := range cases {
		if got := Redact(in); got != in {
			t.Fatalf("非敏感文本被改动: Redact(%q) = %q", in, got)
		}
	}
}

func TestRedactURLCredentials(t *testing.T) {
	raw := "https://user:" + sentinel + "@storage.example.com:9000/bucket/path/to/obj" +
		"?X-Amz-Signature=" + sentinel + "&X-Amz-Expires=3600&prefix=a%2Fb#fragment"
	got := RedactURL(raw)

	if strings.Contains(got, sentinel) {
		t.Fatalf("输出泄漏哨兵 Secret: %q", got)
	}
	if strings.Contains(got, "user:") {
		t.Fatalf("userinfo 未被去掉: %q", got)
	}
	if strings.Contains(got, "fragment") {
		t.Fatalf("fragment 未被丢弃: %q", got)
	}
	for _, keep := range []string{
		"https://storage.example.com:9000",
		"/bucket/path/to/obj",
		"X-Amz-Signature=***",
		"X-Amz-Expires=3600",
		"prefix=a%2Fb",
	} {
		if !strings.Contains(got, keep) {
			t.Fatalf("输出缺少 %q: %q", keep, got)
		}
	}
}

func TestRedactURLSensitiveQueryKeys(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{
			raw:  "https://storage.example.com/obj?token=" + sentinel + "&prefix=a",
			want: "https://storage.example.com/obj?token=***&prefix=a",
		},
		{
			raw:  "https://storage.example.com/obj?TOKEN=" + sentinel,
			want: "https://storage.example.com/obj?TOKEN=***",
		},
		{
			raw:  "https://storage.example.com/obj?secret_key=" + sentinel + "&x=1",
			want: "https://storage.example.com/obj?secret_key=***&x=1",
		},
	}
	for _, tc := range cases {
		got := RedactURL(tc.raw)
		if strings.Contains(got, sentinel) {
			t.Fatalf("输出泄漏哨兵 Secret: %q", got)
		}
		if got != tc.want {
			t.Fatalf("RedactURL(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestRedactURLParseFailure(t *testing.T) {
	for _, raw := range []string{
		"http://[::1",
		"http://storage.example.com/%zz",
		"://missing-scheme",
	} {
		if got := RedactURL(raw); got != "***" {
			t.Fatalf("RedactURL(%q) = %q, want %q", raw, got, "***")
		}
	}
}

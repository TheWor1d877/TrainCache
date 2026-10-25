package svc

import (
	"strings"
	"testing"
)

// TestNewEvictPolicyFromEnvLegalNames 三档合法名都能造出对应策略;未设置时回落默认 heat。
func TestNewEvictPolicyFromEnvLegalNames(t *testing.T) {
	cases := []struct {
		env  string
		want string
	}{
		{"", "heat"}, // envOr 对空值回落默认
		{"heat", "heat"},
		{"lru", "lru"},
		{"lfu", "lfu"},
	}
	for _, c := range cases {
		t.Run("env="+c.env, func(t *testing.T) {
			t.Setenv(envEvictPolicy, c.env)
			p, err := newEvictPolicyFromEnv()
			if err != nil {
				t.Fatalf("策略 %q 应当合法,却报错: %v", c.env, err)
			}
			if p == nil {
				t.Fatalf("策略 %q 返回 nil 策略", c.env)
			}
			if got := p.Name(); got != c.want {
				t.Errorf("策略名 = %q, want %q", got, c.want)
			}
		})
	}
}

// TestNewEvictPolicyFromEnvRejectsUnknown 未知策略名必须拒绝启动并在错误里打印合法清单。
// 撤档后合法清单只剩 heat / lru / lfu 三档,wtinylfu 属于已撤档的名字,同样必须被拒。
func TestNewEvictPolicyFromEnvRejectsUnknown(t *testing.T) {
	for _, name := range []string{"wtinylfu", "tinylfu", "bogus", "HEAT"} {
		t.Run("env="+name, func(t *testing.T) {
			t.Setenv(envEvictPolicy, name)
			p, err := newEvictPolicyFromEnv()
			if err == nil {
				t.Fatalf("策略 %q 非法,应当报错,却造出了 %v", name, p)
			}
			if p != nil {
				t.Errorf("策略 %q 非法,应当返回 nil 策略,却拿到 %v", name, p)
			}
			msg := err.Error()
			for _, legal := range []string{"heat", "lru", "lfu"} {
				if !strings.Contains(msg, legal) {
					t.Errorf("错误信息 %q 未列出合法策略 %q", msg, legal)
				}
			}
			if !strings.Contains(msg, name) {
				t.Errorf("错误信息 %q 未回显被拒绝的策略名 %q", msg, name)
			}
		})
	}
}

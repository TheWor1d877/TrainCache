package main

import (
	"os"
	"testing"
)

// TestAgentLogLevel 覆盖 TRAINCACHE_LOG_LEVEL 的默认、显式与未知取值。
func TestAgentLogLevel(t *testing.T) {
	cases := []struct {
		name  string
		set   bool
		value string
		want  string
	}{
		{name: "unset defaults to info", want: "info"},
		{name: "debug", set: true, value: "DEBUG", want: "debug"},
		{name: "padded warn", set: true, value: " warn ", want: "warn"},
		{name: "unknown degrades to info", set: true, value: "verbose", want: "info"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			old, had := os.LookupEnv("TRAINCACHE_LOG_LEVEL")
			if err := os.Unsetenv("TRAINCACHE_LOG_LEVEL"); err != nil {
				t.Fatalf("unset: %v", err)
			}
			t.Cleanup(func() {
				if had {
					_ = os.Setenv("TRAINCACHE_LOG_LEVEL", old)
				} else {
					_ = os.Unsetenv("TRAINCACHE_LOG_LEVEL")
				}
			})
			if tc.set {
				t.Setenv("TRAINCACHE_LOG_LEVEL", tc.value)
			}
			if got := agentLogLevel(); got != tc.want {
				t.Fatalf("agentLogLevel() = %q, want %q", got, tc.want)
			}
		})
	}
}

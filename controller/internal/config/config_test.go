package config

import (
	"strings"
	"testing"
	"time"
)

func lookupFrom(env map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(lookupFrom(nil))
	if err != nil {
		t.Fatalf("load() error = %v", err)
	}

	want := Config{
		MetricsAddr:           ":9412",
		HealthAddr:            ":8081",
		WebhookAddr:           ":9443",
		Namespace:             "",
		AgentPort:             9410,
		AgentTimeout:          5 * time.Second,
		PollInterval:          2 * time.Second,
		ScoreTTL:              5 * time.Minute,
		SchedulerScoreTTL:     5 * time.Minute,
		ScoreAnnotationPrefix: "traincache.io/scores",
		GateName:              "traincache.io/warmup",
		LeaderElection:        false,
		MaxChunks:             10_000_000,
		LogLevel:              "info",
	}
	if *cfg != want {
		t.Fatalf("defaults = %#v, want %#v", *cfg, want)
	}
}

func TestLoadCustomValues(t *testing.T) {
	cfg, err := load(lookupFrom(map[string]string{
		EnvMetricsAddr:       "127.0.0.1:19412",
		EnvHealthAddr:        ":18081",
		EnvWebhookAddr:       ":19443",
		EnvNamespace:         "train-system",
		EnvAgentPort:         "19410",
		EnvAgentTimeout:      "250ms",
		EnvPollInterval:      "750ms",
		EnvScoreTTL:          "90s",
		EnvSchedulerScoreTTL: "45s",
		EnvScoreAnnotation:   "example.io/scores",
		EnvGateName:          "example.io/warmup",
		EnvLeaderElection:    "true",
		EnvMaxChunks:         "42",
		EnvLogLevel:          "DEBUG",
	}))
	if err != nil {
		t.Fatalf("load() error = %v", err)
	}
	if cfg.MetricsAddr != "127.0.0.1:19412" || cfg.AgentPort != 19410 {
		t.Fatalf("addr/port not parsed: %#v", cfg)
	}
	if cfg.AgentTimeout != 250*time.Millisecond || cfg.PollInterval != 750*time.Millisecond {
		t.Fatalf("durations not parsed: %#v", cfg)
	}
	if !cfg.LeaderElection || cfg.MaxChunks != 42 {
		t.Fatalf("bool/int not parsed: %#v", cfg)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("LogLevel = %q, want debug", cfg.LogLevel)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"bad metrics addr", map[string]string{EnvMetricsAddr: "9412"}, EnvMetricsAddr},
		{"bad port", map[string]string{EnvMetricsAddr: ":99999"}, EnvMetricsAddr},
		{"agent port zero", map[string]string{EnvAgentPort: "0"}, EnvAgentPort},
		{"agent port text", map[string]string{EnvAgentPort: "nine"}, EnvAgentPort},
		{"duration without unit", map[string]string{EnvAgentTimeout: "5"}, EnvAgentTimeout},
		{"zero duration", map[string]string{EnvPollInterval: "0s"}, EnvPollInterval},
		{"bad bool", map[string]string{EnvLeaderElection: "yes"}, EnvLeaderElection},
		{"bad namespace", map[string]string{EnvNamespace: "Bad_NS"}, EnvNamespace},
		{"empty annotation prefix", map[string]string{EnvScoreAnnotation: ""}, EnvScoreAnnotation},
		{"annotation prefix trailing hyphen", map[string]string{EnvScoreAnnotation: "traincache.io/scores-"}, EnvScoreAnnotation},
		{"annotation key too long", map[string]string{EnvScoreAnnotation: "example.io/a-very-long-prefix-name"}, EnvScoreAnnotation},
		{"bad log level", map[string]string{EnvLogLevel: "verbose"}, EnvLogLevel},
		{"zero max chunks", map[string]string{EnvMaxChunks: "0"}, EnvMaxChunks},
		{"zero scheduler ttl", map[string]string{EnvSchedulerScoreTTL: "0s"}, EnvSchedulerScoreTTL},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(lookupFrom(tc.env))
			if err == nil {
				t.Fatalf("load() succeeded, want error mentioning %s", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to mention %s", err, tc.want)
			}
		})
	}
}

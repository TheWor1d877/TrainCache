// Package config loads controller/webhook/scheduler settings from the
// environment. Every invalid value is a startup error; nothing silently falls
// back to a default after a value was explicitly set.
package config

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Environment variable names. See docs/8.1 design.md §8.
const (
	EnvMetricsAddr       = "CONTROLLER_METRICS_ADDR"
	EnvHealthAddr        = "CONTROLLER_HEALTH_ADDR"
	EnvWebhookAddr       = "CONTROLLER_WEBHOOK_ADDR"
	EnvNamespace         = "CONTROLLER_NAMESPACE"
	EnvAgentPort         = "CONTROLLER_AGENT_PORT"
	EnvAgentTimeout      = "CONTROLLER_AGENT_TIMEOUT"
	EnvPollInterval      = "CONTROLLER_POLL_INTERVAL"
	EnvScoreTTL          = "CONTROLLER_SCORE_TTL"
	EnvSchedulerScoreTTL = "SCHEDULER_SCORE_TTL"
	EnvScoreAnnotation   = "CONTROLLER_SCORE_ANNOTATION"
	EnvGateName          = "CONTROLLER_GATE_NAME"
	EnvLeaderElection    = "CONTROLLER_LEADER_ELECTION"
	EnvMaxChunks         = "CONTROLLER_MAX_CHUNKS"
	EnvLogLevel          = "CONTROLLER_LOG_LEVEL"
)

// Config is the shared control-plane configuration.
type Config struct {
	MetricsAddr           string
	HealthAddr            string
	WebhookAddr           string
	Namespace             string
	AgentPort             int
	AgentTimeout          time.Duration
	PollInterval          time.Duration
	ScoreTTL              time.Duration
	SchedulerScoreTTL     time.Duration
	ScoreAnnotationPrefix string
	GateName              string
	LeaderElection        bool
	MaxChunks             int64
	LogLevel              string
}

// Load reads the process environment.
func Load() (*Config, error) {
	return load(os.LookupEnv)
}

func load(lookup func(string) (string, bool)) (*Config, error) {
	cfg := &Config{
		MetricsAddr:           env(lookup, EnvMetricsAddr, ":9412"),
		HealthAddr:            env(lookup, EnvHealthAddr, ":8081"),
		WebhookAddr:           env(lookup, EnvWebhookAddr, ":9443"),
		Namespace:             env(lookup, EnvNamespace, ""),
		ScoreAnnotationPrefix: env(lookup, EnvScoreAnnotation, "traincache.io/scores"),
		GateName:              env(lookup, EnvGateName, "traincache.io/warmup"),
		LogLevel:              strings.ToLower(env(lookup, EnvLogLevel, "info")),
	}

	var err error
	if cfg.AgentPort, err = envInt(lookup, EnvAgentPort, 9410); err != nil {
		return nil, err
	}
	if cfg.AgentTimeout, err = envDuration(lookup, EnvAgentTimeout, 5*time.Second); err != nil {
		return nil, err
	}
	if cfg.PollInterval, err = envDuration(lookup, EnvPollInterval, 2*time.Second); err != nil {
		return nil, err
	}
	if cfg.ScoreTTL, err = envDuration(lookup, EnvScoreTTL, 5*time.Minute); err != nil {
		return nil, err
	}
	if cfg.SchedulerScoreTTL, err = envDuration(lookup, EnvSchedulerScoreTTL, 5*time.Minute); err != nil {
		return nil, err
	}
	if cfg.LeaderElection, err = envBool(lookup, EnvLeaderElection, false); err != nil {
		return nil, err
	}
	if cfg.MaxChunks, err = envInt64(lookup, EnvMaxChunks, 10_000_000); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	for name, addr := range map[string]string{
		EnvMetricsAddr: c.MetricsAddr,
		EnvHealthAddr:  c.HealthAddr,
		EnvWebhookAddr: c.WebhookAddr,
	} {
		if err := validateAddr(addr); err != nil {
			return fmt.Errorf("config: %s: %w", name, err)
		}
	}

	if c.AgentPort < 1 || c.AgentPort > 65535 {
		return fmt.Errorf("config: %s: port %d out of range 1..65535", EnvAgentPort, c.AgentPort)
	}
	if c.AgentTimeout <= 0 {
		return fmt.Errorf("config: %s: must be positive", EnvAgentTimeout)
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("config: %s: must be positive", EnvPollInterval)
	}
	if c.ScoreTTL <= 0 {
		return fmt.Errorf("config: %s: must be positive", EnvScoreTTL)
	}
	if c.SchedulerScoreTTL <= 0 {
		return fmt.Errorf("config: %s: must be positive", EnvSchedulerScoreTTL)
	}
	if c.MaxChunks <= 0 {
		return fmt.Errorf("config: %s: must be positive", EnvMaxChunks)
	}

	if c.Namespace != "" && !validNamespace(c.Namespace) {
		return fmt.Errorf("config: %s: %q is not a valid namespace", EnvNamespace, c.Namespace)
	}
	if c.ScoreAnnotationPrefix == "" {
		return fmt.Errorf("config: %s: must not be empty", EnvScoreAnnotation)
	}
	if strings.TrimSpace(c.ScoreAnnotationPrefix) != c.ScoreAnnotationPrefix ||
		strings.HasSuffix(c.ScoreAnnotationPrefix, "-") ||
		strings.HasSuffix(c.ScoreAnnotationPrefix, ".") {
		return fmt.Errorf("config: %s: %q must not contain surrounding whitespace or end with '-'/'.'", EnvScoreAnnotation, c.ScoreAnnotationPrefix)
	}
	// The generated annotation key is
	// <prefix>-<namePrefix32>-<hash12>; ensure the part after '/' fits the
	// Kubernetes 63-character key-name limit even at maximum truncation.
	namePart := c.ScoreAnnotationPrefix[strings.LastIndex(c.ScoreAnnotationPrefix, "/")+1:]
	if namePart == "" || len(namePart)+46 > 63 {
		return fmt.Errorf("config: %s: %q leaves no room for a 63-character annotation key", EnvScoreAnnotation, c.ScoreAnnotationPrefix)
	}
	if c.GateName == "" {
		return fmt.Errorf("config: %s: must not be empty", EnvGateName)
	}

	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("config: %s: %q is not one of debug/info/warn/error", EnvLogLevel, c.LogLevel)
	}
	return nil
}

func validateAddr(addr string) error {
	if addr == "" {
		return fmt.Errorf("address must not be empty")
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%q: %w", addr, err)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("%q: port %q out of range 1..65535", addr, port)
	}
	return nil
}

var namespaceRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func validNamespace(ns string) bool {
	return len(ns) <= 63 && namespaceRE.MatchString(ns)
}

func env(lookup func(string) (string, bool), key, def string) string {
	if v, ok := lookup(key); ok {
		return v
	}
	return def
}

func envInt(lookup func(string) (string, bool), key string, def int) (int, error) {
	raw := env(lookup, key, strconv.Itoa(def))
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q: %w", key, raw, err)
	}
	return v, nil
}

func envInt64(lookup func(string) (string, bool), key string, def int64) (int64, error) {
	raw := env(lookup, key, strconv.FormatInt(def, 10))
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q: %w", key, raw, err)
	}
	return v, nil
}

func envDuration(lookup func(string) (string, bool), key string, def time.Duration) (time.Duration, error) {
	raw := env(lookup, key, def.String())
	v, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q: %w", key, raw, err)
	}
	return v, nil
}

func envBool(lookup func(string) (string, bool), key string, def bool) (bool, error) {
	raw := env(lookup, key, strconv.FormatBool(def))
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("config: %s=%q: %w", key, raw, err)
	}
	return v, nil
}

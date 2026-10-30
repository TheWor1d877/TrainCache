// Package scheduler implements the TrainCache kube-scheduler score plugin.
//
// The plugin only reads the score annotation written by the controller on the
// training Pod itself. It never mutates Nodes, Pods, or any other API object.
// Every failure mode returns a score of 0 with a nil status so a malformed
// annotation can never make a Pod unschedulable (docs/7.1 design.md §5.8/D18).
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	schedframework "k8s.io/kube-scheduler/framework"

	"traincache/controller/internal/metrics"
	"traincache/controller/internal/scorefmt"
)

const (
	// Name is the plugin name used in KubeSchedulerConfiguration.
	Name = "TrainCacheScore"

	// AnnotationWarmupTask is injected by the mutating webhook and holds
	// "<namespace>/<name>" (docs/7.1 design.md §6.3).
	AnnotationWarmupTask = "traincache.io/warmup-task"

	defaultTTL = 5 * time.Minute
)

// Options configures the plugin. Zero values fall back to the defaults that
// match docs/7.1 design.md §8.
type Options struct {
	// Annotation is the Pod score annotation key (CONTROLLER_SCORE_ANNOTATION).
	Annotation string
	// TTL is the maximum age of a score entry (SCHEDULER_SCORE_TTL).
	TTL time.Duration
	// Logger receives Debug score decisions and Warn parse failures.
	Logger *slog.Logger
	// Now is replaceable in tests.
	Now func() time.Time
	// Metrics is optional. Scheduler metrics live in this process's own
	// independent registry, never the controller-manager one.
	Metrics *metrics.Metrics
}

// Plugin implements framework.ScorePlugin.
type Plugin struct {
	annotation string
	ttl        time.Duration
	log        *slog.Logger
	now        func() time.Time
	metrics    *metrics.Metrics
}

var (
	_ schedframework.ScorePlugin     = (*Plugin)(nil)
	_ schedframework.ScoreExtensions = (*Plugin)(nil)
)

// New is the registry factory with default options. It exists so the plugin
// can be registered with a stock signature; cmd/scheduler uses Factory to
// inject the environment configuration.
func New(_ context.Context, _ runtime.Object, _ schedframework.Handle) (schedframework.Plugin, error) {
	return NewWithOptions(Options{})
}

// NewWithOptions builds the plugin and validates its options.
func NewWithOptions(opts Options) (*Plugin, error) {
	if opts.Annotation == "" {
		opts.Annotation = scorefmt.DefaultAnnotation
	}
	if opts.TTL == 0 {
		opts.TTL = defaultTTL
	}
	if opts.TTL < 0 {
		return nil, fmt.Errorf("scheduler: TTL must be positive, got %s", opts.TTL)
	}
	if opts.Annotation != strings.TrimSpace(opts.Annotation) ||
		strings.HasSuffix(opts.Annotation, "-") || strings.HasSuffix(opts.Annotation, ".") {
		return nil, fmt.Errorf("scheduler: invalid annotation key %q", opts.Annotation)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Plugin{
		annotation: opts.Annotation,
		ttl:        opts.TTL,
		log:        opts.Logger,
		now:        opts.Now,
		metrics:    opts.Metrics,
	}, nil
}

// Factory returns a registry factory that is assignable to the scheduler's
// runtime.PluginFactory. cmd/scheduler passes environment-derived options
// here so the plugin and controller share one annotation key and TTL.
func Factory(opts Options) func(context.Context, runtime.Object, schedframework.Handle) (schedframework.Plugin, error) {
	return func(_ context.Context, _ runtime.Object, _ schedframework.Handle) (schedframework.Plugin, error) {
		return NewWithOptions(opts)
	}
}

// Name returns the plugin name.
func (p *Plugin) Name() string { return Name }

// Score returns the score the controller wrote on this Pod for this Node.
//
// A missing annotation, a mismatched task, an invalid generation, malformed
// JSON, an expired value, and a missing node entry all return 0 with a nil
// status. Any error is logged but never fails scheduling.
func (p *Plugin) Score(_ context.Context, _ schedframework.CycleState, pod *corev1.Pod, nodeInfo schedframework.NodeInfo) (int64, *schedframework.Status) {
	if pod == nil || nodeInfo == nil {
		return 0, nil
	}
	node := nodeInfo.Node()
	if node == nil {
		return 0, nil
	}

	return p.scoreNode(pod, node, p.now()), nil
}

// scoreNode evaluates and emits the §9.1 log points: Debug for every scoring
// decision and Warn for malformed annotations.
func (p *Plugin) scoreNode(pod *corev1.Pod, node *corev1.Node, now time.Time) int64 {
	score, key, stale, result, err := p.evaluateResult(pod, node, now)
	p.metrics.ObserveSchedulerScore(result)
	podKey := pod.Namespace + "/" + pod.Name
	if err != nil {
		p.log.Warn("scheduler score rejected",
			"pod", podKey,
			"node", node.Name,
			"annotation_key", key,
			"err", err,
		)
	}
	p.log.Debug("scheduler score",
		"pod", podKey,
		"node", node.Name,
		"annotation_key", key,
		"score", score,
		"stale", stale,
	)
	return score
}

// ScoreExtensions implements framework.ScorePlugin.
func (p *Plugin) ScoreExtensions() schedframework.ScoreExtensions { return p }

// NormalizeScore scales the raw scores to 0..100 relative to the maximum.
// An all-zero list stays all zero; negative values (which Score never emits)
// are clamped to zero.
func (p *Plugin) NormalizeScore(_ context.Context, _ schedframework.CycleState, _ *corev1.Pod, scores schedframework.NodeScoreList) *schedframework.Status {
	var max int64
	for _, s := range scores {
		if s.Score > max {
			max = s.Score
		}
	}
	if max <= 0 {
		for i := range scores {
			scores[i].Score = 0
		}
		return nil
	}
	for i := range scores {
		s := scores[i].Score
		if s <= 0 {
			scores[i].Score = 0
			continue
		}
		// Integer rounding: floor((s*100 + max/2) / max).
		normalized := (s*100 + max/2) / max
		if normalized > 100 {
			normalized = 100
		}
		scores[i].Score = normalized
	}
	return nil
}

// evaluate reads the controller-written annotation for the Pod's task and
// pod index. stale is true whenever the entry must not influence scheduling.
// err is non-nil only for malformed inputs, which are worth a Warn log;
// legitimate absences (no annotation, no entry, expired TTL) are silent.
func (p *Plugin) evaluate(pod *corev1.Pod, node *corev1.Node, now time.Time) (score int64, key string, stale bool, err error) {
	score, key, stale, _, err = p.evaluateResult(pod, node, now)
	return score, key, stale, err
}

// evaluateResult is evaluate plus the fixed §9.5 result enum used by the
// scheduler_score_total metric. Every return path sets exactly one branch.
func (p *Plugin) evaluateResult(pod *corev1.Pod, node *corev1.Node, now time.Time) (score int64, key string, stale bool, result metrics.ScoreResult, err error) {
	if pod == nil || node == nil {
		return 0, "", true, metrics.ScoreNoTaskRef, nil
	}

	key = p.annotation
	taskRef := pod.Annotations[AnnotationWarmupTask]
	if _, _, ok := splitTaskRef(taskRef); !ok {
		if taskRef == "" {
			return 0, key, true, metrics.ScoreNoTaskRef, nil
		}
		return 0, key, true, metrics.ScoreInvalidTaskRef, fmt.Errorf("invalid %s annotation %q", AnnotationWarmupTask, taskRef)
	}

	raw, ok := pod.Annotations[key]
	if !ok {
		return 0, key, true, metrics.ScoreNoAnnotation, nil
	}
	value, err := scorefmt.DecodeValue(raw)
	if err != nil {
		return 0, key, true, metrics.ScoreInvalidValue, err
	}
	if value.Task != taskRef {
		return 0, key, true, metrics.ScoreTaskMismatch, fmt.Errorf("annotation task %q does not match pod task %q", value.Task, taskRef)
	}
	if value.G < 1 {
		return 0, key, true, metrics.ScoreStaleGeneration, fmt.Errorf("stale generation %d", value.G)
	}
	if !scorefmt.Fresh(value, now, p.ttl) {
		return 0, key, true, metrics.ScoreExpired, nil
	}

	nodeScore, ok := value.S[node.Name]
	if !ok {
		return 0, key, true, metrics.ScoreNoNodeEntry, nil
	}
	return int64(nodeScore), key, false, metrics.ScoreOK, nil
}

// splitTaskRef splits the webhook's "<namespace>/<name>" annotation. Exactly
// one separator is required; empty components are rejected.
func splitTaskRef(ref string) (namespace, name string, ok bool) {
	namespace, name, found := strings.Cut(ref, "/")
	if !found || namespace == "" || name == "" || strings.Contains(name, "/") {
		return "", "", false
	}
	return namespace, name, true
}

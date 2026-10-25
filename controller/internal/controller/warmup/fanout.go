package warmup

// M14 controller-side fanout helpers. Bound normalization lives in status.go;
// this file formats the adaptive decision fields for logs and attributes the
// reason a previously submitted (Pod, node) pair was pruned.

import (
	"fmt"
	"sort"
	"strings"

	"traincache/controller/internal/gwaa"
)

// fanoutSummary flattens the per-Pod M14 decisions into the values attached to
// placement log records.
type fanoutSummary struct {
	Target              string
	Actual              string
	LowSignal           bool
	ForcedByMin         bool
	MinUnmet            bool
	FallbackOutOfTarget bool
	PruneReason         string
}

// summarizeFanout folds per-Pod decisions into record-level fields. The flags
// are existential: true means at least one Pod hit that condition, which keeps
// the field a plain boolean while the per-Pod target/actual counts stay
// readable strings.
func summarizeFanout(placement gwaa.Output, actions []PlacementAction) fanoutSummary {
	summary := fanoutSummary{
		Target:      formatTargetCounts(placement.TargetSet),
		Actual:      formatWarmNodeCounts(placement.Assignment),
		PruneReason: pruneReasons(placement, actions),
	}
	for _, decision := range placement.Decisions {
		summary.LowSignal = summary.LowSignal || decision.LowSignal
		summary.ForcedByMin = summary.ForcedByMin || decision.ForcedByMin
		summary.MinUnmet = summary.MinUnmet || decision.MinUnmet
		summary.FallbackOutOfTarget = summary.FallbackOutOfTarget || decision.FallbackOutOfTarget
	}
	return summary
}

// fields returns the M14 fields shared by every placement log record. The
// caller adds fanout_min/fanout_max because they come from the spec.
func (s fanoutSummary) fields() []any {
	return []any{
		"fanout_target", s.Target,
		"fanout_actual", s.Actual,
		"low_signal", s.LowSignal,
		"forced_by_min", s.ForcedByMin,
		"min_unmet", s.MinUnmet,
		"fallback_out_of_target", s.FallbackOutOfTarget,
		"prune_reason", s.PruneReason,
	}
}

// formatTargetCounts renders the target set size per Pod, e.g. "p0=2,p1=1".
func formatTargetCounts(targets map[int32][]string) string {
	if len(targets) == 0 {
		return "none"
	}
	pods := make([]int32, 0, len(targets))
	for index := range targets {
		pods = append(pods, index)
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i] < pods[j] })
	parts := make([]string, 0, len(pods))
	for _, index := range pods {
		parts = append(parts, fmt.Sprintf("p%d=%d", index, len(targets[index])))
	}
	return strings.Join(parts, ",")
}

// pruneReason explains why a previously submitted pair is no longer desired.
// A node still in the target set was dropped by conflict adjudication or
// top-up (target_shrink); any other node left the target set (out_of_target).
func pruneReason(placement gwaa.Output, pod int32, node string) string {
	for _, target := range placement.TargetSet[pod] {
		if target == node {
			return "target_shrink"
		}
	}
	return "out_of_target"
}

// pruneReasons lists the distinct prune reasons across cancel actions, sorted
// and joined with "+". "none" means this pass issued no Cancel.
func pruneReasons(placement gwaa.Output, actions []PlacementAction) string {
	reasons := make(map[string]struct{})
	for _, action := range actions {
		if action.Kind != ActionCancel {
			continue
		}
		reasons[pruneReason(placement, action.PodIndex, action.Node)] = struct{}{}
	}
	if len(reasons) == 0 {
		return "none"
	}
	out := make([]string, 0, len(reasons))
	for reason := range reasons {
		out = append(out, reason)
	}
	sort.Strings(out)
	return strings.Join(out, "+")
}

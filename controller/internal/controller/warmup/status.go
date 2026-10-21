package warmup

import (
	"math"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// requiredReady is the integer gate threshold from docs/8.1 §5.6:
// floor(threshold*K) with a floor of 1 when threshold > 0, capped at K.
// M5 persists it per Pod so M7 only compares integers.
func requiredReady(threshold float64, k int32) int32 {
	if k <= 0 || threshold <= 0 {
		return 0
	}
	required := int32(math.Floor(threshold * float64(k)))
	if required == 0 {
		required = 1
	}
	if required > k {
		required = k
	}
	return required
}

// preheatState maps the Agent status machine onto the CRD PreheatState. A
// completed_block count that reaches total_blocks is treated as Ready even when
// the Agent leaves State at RUNNING.
func preheatState(resp *agentpb.PreheatStatus) traincachev1alpha1.PreheatState {
	switch resp.GetState() {
	case agentpb.PreheatStatus_ERROR:
		return traincachev1alpha1.PreheatStateFailed
	case agentpb.PreheatStatus_DONE:
		return traincachev1alpha1.PreheatStateReady
	}
	if resp.GetFailedBlocks() > 0 {
		return traincachev1alpha1.PreheatStateFailed
	}
	if resp.GetTotalBlocks() > 0 && resp.GetCompletedBlocks() >= resp.GetTotalBlocks() {
		return traincachev1alpha1.PreheatStateReady
	}
	return traincachev1alpha1.PreheatStatePreheating
}

// reportTime prefers the Agent's UpdatedAt and falls back to the injected
// local clock when the Agent omitted it (docs/8.1 §7.3 step 4).
func reportTime(resp *agentpb.PreheatStatus, now time.Time) *metav1.Time {
	if ts := resp.GetUpdatedAt(); ts != nil && ts.CheckValid() == nil {
		updated := metav1.NewTime(ts.AsTime().UTC())
		return &updated
	}
	local := metav1.NewTime(now.UTC())
	return &local
}

// readinessKey identifies one (Pod, candidate node) readiness fact. The
// 8.0 baseline stores one PodNodeReadiness per mapping, so the node name is
// part of the identity.
type readinessKey struct {
	podIndex int32
	node     string
}

// windowReadyRate returns ready/K_i for observation only; K is validated >= 1.
func windowReadyRate(ready, k int32) float64 {
	if k <= 0 {
		return 0
	}
	return float64(ready) / float64(k)
}

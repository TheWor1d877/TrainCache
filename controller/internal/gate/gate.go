// Package gate implements the three-condition WarmupTask gate from
// docs/7.1 design.md §5.6, §7.5 and docs/7.2 D14/D15/D26. It is a pure package:
// it reads only its Input and returns a Decision. Rate/Gap/Slope are
// observations for logging and the marginal-gain condition; condition ① is an
// existential quantifier over the Pod's preheat node set and compares each
// node's integer ready count against the integer required count.
package gate

import (
	"math"
	"time"
)

// GateState is the frozen gate verdict.
type GateState string

const (
	Hold     GateState = "Hold"
	Released GateState = "Released"
	Degraded GateState = "Degraded"
)

// Reasons returned in Decision.Reason. They are stable identifiers used in
// status and logs.
const (
	ReasonTMaxExceeded = "tmax_exceeded"
	ReasonAllPodsReady = "all_pods_ready"
	ReasonMarginalGain = "marginal_gain_insufficient"
	ReasonHold         = "hold"
)

// GatePolicy is the controller-independent form of spec.gate.
type GatePolicy struct {
	// Threshold is the required ready fraction in [0,1].
	Threshold float64
	// GapMin is the marginal-gain gap threshold.
	GapMin float64
	// SlopeMin is the marginal-gain slope threshold (fraction per second).
	SlopeMin float64
	// TMax is the absolute timeout. A non-positive value disables condition ③.
	TMax time.Duration
	// MinInterval is the minimum spacing between slope samples and the minimum
	// age of FirstReadyAt before condition ② may trigger.
	MinInterval time.Duration
}

// PodGateState is one Pod's gate input over its preheat node set.
type PodGateState struct {
	Index int32
	// AssignedNodes is the GWAA preheat node set for this Pod. An empty set
	// means condition ① cannot be satisfied; a non-empty set is satisfied as
	// soon as at least one node reaches RequiredReady.
	AssignedNodes []string
	// ReadyByNode is the number of validated, persisted blocks in the startup
	// window on each preheat node. A missing entry is treated as 0.
	ReadyByNode map[string]int32
	K           int32
	// Rate is the caller-observed maximum ReadyByNode[node]/K over
	// AssignedNodes, kept for observation parity. Evaluate derives the same
	// value from AssignedNodes/ReadyByNode when the set is non-empty; it falls
	// back to this field only when no node set is supplied.
	Rate float64
	// PrevRate and PrevAt describe the previous rate sample. FirstSampleSeen
	// reports that the caller has at least one sample; a slope is only valid
	// when FirstSampleSeen, PrevAt is non-zero and Now-PrevAt >= MinInterval.
	PrevRate        float64
	PrevAt          time.Time
	FirstSampleSeen bool
}

// Input is the frozen gate input.
type Input struct {
	Now          time.Time
	Policy       GatePolicy
	Pods         []PodGateState
	FirstReadyAt time.Time
	StartedAt    time.Time
}

// PodRate is one Pod's observation at decision time.
type PodRate struct {
	PodIndex   int32
	Ready      int32
	Required   int32
	Rate       float64
	Gap        float64
	Slope      float64
	SlopeValid bool
}

// Decision is the gate verdict.
type Decision struct {
	State  GateState
	Reason string
	Rates  []PodRate
}

// Evaluate applies the fixed order ③ TMax → ① all Pods ready → ②
// insufficient marginal gain → Hold (docs/7.1 §5.6).
func Evaluate(in Input) Decision {
	rates := make([]PodRate, len(in.Pods))
	for i := range in.Pods {
		pod := in.Pods[i]
		required := RequiredReady(in.Policy.Threshold, pod.K)
		rate := podRate(pod)
		slope, valid := slopeOf(in, pod, rate)
		rates[i] = PodRate{
			PodIndex:   pod.Index,
			Ready:      podBestReady(pod),
			Required:   required,
			Rate:       rate,
			Gap:        in.Policy.Threshold - rate,
			Slope:      slope,
			SlopeValid: valid,
		}
	}
	decision := Decision{Rates: rates}

	// ③ TMax is the absolute upper bound and wins over every other condition.
	if in.Policy.TMax > 0 && !in.StartedAt.IsZero() &&
		!in.Now.Before(in.StartedAt.Add(in.Policy.TMax)) {
		decision.State = Degraded
		decision.Reason = ReasonTMaxExceeded
		return decision
	}

	// ① Every Pod must reach its integer threshold on at least one preheat
	// node (existential quantifier, docs/7.1 §5.6, §7.5).
	allReady := len(in.Pods) > 0
	for i := range in.Pods {
		if !podMeetsThreshold(in.Pods[i], rates[i].Required) {
			allReady = false
			break
		}
	}
	if allReady {
		decision.State = Released
		decision.Reason = ReasonAllPodsReady
		return decision
	}

	// ② Marginal gain: after the first Pod reaches its threshold, every Pod
	// that is still below threshold must show a large remaining gap and a
	// slope that has flattened. Missing samples make the condition false.
	eligible := len(in.Pods) > 0 && !in.FirstReadyAt.IsZero() &&
		in.Now.Sub(in.FirstReadyAt) >= nonNegative(in.Policy.MinInterval)
	if eligible {
		for i := range in.Pods {
			if podMeetsThreshold(in.Pods[i], rates[i].Required) {
				continue
			}
			if !rates[i].SlopeValid ||
				rates[i].Gap <= in.Policy.GapMin ||
				rates[i].Slope >= in.Policy.SlopeMin {
				eligible = false
				break
			}
		}
	}
	if eligible {
		decision.State = Released
		decision.Reason = ReasonMarginalGain
		return decision
	}

	decision.State = Hold
	decision.Reason = ReasonHold
	return decision
}

// RequiredReady is floor(threshold*K), raised to 1 when threshold > 0 and the
// floor is 0, and clamped to [0,K]. Threshold 0 yields 0 regardless of K
// (docs/7.1 §5.6, D26).
func RequiredReady(threshold float64, k int32) int32 {
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

// podMeetsThreshold reports whether at least one node in the Pod's preheat set
// has reached required. An empty set never satisfies condition ①.
func podMeetsThreshold(pod PodGateState, required int32) bool {
	for _, node := range pod.AssignedNodes {
		if pod.ReadyByNode[node] >= required {
			return true
		}
	}
	return false
}

// podBestReady is the largest ready-block count over the preheat set, used for
// observation in PodRate.Ready.
func podBestReady(pod PodGateState) int32 {
	var best int32
	for _, node := range pod.AssignedNodes {
		if ready := pod.ReadyByNode[node]; ready > best {
			best = ready
		}
	}
	return best
}

// podRate is the maximum ReadyByNode[node]/K over the preheat set. It falls
// back to the caller-observed Rate only when no node set is supplied.
func podRate(pod PodGateState) float64 {
	if len(pod.AssignedNodes) == 0 {
		return pod.Rate
	}
	if pod.K <= 0 {
		return 0
	}
	best := 0.0
	for _, node := range pod.AssignedNodes {
		if rate := float64(pod.ReadyByNode[node]) / float64(pod.K); rate > best {
			best = rate
		}
	}
	return best
}

func slopeOf(in Input, pod PodGateState, rate float64) (float64, bool) {
	if !pod.FirstSampleSeen || pod.PrevAt.IsZero() {
		return 0, false
	}
	elapsed := in.Now.Sub(pod.PrevAt)
	if elapsed < nonNegative(in.Policy.MinInterval) || elapsed <= 0 {
		return 0, false
	}
	return (rate - pod.PrevRate) / elapsed.Seconds(), true
}

func nonNegative(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}

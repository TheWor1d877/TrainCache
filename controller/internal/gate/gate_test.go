package gate

import (
	"testing"
	"time"
)

var testNow = time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)

// pod builds a single-node PodGateState. Passing no node yields an empty
// preheat set.
func pod(index int32, k, ready int32, nodes ...string) PodGateState {
	readyByNode := make(map[string]int32, len(nodes))
	for _, node := range nodes {
		readyByNode[node] = ready
	}
	rate := 0.0
	if k > 0 {
		rate = float64(ready) / float64(k)
	}
	return PodGateState{
		Index:         index,
		K:             k,
		AssignedNodes: nodes,
		ReadyByNode:   readyByNode,
		Rate:          rate,
	}
}

// multiPod builds a multi-node PodGateState with per-node readiness. Rate is
// the maximum ReadyByNode[node]/K, matching §7.5.
func multiPod(index, k int32, readyByNode map[string]int32, nodes []string) PodGateState {
	best := int32(0)
	for _, node := range nodes {
		if readyByNode[node] > best {
			best = readyByNode[node]
		}
	}
	rate := 0.0
	if k > 0 {
		rate = float64(best) / float64(k)
	}
	return PodGateState{
		Index:         index,
		K:             k,
		AssignedNodes: nodes,
		ReadyByNode:   readyByNode,
		Rate:          rate,
	}
}

func allReadyPods(k int32, count int) []PodGateState {
	pods := make([]PodGateState, count)
	for i := range pods {
		pods[i] = pod(int32(i), k, k, "node")
	}
	return pods
}

func TestRequiredReadyMatrix(t *testing.T) {
	cases := []struct {
		k         int32
		threshold float64
		want      int32
	}{
		{1, 0, 0},
		{1, 0.8, 1},
		{1, 0.9, 1},
		{7, 0, 0},
		{7, 0.8, 5},
		{7, 0.9, 6},
		{8, 0, 0},
		{8, 0.8, 6},
		{8, 0.9, 7},
		{9, 0, 0},
		{9, 0.8, 7},
		{9, 0.9, 8},
	}
	for _, tc := range cases {
		if got := RequiredReady(tc.threshold, tc.k); got != tc.want {
			t.Fatalf("RequiredReady(%v, %d) = %d, want %d", tc.threshold, tc.k, got, tc.want)
		}
	}
}

func TestEvaluateMatrixConditionOne(t *testing.T) {
	for _, k := range []int32{1, 7, 8, 9} {
		for _, threshold := range []float64{0, 0.8, 0.9} {
			t.Run("", func(t *testing.T) {
				required := RequiredReady(threshold, k)
				pods := allReadyPods(k, 1)
				in := Input{
					Now:    testNow,
					Policy: GatePolicy{Threshold: threshold},
					Pods:   pods,
				}
				decision := Evaluate(in)
				if decision.State != Released {
					t.Fatalf("K=%d threshold=%v ready=K state = %s, want Released", k, threshold, decision.State)
				}
				if decision.Rates[0].Required != required {
					t.Fatalf("K=%d threshold=%v required = %d, want %d", k, threshold, decision.Rates[0].Required, required)
				}

				if threshold == 0 {
					// threshold 0 makes condition ① true even with Ready 0.
					in.Pods[0] = pod(0, k, 0, "node")
					if got := Evaluate(in).State; got != Released {
						t.Fatalf("threshold=0 zero-ready state = %s, want Released", got)
					}
					return
				}

				in.Pods[0] = pod(0, k, required-1, "node")
				if got := Evaluate(in).State; got != Hold {
					t.Fatalf("K=%d threshold=%v below-required state = %s, want Hold", k, threshold, got)
				}
			})
		}
	}
}

// TestEvaluateConditionOneAnyNodePasses pins the existential quantifier: one
// node at required is enough, while every node below required still holds.
func TestEvaluateConditionOneAnyNodePasses(t *testing.T) {
	nodes := []string{"node-a", "node-b", "node-c"}
	in := Input{
		Now:    testNow,
		Policy: GatePolicy{Threshold: 0.8},
		Pods: []PodGateState{multiPod(0, 8,
			map[string]int32{"node-a": 0, "node-b": 6, "node-c": 2}, nodes)},
	}
	if got := Evaluate(in).State; got != Released {
		t.Fatalf("state = %s, want Released when one node reaches required", got)
	}

	in.Pods[0] = multiPod(0, 8,
		map[string]int32{"node-a": 5, "node-b": 5, "node-c": 5}, nodes)
	if got := Evaluate(in).State; got != Hold {
		t.Fatalf("state = %s, want Hold when every node is below required", got)
	}
}

// TestEvaluateConditionOneIgnoresNodesOutsideSet pins D38: candidate nodes
// that were not chosen as preheat nodes do not participate in condition ① or
// in the observation rate.
func TestEvaluateConditionOneIgnoresNodesOutsideSet(t *testing.T) {
	in := Input{
		Now:    testNow,
		Policy: GatePolicy{Threshold: 0.8},
		Pods: []PodGateState{{
			Index: 0, K: 8,
			AssignedNodes: []string{"node-a"},
			ReadyByNode:   map[string]int32{"node-a": 2, "node-b": 8},
		}},
	}
	decision := Evaluate(in)
	if decision.State != Hold {
		t.Fatalf("state = %s, want Hold when only a non-preheat node is ready", decision.State)
	}
	if decision.Rates[0].Rate != 0.25 {
		t.Fatalf("Rate = %v, want 0.25 from node-a only", decision.Rates[0].Rate)
	}
}

// TestEvaluateConditionOneThresholdZeroMultiNode checks required 0 with a
// non-empty set releases regardless of per-node readiness.
func TestEvaluateConditionOneThresholdZeroMultiNode(t *testing.T) {
	in := Input{
		Now:    testNow,
		Policy: GatePolicy{Threshold: 0},
		Pods: []PodGateState{multiPod(0, 8,
			map[string]int32{"node-a": 0, "node-b": 0}, []string{"node-a", "node-b"})},
	}
	if got := Evaluate(in).State; got != Released {
		t.Fatalf("state = %s, want Released with required 0 and a non-empty set", got)
	}
}

func TestEvaluateConditionOneEmptySetFails(t *testing.T) {
	cases := []PodGateState{
		// Completely empty.
		{Index: 0, K: 8},
		// Ready facts exist, but no preheat node was assigned.
		{Index: 0, K: 8, ReadyByNode: map[string]int32{"node": 8}, Rate: 1},
	}
	for i, p := range cases {
		in := Input{Now: testNow, Pods: []PodGateState{p}}
		if got := Evaluate(in).State; got != Hold {
			t.Fatalf("case %d: state = %s, want Hold without a preheat set", i, got)
		}
	}
}

func TestEvaluateConditionOneUsesIntegersNotRate(t *testing.T) {
	in := Input{
		Now:    testNow,
		Policy: GatePolicy{Threshold: 0.8},
		Pods: []PodGateState{{
			Index: 0, K: 8, AssignedNodes: []string{"node"},
			ReadyByNode: map[string]int32{"node": 6},
			Rate:        0.1, // stale observation must not affect condition ①
		}},
	}
	decision := Evaluate(in)
	if decision.State != Released {
		t.Fatalf("state = %s, want Released from Ready >= required", decision.State)
	}
	if decision.Rates[0].Rate != 0.75 {
		t.Fatalf("Rate = %v, want max ReadyByNode/K = 0.75", decision.Rates[0].Rate)
	}
}

// TestEvaluateRateIsMaxOverNodes pins the observation rate used by Gap/Slope.
func TestEvaluateRateIsMaxOverNodes(t *testing.T) {
	in := Input{
		Now:    testNow,
		Policy: GatePolicy{Threshold: 0.9},
		Pods: []PodGateState{multiPod(0, 8,
			map[string]int32{"node-a": 2, "node-b": 6, "node-c": 4},
			[]string{"node-a", "node-b", "node-c"})},
	}
	decision := Evaluate(in)
	if decision.Rates[0].Rate != 0.75 {
		t.Fatalf("Rate = %v, want 0.75", decision.Rates[0].Rate)
	}
	if decision.Rates[0].Ready != 6 {
		t.Fatalf("Ready = %d, want 6", decision.Rates[0].Ready)
	}
	if diff := decision.Rates[0].Gap - 0.15; diff > 1e-12 || diff < -1e-12 {
		t.Fatalf("Gap = %v, want 0.15", decision.Rates[0].Gap)
	}
}

// TestEvaluateFanoutOneRegression confirms a single-node input behaves like
// the pre-fanout gate at the threshold boundary.
func TestEvaluateFanoutOneRegression(t *testing.T) {
	for _, k := range []int32{1, 7, 8, 9} {
		for _, threshold := range []float64{0.8, 0.9} {
			required := RequiredReady(threshold, k)
			in := Input{
				Now:    testNow,
				Policy: GatePolicy{Threshold: threshold},
				Pods:   []PodGateState{pod(0, k, required-1, "node")},
			}
			if got := Evaluate(in).State; got != Hold {
				t.Fatalf("K=%d threshold=%v required-1 state = %s, want Hold", k, threshold, got)
			}
			in.Pods[0] = pod(0, k, required, "node")
			if got := Evaluate(in).State; got != Released {
				t.Fatalf("K=%d threshold=%v required state = %s, want Released", k, threshold, got)
			}
		}
	}
}

func TestEvaluateTMaxWinsOverReady(t *testing.T) {
	started := testNow.Add(-10 * time.Minute)
	in := Input{
		Now:       testNow,
		StartedAt: started,
		Policy:    GatePolicy{Threshold: 0.8, TMax: 5 * time.Minute},
		Pods:      allReadyPods(8, 1),
	}
	decision := Evaluate(in)
	if decision.State != Degraded || decision.Reason != ReasonTMaxExceeded {
		t.Fatalf("decision = %+v, want Degraded/tmax", decision)
	}

	in.Policy.TMax = 20 * time.Minute
	if got := Evaluate(in).State; got != Released {
		t.Fatalf("before TMax state = %s, want Released", got)
	}
}

func TestEvaluateTMaxDisabledWhenZero(t *testing.T) {
	in := Input{
		Now:       testNow,
		StartedAt: testNow.Add(-24 * time.Hour),
		Policy:    GatePolicy{Threshold: 0.8, TMax: 0},
		Pods:      []PodGateState{pod(0, 8, 0, "node")},
	}
	if got := Evaluate(in).State; got != Hold {
		t.Fatalf("state = %s, want Hold with TMax disabled", got)
	}
}

func TestEvaluateMarginalGain(t *testing.T) {
	policy := GatePolicy{
		Threshold:   0.8,
		GapMin:      0.5,
		SlopeMin:    0.0005,
		MinInterval: time.Minute,
	}
	firstReady := testNow.Add(-2 * time.Minute)

	// second builds the unready Pod at rate ready/K with an explicit previous
	// sample, while the first Pod already meets its threshold on node-a.
	second := func(ready int32, prev float64, prevAt time.Time) PodGateState {
		p := pod(1, 8, ready, "node-b")
		p.PrevRate = prev
		p.PrevAt = prevAt
		p.FirstSampleSeen = true
		return p
	}
	base := func(unready PodGateState) Input {
		return Input{
			Now:          testNow,
			Policy:       policy,
			FirstReadyAt: firstReady,
			Pods:         []PodGateState{pod(0, 8, 8, "node-a"), unready},
		}
	}

	// gap = 0.8-0.125 = 0.675 > 0.5 and slope = (0.125-0.125)/120s = 0 < 0.0005.
	if got := Evaluate(base(second(1, 0.125, testNow.Add(-2*time.Minute)))); got.State != Released ||
		got.Reason != ReasonMarginalGain {
		t.Fatalf("state = %s reason = %s, want marginal-gain release", got.State, got.Reason)
	}

	// gap small: 0.8-0.625 = 0.175, not > GapMin.
	if got := Evaluate(base(second(5, 0.625, testNow.Add(-2*time.Minute)))); got.State != Hold {
		t.Fatalf("near-threshold state = %s, want Hold", got.State)
	}

	// slope still positive and above SlopeMin.
	if got := Evaluate(base(second(1, 0.0, testNow.Add(-2*time.Minute)))); got.State != Hold {
		t.Fatalf("rising state = %s, want Hold", got.State)
	}

	// Not enough samples.
	noPrev := second(1, 0.125, time.Time{})
	noPrev.FirstSampleSeen = false
	if got := Evaluate(base(noPrev)); got.State != Hold {
		t.Fatalf("missing-sample state = %s, want Hold", got.State)
	}

	// Sample interval below MinInterval.
	if got := Evaluate(base(second(1, 0.125, testNow.Add(-30*time.Second)))); got.State != Hold {
		t.Fatalf("short-interval state = %s, want Hold", got.State)
	}

	// FirstReadyAt unknown: condition ② cannot trigger.
	noFirst := base(second(1, 0.125, testNow.Add(-2*time.Minute)))
	noFirst.FirstReadyAt = time.Time{}
	if got := Evaluate(noFirst); got.State != Hold {
		t.Fatalf("no-first-ready state = %s, want Hold", got.State)
	}

	// FirstReadyAt younger than MinInterval.
	young := base(second(1, 0.125, testNow.Add(-2*time.Minute)))
	young.FirstReadyAt = testNow.Add(-30 * time.Second)
	if got := Evaluate(young); got.State != Hold {
		t.Fatalf("young-first-ready state = %s, want Hold", got.State)
	}

	// Every unready Pod must satisfy the condition; one rising Pod blocks it.
	blocked := base(second(1, 0.125, testNow.Add(-2*time.Minute)))
	blocked.Pods = append(blocked.Pods, second(1, 0.0, testNow.Add(-2*time.Minute)))
	blocked.Pods[2].Index = 2
	blocked.Pods[2].AssignedNodes = []string{"node-c"}
	blocked.Pods[2].ReadyByNode = map[string]int32{"node-c": 1}
	if got := Evaluate(blocked); got.State != Hold {
		t.Fatalf("state = %s, want Hold when another unready Pod is still rising", got.State)
	}
}

// TestEvaluateMarginalGainMultiNode checks condition ② is unchanged under
// fanout: a Pod that meets its threshold on one node is skipped, and a Pod
// still below threshold on every node is judged by its maximum rate slope.
func TestEvaluateMarginalGainMultiNode(t *testing.T) {
	in := Input{
		Now: testNow,
		Policy: GatePolicy{
			Threshold:   0.8,
			GapMin:      0.5,
			SlopeMin:    0.0005,
			MinInterval: time.Minute,
		},
		FirstReadyAt: testNow.Add(-2 * time.Minute),
		Pods: []PodGateState{
			// Meets threshold on node-b only.
			multiPod(0, 8, map[string]int32{"node-a": 0, "node-b": 6},
				[]string{"node-a", "node-b"}),
			// Every node below threshold; max rate 0.125 and flat.
			func() PodGateState {
				p := multiPod(1, 8, map[string]int32{"node-c": 1, "node-d": 0},
					[]string{"node-c", "node-d"})
				p.PrevRate = 1.0 / 8.0
				p.PrevAt = testNow.Add(-2 * time.Minute)
				p.FirstSampleSeen = true
				return p
			}(),
		},
	}
	decision := Evaluate(in)
	if decision.State != Released || decision.Reason != ReasonMarginalGain {
		t.Fatalf("decision = %+v, want marginal-gain release", decision)
	}
}

func TestEvaluateSlopeUsesContinuousRate(t *testing.T) {
	in := Input{
		Now: testNow,
		Policy: GatePolicy{
			Threshold: 0.9, GapMin: 0.5, SlopeMin: 0.01, MinInterval: time.Minute,
		},
		FirstReadyAt: testNow.Add(-2 * time.Minute),
		Pods: []PodGateState{
			pod(0, 8, 8, "a"),
			{
				Index: 1, K: 8, AssignedNodes: []string{"b"},
				ReadyByNode: map[string]int32{"b": 5},
				Rate:        5.0 / 8.0, PrevRate: 4.0 / 8.0, PrevAt: testNow.Add(-2 * time.Minute),
				FirstSampleSeen: true,
			},
		},
	}
	decision := Evaluate(in)
	if !decision.Rates[1].SlopeValid {
		t.Fatal("slope should be valid with two samples")
	}
	wantSlope := (5.0/8.0 - 4.0/8.0) / 120.0
	if diff := decision.Rates[1].Slope - wantSlope; diff > 1e-12 || diff < -1e-12 {
		t.Fatalf("slope = %v, want %v", decision.Rates[1].Slope, wantSlope)
	}
}

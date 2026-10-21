package gate

import (
	"testing"
	"time"
)

var testNow = time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)

func pod(index int32, k, ready int32, node string) PodGateState {
	rate := 0.0
	if k > 0 {
		rate = float64(ready) / float64(k)
	}
	return PodGateState{Index: index, K: k, Ready: ready, Rate: rate, AssignedNode: node}
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

				in.Pods[0].Ready = required - 1
				in.Pods[0].Rate = float64(required-1) / float64(k)
				if got := Evaluate(in).State; got != Hold {
					t.Fatalf("K=%d threshold=%v below-required state = %s, want Hold", k, threshold, got)
				}
			})
		}
	}
}

func TestEvaluateConditionOneRequiresAssignedNode(t *testing.T) {
	in := Input{Now: testNow, Pods: []PodGateState{pod(0, 8, 8, "")}}
	if got := Evaluate(in).State; got != Hold {
		t.Fatalf("state = %s, want Hold without an assigned node", got)
	}
}

func TestEvaluateConditionOneUsesIntegersNotRate(t *testing.T) {
	in := Input{
		Now:    testNow,
		Policy: GatePolicy{Threshold: 0.8},
		Pods: []PodGateState{{
			Index: 0, K: 8, Ready: 6, Rate: 0.1, // Rate is observation only.
			AssignedNode: "node",
		}},
	}
	if got := Evaluate(in).State; got != Released {
		t.Fatalf("state = %s, want Released from Ready >= required", got)
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

	base := func(ready int32, rate, prev float64, prevAt time.Time) Input {
		return Input{
			Now:          testNow,
			Policy:       policy,
			FirstReadyAt: firstReady,
			Pods: []PodGateState{
				pod(0, 8, 8, "node-a"), // one Pod already met its threshold
				{
					Index: 1, K: 8, Ready: ready, Rate: rate,
					PrevRate: prev, PrevAt: prevAt, FirstSampleSeen: true,
					AssignedNode: "node-b",
				},
			},
		}
	}

	// gap = 0.8-0.1 = 0.7 > 0.5 and slope = (0.1-0.1)/120s = 0 < 0.0005.
	if got := Evaluate(base(1, 0.1, 0.1, testNow.Add(-2*time.Minute))); got.State != Released ||
		got.Reason != ReasonMarginalGain {
		t.Fatalf("state = %s reason = %s, want marginal-gain release", got.State, got.Reason)
	}

	// gap small: 0.8-0.625 = 0.175, not > GapMin.
	if got := Evaluate(base(5, 0.625, 0.625, testNow.Add(-2*time.Minute))); got.State != Hold {
		t.Fatalf("near-threshold state = %s, want Hold", got.State)
	}

	// slope still positive and above SlopeMin.
	if got := Evaluate(base(1, 0.1, 0.0, testNow.Add(-2*time.Minute))); got.State != Hold {
		t.Fatalf("rising state = %s, want Hold", got.State)
	}

	// Not enough samples.
	noPrev := base(1, 0.1, 0, time.Time{})
	noPrev.Pods[1].FirstSampleSeen = false
	if got := Evaluate(noPrev); got.State != Hold {
		t.Fatalf("missing-sample state = %s, want Hold", got.State)
	}

	// Sample interval below MinInterval.
	tooClose := base(1, 0.1, 0.1, testNow.Add(-30*time.Second))
	if got := Evaluate(tooClose); got.State != Hold {
		t.Fatalf("short-interval state = %s, want Hold", got.State)
	}

	// FirstReadyAt unknown: condition ② cannot trigger.
	noFirst := base(1, 0.1, 0.1, testNow.Add(-2*time.Minute))
	noFirst.FirstReadyAt = time.Time{}
	if got := Evaluate(noFirst); got.State != Hold {
		t.Fatalf("no-first-ready state = %s, want Hold", got.State)
	}

	// FirstReadyAt younger than MinInterval.
	young := base(1, 0.1, 0.1, testNow.Add(-2*time.Minute))
	young.FirstReadyAt = testNow.Add(-30 * time.Second)
	if got := Evaluate(young); got.State != Hold {
		t.Fatalf("young-first-ready state = %s, want Hold", got.State)
	}

	// Every unready Pod must satisfy the condition; one rising Pod blocks it.
	blocked := base(1, 0.1, 0.1, testNow.Add(-2*time.Minute))
	blocked.Pods = append(blocked.Pods, PodGateState{
		Index: 2, K: 8, Ready: 1, Rate: 0.1, PrevRate: 0, PrevAt: testNow.Add(-2 * time.Minute),
		FirstSampleSeen: true, AssignedNode: "node-c",
	})
	if got := Evaluate(blocked); got.State != Hold {
		t.Fatalf("state = %s, want Hold when another unready Pod is still rising", got.State)
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
				Index: 1, K: 8, Ready: 5, Rate: 5.0 / 8.0,
				PrevRate: 4.0 / 8.0, PrevAt: testNow.Add(-2 * time.Minute),
				FirstSampleSeen: true, AssignedNode: "b",
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

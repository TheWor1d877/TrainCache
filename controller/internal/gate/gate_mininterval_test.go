package gate

import (
	"testing"
	"time"
)

// TestEvaluateMinIntervalBoundary pins both >= boundaries in condition ②: the
// age of FirstReadyAt and the spacing between slope samples. At exactly
// MinInterval both sub-conditions hold (docs/8.3 §4).
func TestEvaluateMinIntervalBoundary(t *testing.T) {
	policy := GatePolicy{
		Threshold:   0.8,
		GapMin:      0.5,
		SlopeMin:    0.0005,
		MinInterval: time.Minute,
	}
	firstReady := testNow

	// ready builds a two-Pod input: Pod 0 already meets its threshold on
	// node-a, Pod 1 is flat at 1/8 on node-b with an optional previous sample.
	ready := func(now time.Time, sampled bool, prevAt time.Time) Input {
		second := pod(1, 8, 1, "node-b")
		if sampled {
			second.FirstSampleSeen = true
			second.PrevRate = 0.125
			second.PrevAt = prevAt
		}
		return Input{
			Now:          now,
			Policy:       policy,
			FirstReadyAt: firstReady,
			Pods:         []PodGateState{pod(0, 8, 8, "node-a"), second},
		}
	}

	// The second Pod is not ready yet at t=MinInterval: no slope sample, so
	// condition ② fails and the gate holds.
	if got := Evaluate(ready(firstReady.Add(time.Minute), false, time.Time{})); got.State != Hold {
		t.Fatalf("no sample at t=MinInterval: state = %s, want Hold", got.State)
	}

	// Age exactly MinInterval and sample spacing exactly MinInterval: release.
	edge := Evaluate(ready(firstReady.Add(time.Minute), true, firstReady))
	if edge.State != Released || edge.Reason != ReasonMarginalGain {
		t.Fatalf("t=MinInterval: state = %s reason = %s, want marginal-gain release", edge.State, edge.Reason)
	}

	// Age 1ns below MinInterval: hold, even though the sample spacing is fine.
	young := ready(firstReady.Add(time.Minute-time.Nanosecond), true, firstReady.Add(-time.Second))
	if got := Evaluate(young); got.State != Hold {
		t.Fatalf("age 1ns below MinInterval: state = %s, want Hold", got.State)
	}

	// Sample spacing 1ns below MinInterval: hold, even though the age is fine.
	short := ready(firstReady.Add(2*time.Minute), true, firstReady.Add(time.Minute+time.Nanosecond))
	if got := Evaluate(short); got.State != Hold {
		t.Fatalf("sample spacing 1ns below MinInterval: state = %s, want Hold", got.State)
	}

	// Sample spacing exactly MinInterval: release.
	exact := ready(firstReady.Add(2*time.Minute), true, firstReady.Add(time.Minute))
	if got := Evaluate(exact); got.State != Released {
		t.Fatalf("sample spacing = MinInterval: state = %s, want Released", got.State)
	}
}

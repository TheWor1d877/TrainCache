package nodescore

import (
	"math"
	"testing"
)

func TestWeightBoundariesPerPod(t *testing.T) {
	cases := []struct {
		name   string
		params Params
		i      int32
		want   float64
	}{
		// Pod A: K=8, H=4, C=12. Platform is [0,8), decay is [8,12).
		{"A before platform end", Params{K: 8, H: 4, C: 12}, 7, 1},
		{"A platform end is still 1", Params{K: 8, H: 4, C: 12}, 8, 1},
		{"A first decay", Params{K: 8, H: 4, C: 12}, 9, math.Pow(0.5, 0.25)},
		{"A last decay", Params{K: 8, H: 4, C: 12}, 11, math.Pow(0.5, 0.75)},
		{"A at cutoff", Params{K: 8, H: 4, C: 12}, 12, 0},
		// Pod B: K=4, H=2, C=12. Different K_i must shift the boundaries.
		{"B before platform end", Params{K: 4, H: 2, C: 12}, 3, 1},
		{"B platform end is still 1", Params{K: 4, H: 2, C: 12}, 4, 1},
		{"B first decay", Params{K: 4, H: 2, C: 12}, 5, math.Pow(0.5, 0.5)},
		{"B last decay", Params{K: 4, H: 2, C: 12}, 11, math.Pow(0.5, 3.5)},
		{"B at cutoff", Params{K: 4, H: 2, C: 12}, 12, 0},
		{"negative index", Params{K: 8, H: 4, C: 12}, -1, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Weight(tc.params, tc.i)
			if math.Abs(got-tc.want) > 1e-12 {
				t.Fatalf("Weight = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestComputeDifferentKPerPod(t *testing.T) {
	bits := []bool{true, true, true, true, true, true, false, false}

	// Pod A: K=4, H=2, C=8. Weights: 4x1 in-window + 1 + 0.5^0.5 + 0.5 +
	// 0.5^1.5 out-of-window.
	paramsA := Params{K: 4, H: 2, C: 8}
	weightSumA := 4 + 1 + math.Pow(0.5, 0.5) + 0.5 + math.Pow(0.5, 1.5)
	gotA := Compute(bits, paramsA)
	wantRawA := 100 * (4 + 1 + math.Pow(0.5, 0.5)) / weightSumA
	assertNear(t, "A WeightSum", gotA.WeightSum, weightSumA)
	assertNear(t, "A Raw", gotA.Raw, wantRawA)
	if gotA.Total != 87 {
		t.Fatalf("A Total = %d, want 87 (raw %.4f)", gotA.Total, wantRawA)
	}
	assertNear(t, "A WindowIn", gotA.WindowIn, 100*4/weightSumA)
	assertNear(t, "A WindowOut", gotA.WindowOut, 100*(1+math.Pow(0.5, 0.5))/weightSumA)
	assertNear(t, "A components sum", gotA.WindowIn+gotA.WindowOut, gotA.Raw)
	if gotA.ReadyInWindow != 4 || gotA.ReadyTotal != 6 {
		t.Fatalf("A ready = (%d,%d), want (4,6)", gotA.ReadyInWindow, gotA.ReadyTotal)
	}

	// Pod B: K=6, H=1, C=8. All six platform bits ready; weights sum to
	// 6 + 0.5^0 + 0.5^1 = 7.5.
	paramsB := Params{K: 6, H: 1, C: 8}
	gotB := Compute(bits, paramsB)
	wantRawB := 100 * 6 / 7.5
	assertNear(t, "B Raw", gotB.Raw, wantRawB)
	if gotB.Total != 80 {
		t.Fatalf("B Total = %d, want 80 (raw %.4f)", gotB.Total, wantRawB)
	}
	if gotB.WindowIn != gotB.Raw || gotB.WindowOut != 0 {
		t.Fatalf("B components = (%v,%v), want all in-window", gotB.WindowIn, gotB.WindowOut)
	}
}

func TestComputeZeroWeightSum(t *testing.T) {
	for _, params := range []Params{{K: 4, H: 2, C: 0}, {K: 0, H: 2, C: -3}} {
		got := Compute([]bool{true, true}, params)
		if got.Total != 0 || got.Raw != 0 || got.WeightSum != 0 {
			t.Fatalf("Compute(%+v) = %+v, want zero score", params, got)
		}
	}
}

func TestComputeRounding(t *testing.T) {
	// 7/8 ready with K=C=8 -> 87.5, rounds half up to 88.
	seven := make([]bool, 8)
	for i := range seven {
		seven[i] = true
	}
	seven[7] = false
	if got := Compute(seven, Params{K: 8, H: 1, C: 8}); got.Total != 88 {
		t.Fatalf("87.5 Total = %d, want 88", got.Total)
	}

	// 1/8 ready -> 12.5, rounds half up to 13.
	one := make([]bool, 8)
	one[0] = true
	if got := Compute(one, Params{K: 8, H: 1, C: 8}); got.Total != 13 {
		t.Fatalf("12.5 Total = %d, want 13", got.Total)
	}

	// 2/3 ready -> 66.66.., rounds to 67.
	if got := Compute([]bool{true, true, false}, Params{K: 3, H: 1, C: 3}); got.Total != 67 {
		t.Fatalf("2/3 Total = %d, want 67", got.Total)
	}
}

func TestComputeClampsToRange(t *testing.T) {
	all := make([]bool, 8)
	for i := range all {
		all[i] = true
	}
	got := Compute(all, Params{K: 4, H: 2, C: 8})
	if got.Total != 100 || got.Raw > 100 || got.WindowIn > 100 || got.WindowOut > 100 {
		t.Fatalf("all-ready score = %+v, want 100 without overshoot", got)
	}
}

func TestComputeKGreaterThanCutoff(t *testing.T) {
	// Platform is truncated to C; there is no decay segment.
	got := Compute([]bool{true, true, true, true, true, true}, Params{K: 10, H: 2, C: 4})
	if got.WeightSum != 4 || got.WindowInWeightSum != 4 || got.WindowOutWeightSum != 0 {
		t.Fatalf("weights = %+v, want 4/4/0", got)
	}
	if got.Total != 100 {
		t.Fatalf("Total = %d, want 100", got.Total)
	}
}

func TestComputeMissingBitsCountAsNotReady(t *testing.T) {
	// No bits supplied: denominator still covers [0,C).
	got := Compute(nil, Params{K: 2, H: 1, C: 4})
	if got.Total != 0 || got.ReadyTotal != 0 {
		t.Fatalf("missing bits = %+v, want zero score", got)
	}
	if math.Abs(got.WeightSum-3.5) > 1e-12 {
		t.Fatalf("WeightSum = %v, want 3.5", got.WeightSum)
	}
}

func TestComputeBitsPastCutoffIgnored(t *testing.T) {
	// Bits at/after C must not affect the score.
	bits := []bool{true, true, true, true}
	got := Compute(bits, Params{K: 4, H: 1, C: 2})
	if got.ReadyTotal != 2 || got.Total != 100 || got.ReadyInWindow != 2 {
		t.Fatalf("bits past cutoff = %+v, want only the first two counted", got)
	}
}

func assertNear(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

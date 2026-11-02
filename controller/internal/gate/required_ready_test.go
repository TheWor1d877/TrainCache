package gate

import (
	"math"
	"testing"
)

// TestRequiredReadyMatrix pins the K x threshold matrix from S19 and
// asserts the invariant required <= K for every cell.
func TestRequiredReadyMatrix(t *testing.T) {
	cases := []struct {
		k         int32
		threshold float64
		want      int32
	}{
		{1, 0, 0}, {1, 0.8, 1}, {1, 0.9, 1},
		{7, 0, 0}, {7, 0.8, 5}, {7, 0.9, 6},
		{8, 0, 0}, {8, 0.8, 6}, {8, 0.9, 7},
		{9, 0, 0}, {9, 0.8, 7}, {9, 0.9, 8},
	}
	for _, tc := range cases {
		got := RequiredReady(tc.threshold, tc.k)
		if got != tc.want {
			t.Fatalf("RequiredReady(%v, %d) = %d, want %d", tc.threshold, tc.k, got, tc.want)
		}
		if got < 0 || got > tc.k {
			t.Fatalf("RequiredReady(%v, %d) = %d violates 0 <= required <= K", tc.threshold, tc.k, got)
		}
		switch {
		case tc.threshold == 0 && got != 0:
			t.Fatalf("threshold 0 gave required %d, want 0", got)
		case tc.threshold > 0 && got < 1:
			t.Fatalf("threshold %v gave required %d, want >= 1", tc.threshold, got)
		}
	}
}

// TestRequiredReadyFloorProperty checks the floor formula over a wider
// grid and the required <= K clamp.
func TestRequiredReadyFloorProperty(t *testing.T) {
	ks := []int32{1, 2, 7, 8, 9, 16, 64, 256}
	for _, k := range ks {
		for step := 0; step <= 20; step++ {
			threshold := float64(step) / 20 // 0.00 .. 1.00
			got := RequiredReady(threshold, k)

			want := int32(math.Floor(threshold * float64(k)))
			if threshold > 0 && want == 0 {
				want = 1
			}
			if want > k {
				want = k
			}
			if got != want {
				t.Fatalf("RequiredReady(%v, %d) = %d, want %d", threshold, k, got, want)
			}
			if got < 0 || got > k {
				t.Fatalf("RequiredReady(%v, %d) = %d violates 0 <= required <= K", threshold, k, got)
			}
		}
	}

	// Non-positive K is outside the contract and must never yield > 0.
	for _, k := range []int32{0, -1} {
		if got := RequiredReady(0.9, k); got != 0 {
			t.Fatalf("RequiredReady(0.9, %d) = %d, want 0", k, got)
		}
	}
}

// This file computes the global decay readiness score from docs/8.0
// "就绪度得分逻辑" and docs/8.1 design.md §7.6.
package nodescore

import "math"

// Params is one Pod's scoring window. K_i comes from PodDetail.K (D25), H and
// C come from spec.score. The platform segment is min(K, C) chunks long; the
// decay segment is [min(K, C), C) with weight 0.5^((i-K)/H).
type Params struct {
	// K is the startup window (platform segment length), K >= 1 per CRD
	// validation. Negative values are treated as 0 here.
	K int32
	// H is the half-life in chunks. H <= 0 makes the decay segment weigh 0.
	H float64
	// C is the cutoff. Chunks at i >= C carry no weight. Negative values are
	// treated as 0 here.
	C int32
}

// Score is the readiness score of one Pod on one node.
//
// Total is round(100*Σ(w·ready)/Σw) clamped to [0,100]; when Σw=0, Total=0.
// Raw and the WindowIn/WindowOut components are unrounded so callers can store
// or log the in-window and out-window readiness separately without double
// rounding. WindowIn+WindowOut == Raw. Component values are fractions of the
// same denominator (Σw), so they are already on the 0..100 scale.
type Score struct {
	// Total is the rounded score written to the Node annotation.
	Total int32
	// Raw is the unrounded score, 0..100.
	Raw float64
	// WindowIn is the unrounded contribution of chunks i < min(K,C).
	WindowIn float64
	// WindowOut is the unrounded contribution of chunks min(K,C) <= i < C.
	WindowOut float64
	// WeightSum is Σw over i in [0,C).
	WeightSum float64
	// WindowInWeightSum is Σw over i < min(K,C).
	WindowInWeightSum float64
	// WindowOutWeightSum is Σw over min(K,C) <= i < C.
	WindowOutWeightSum float64
	// ReadyInWindow counts ready bits with i < min(K,C).
	ReadyInWindow int32
	// ReadyTotal counts ready bits with i < C.
	ReadyTotal int32
}

// Weight returns w(i) for the Pod described by p.
func Weight(p Params, i int32) float64 {
	k, h, c := normalize(p)
	if i < 0 || i >= c {
		return 0
	}
	return weightAt(i, k, h)
}

// Compute scores one Pod's ready bits. Bits past C and missing bits are
// ignored (they count as not ready) but still contribute to the denominator,
// matching "位图缺失的位按 0 计".
func Compute(bits []bool, p Params) Score {
	k, h, c := normalize(p)

	var (
		s           Score
		weighted    float64
		inWeighted  float64
		outWeighted float64
	)
	for i := int32(0); i < c; i++ {
		w := weightAt(i, k, h)
		s.WeightSum += w
		inWindow := i < k
		if inWindow {
			s.WindowInWeightSum += w
		} else {
			s.WindowOutWeightSum += w
		}
		if int(i) < len(bits) && bits[i] {
			if inWindow {
				s.ReadyInWindow++
			}
			s.ReadyTotal++
			weighted += w
			if inWindow {
				inWeighted += w
			} else {
				outWeighted += w
			}
		}
	}

	if s.WeightSum <= 0 {
		return s
	}
	s.Raw = 100 * weighted / s.WeightSum
	s.WindowIn = 100 * inWeighted / s.WeightSum
	s.WindowOut = 100 * outWeighted / s.WeightSum
	s.Total = roundScore(s.Raw)
	return s
}

// weightAt assumes k, h, c are already normalized (0 <= k <= c).
func weightAt(i, k int32, h float64) float64 {
	if i < k {
		return 1
	}
	if h <= 0 {
		return 0
	}
	return math.Pow(0.5, float64(i-k)/h)
}

func normalize(p Params) (k int32, h float64, c int32) {
	c = p.C
	if c < 0 {
		c = 0
	}
	k = p.K
	if k < 0 {
		k = 0
	}
	if k > c {
		k = c
	}
	return k, p.H, c
}

// roundScore rounds half away from zero (math.Round) and clamps to [0,100];
// scores are non-negative, so this is plain四舍五入. The clamp guards against
// floating-point overshoot of the exact maximum.
func roundScore(v float64) int32 {
	if v <= 0 {
		return 0
	}
	if v >= 100 {
		return 100
	}
	return int32(math.Round(v))
}

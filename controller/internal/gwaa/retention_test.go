package gwaa

// Gatekeeping for the D45 hysteresis (docs/7.1 §5.5, docs/7.3 D45). The wide
// preheat hands the same window to every candidate, so their readiness counts
// advance in lockstep; with a strict c_ij ordering the target set then changed
// hands every few seconds and each change cost one Submit plus one Cancel.

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestComputeKeepsHeldNodeInsideRetentionLine(t *testing.T) {
	ready := map[string]int32{"n0": 4, "n1": 5, "n2": 0}
	bare := Compute(Input{
		Pods:  []PodState{testPod(0, ready)},
		Nodes: testNodes("n0", "n1", "n2"),
		Rand:  rand.New(rand.NewSource(1)),
	})
	if got := bare.Assignment[0]; !reflect.DeepEqual(got, []string{"n1"}) {
		t.Fatalf("without Held assignment = %v, want [n1] (strict best)", got)
	}

	kept := Compute(Input{
		Pods:  []PodState{testPod(0, ready)},
		Nodes: testNodes("n0", "n1", "n2"),
		Held:  map[int32][]string{0: {"n0"}},
		Rand:  rand.New(rand.NewSource(1)),
	})
	if got := kept.Assignment[0]; !reflect.DeepEqual(got, []string{"n0"}) {
		t.Fatalf("assignment = %v, want [n0]: it is within the retention line (2*4 >= 5)", got)
	}
	if kept.Decisions[0].TargetFanout != 1 {
		t.Fatalf("target fanout = %d, want 1: hysteresis never grows the set", kept.Decisions[0].TargetFanout)
	}
}

func TestComputeDropsHeldNodeOutsideRetentionLine(t *testing.T) {
	out := Compute(Input{
		Pods:  []PodState{testPod(0, map[string]int32{"n0": 1, "n1": 5})},
		Nodes: testNodes("n0", "n1"),
		Held:  map[int32][]string{0: {"n0"}},
		Rand:  rand.New(rand.NewSource(1)),
	})
	if got := out.Assignment[0]; !reflect.DeepEqual(got, []string{"n1"}) {
		t.Fatalf("assignment = %v, want [n1]: 2*1 < 5 puts the incumbent out of the band", got)
	}
}

func TestComputeKeepsHeldNodeWhenSignalIsLow(t *testing.T) {
	// Cold start: every c_ij is zero, so the distribution says nothing and the
	// incumbent keeps the slot instead of the order falling back to node names.
	out := Compute(Input{
		Pods:  []PodState{testPod(0, map[string]int32{})},
		Nodes: testNodes("n0", "n1", "n2"),
		Held:  map[int32][]string{0: {"n2"}},
		Rand:  rand.New(rand.NewSource(1)),
	})
	if got := out.Assignment[0]; !reflect.DeepEqual(got, []string{"n2"}) {
		t.Fatalf("assignment = %v, want [n2] (LowSignal keeps the incumbent)", got)
	}
	if !out.Decisions[0].LowSignal {
		t.Fatal("LowSignal = false, want true at a zero readiness table")
	}
}

func TestComputeHeldNeverGrowsTheTargetSet(t *testing.T) {
	out := Compute(Input{
		Pods:   []PodState{testPod(0, map[string]int32{"n0": 8, "n1": 7, "n2": 6})},
		Nodes:  testNodes("n0", "n1", "n2"),
		Held:   map[int32][]string{0: {"n2"}},
		Fanout: 2,
		Rand:   rand.New(rand.NewSource(1)),
	})
	if got := out.Assignment[0]; !reflect.DeepEqual(got, []string{"n0", "n2"}) {
		t.Fatalf("assignment = %v, want [n0 n2]: n2 replaces the lowest non-held member", got)
	}
	if got := out.TargetSet[0]; !reflect.DeepEqual(got, []string{"n0", "n2"}) {
		t.Fatalf("target set = %v, want [n0 n2] in deterministic order", got)
	}
}

func TestComputeIgnoresHeldOnStoppedNode(t *testing.T) {
	nodes := testNodes("n0", "n1")
	nodes[1].Stopped = true
	out := Compute(Input{
		Pods:  []PodState{testPod(0, map[string]int32{"n0": 4, "n1": 5})},
		Nodes: nodes,
		Held:  map[int32][]string{0: {"n1"}},
		Rand:  rand.New(rand.NewSource(1)),
	})
	if got := out.Assignment[0]; !reflect.DeepEqual(got, []string{"n0"}) {
		t.Fatalf("assignment = %v, want [n0]: a stopped node cannot be kept in the set", got)
	}
}

func TestComputeHeldIsPerPod(t *testing.T) {
	ready := map[string]int32{"n0": 4, "n1": 5}
	out := Compute(Input{
		Pods: []PodState{
			testPod(0, ready),
			testPod(1, ready),
		},
		Nodes:                testNodes("n0", "n1"),
		Held:                 map[int32][]string{0: {"n0"}},
		AllowMultiPodPerNode: true,
		Rand:                 rand.New(rand.NewSource(1)),
	})
	if got := out.Assignment[0]; !reflect.DeepEqual(got, []string{"n0"}) {
		t.Fatalf("pod 0 assignment = %v, want [n0] (its own incumbent)", got)
	}
	if got := out.Assignment[1]; !reflect.DeepEqual(got, []string{"n1"}) {
		t.Fatalf("pod 1 assignment = %v, want [n1] (nothing held, strict best)", got)
	}
}

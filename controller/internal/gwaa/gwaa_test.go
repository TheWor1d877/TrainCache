package gwaa

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func testPod(index int32, ready map[string]int32) PodState {
	return PodState{Index: index, K: 8, ReadyCountByNode: ready}
}

func testNodes(names ...string) []NodeState {
	out := make([]NodeState, len(names))
	for i, name := range names {
		out[i] = NodeState{Name: name}
	}
	return out
}

func tieRichInput(seed int64) Input {
	return Input{
		Pods: []PodState{
			testPod(0, map[string]int32{"n0": 4, "n1": 4, "n2": 4}),
			testPod(1, map[string]int32{"n0": 4, "n1": 4, "n2": 4}),
			testPod(2, map[string]int32{"n0": 3, "n1": 3, "n2": 3}),
			testPod(3, map[string]int32{"n0": 2, "n1": 2, "n2": 2}),
		},
		Nodes: testNodes("n0", "n1", "n2"),
		Rand:  rand.New(rand.NewSource(seed)),
	}
}

func TestComputeFixedSeedReproducibleFanout2(t *testing.T) {
	in := tieRichInput(42)
	in.Fanout = 2
	in.AllowMultiPodPerNode = true
	want := Compute(in)
	if want.Err != nil {
		t.Fatalf("Compute returned error: %v", want.Err)
	}
	for i := 0; i < 100; i++ {
		in.Rand = rand.New(rand.NewSource(42))
		got := Compute(in)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("run %d differs from first run", i)
		}
	}
}

func TestComputeRandomSeedsProduceLegalPlacementsFanout2(t *testing.T) {
	var first string
	distinct := map[string]struct{}{}
	for seed := int64(0); seed < 64; seed++ {
		in := tieRichInput(seed)
		in.Fanout = 2
		in.AllowMultiPodPerNode = true
		out := Compute(in)
		if out.Err != nil {
			t.Fatalf("seed %d: %v", seed, out.Err)
		}
		if len(out.UnassignedPods) != 0 {
			t.Fatalf("seed %d: unassigned = %v, want none", seed, out.UnassignedPods)
		}
		for index, nodes := range out.Assignment {
			if len(nodes) != 2 {
				t.Fatalf("seed %d: pod %d got %v, want 2 nodes", seed, index, nodes)
			}
		}
		assertPrewarmCoversAssignment(t, out)
		key := assignmentKey(out)
		distinct[key] = struct{}{}
		if first == "" {
			first = key
		}
	}
	if len(distinct) < 2 {
		t.Fatalf("random tie-breaking produced only one placement: %s", first)
	}
}

func TestComputeFanoutTwoPicksTopKDescending(t *testing.T) {
	in := Input{
		Pods: []PodState{
			testPod(0, map[string]int32{"n0": 8, "n1": 6, "n2": 1, "n3": 0}),
			testPod(1, map[string]int32{"n0": 0, "n1": 1, "n2": 6, "n3": 8}),
		},
		Nodes:  testNodes("n0", "n1", "n2", "n3"),
		Rand:   rand.New(rand.NewSource(4)),
		Fanout: 2,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	want := map[int32][]string{
		0: {"n0", "n1"},
		1: {"n3", "n2"},
	}
	if !reflect.DeepEqual(out.Assignment, want) {
		t.Fatalf("assignment = %v, want %v", out.Assignment, want)
	}
	assertPrewarmCoversAssignment(t, out)
}

func TestComputeFanoutCappedByFeasibleNodes(t *testing.T) {
	// The minimum asks for 3 nodes but only 2 are feasible: target and
	// assignment clamp to 2 and the decision reports MinUnmet.
	in := Input{
		Pods:      []PodState{testPod(0, map[string]int32{"n0": 5, "n1": 2})},
		Nodes:     testNodes("n0", "n1"),
		Rand:      rand.New(rand.NewSource(0)),
		Fanout:    3,
		FanoutMin: 3,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if got := out.Assignment[0]; !reflect.DeepEqual(got, []string{"n0", "n1"}) {
		t.Fatalf("assignment = %v, want both feasible nodes", got)
	}
	if len(out.UnassignedPods) != 0 {
		t.Fatalf("unassigned = %v, want none", out.UnassignedPods)
	}
	decision := out.Decisions[0]
	if decision.TargetFanout != 2 || !decision.MinUnmet {
		t.Fatalf("decision = %+v, want TargetFanout=2 MinUnmet=true", decision)
	}
}

// TestComputeMinUnmetFollowsFinalAssignment pins the M14 rule that MinUnmet
// describes the actual claims, not the planned target. Two Pods with the same
// 11,7,3 distribution both target two nodes, but one-pod-per-node placement
// leaves at least one of them below min=2 after conflicts and fallback.
func TestComputeMinUnmetFollowsFinalAssignment(t *testing.T) {
	ready := map[string]int32{"n0": 11, "n1": 7, "n2": 3}
	in := Input{
		Pods: []PodState{
			{Index: 0, K: 16, ReadyCountByNode: ready},
			{Index: 1, K: 16, ReadyCountByNode: ready},
		},
		Nodes:                testNodes("n0", "n1", "n2"),
		Rand:                 rand.New(rand.NewSource(3)),
		Fanout:               2,
		FanoutMin:            2,
		AllowMultiPodPerNode: false,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}

	below := 0
	for pod := int32(0); pod < 2; pod++ {
		if got := len(out.TargetSet[pod]); got != 2 {
			t.Fatalf("pod %d target count = %d, want 2: %#v", pod, got, out.TargetSet)
		}
		actual := len(out.Assignment[pod])
		if actual > int(in.Fanout) {
			t.Fatalf("pod %d actual = %d, exceeds fanout %d", pod, actual, in.Fanout)
		}
		if actual < int(in.FanoutMin) {
			below++
			if !out.Decisions[pod].MinUnmet {
				t.Fatalf("pod %d actual = %d < min, MinUnmet = false: %#v",
					pod, actual, out.Decisions[pod])
			}
		} else if out.Decisions[pod].MinUnmet {
			t.Fatalf("pod %d actual = %d >= min, MinUnmet = true: %#v",
				pod, actual, out.Decisions[pod])
		}
	}
	if below == 0 {
		t.Fatalf("no Pod fell below min: assignment=%#v target=%#v",
			out.Assignment, out.TargetSet)
	}
	// The loser either stays inside its two-node target with one claim or takes
	// exactly one out-of-target fallback node.
	assertAssignmentSubsetOfTarget(t, out)
	assertPrewarmCoversAssignment(t, out)
}

// TestComputeMinMetStaysFalse confirms a normal two-node assignment keeps
// MinUnmet false when the lower bound is met.
func TestComputeMinMetStaysFalse(t *testing.T) {
	in := Input{
		Pods:      []PodState{testPod(0, map[string]int32{"n0": 11, "n1": 7, "n2": 3})},
		Nodes:     testNodes("n0", "n1", "n2"),
		Rand:      rand.New(rand.NewSource(3)),
		Fanout:    2,
		FanoutMin: 2,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if got := len(out.Assignment[0]); got != 2 {
		t.Fatalf("assignment count = %d, want 2: %#v", got, out.Assignment)
	}
	if out.Decisions[0].MinUnmet {
		t.Fatalf("MinUnmet = true, want false: %#v", out.Decisions[0])
	}
}

// fanoutEqualInput is the 2 Pod x 4 node scenario that exposed the top-K
// underfill: every c_ij is equal, so every conflict is a tie decided by the
// injected seed. Feasible nodes (4) exceed Fanout (2), so every Pod must end
// up with exactly 2 nodes regardless of tie order.
func fanoutEqualInput(seed int64, multi bool, anti string) Input {
	ready := map[string]int32{"n0": 4, "n1": 4, "n2": 4, "n3": 4}
	return Input{
		Pods: []PodState{
			{Index: 0, K: 8, ReadyCountByNode: ready, AntiAffinityGroup: anti},
			{Index: 1, K: 8, ReadyCountByNode: ready, AntiAffinityGroup: anti},
		},
		Nodes:                testNodes("n0", "n1", "n2", "n3"),
		Rand:                 rand.New(rand.NewSource(seed)),
		Fanout:               2,
		AllowMultiPodPerNode: multi,
	}
}

func TestComputeFanoutFillsEveryPodAcrossSeeds(t *testing.T) {
	variants := []struct {
		name  string
		multi bool
		anti  string
	}{
		{name: "one-pod-per-node", multi: false},
		{name: "allow-multi", multi: true},
		{name: "allow-multi-shared-anti", multi: true, anti: "a"},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			for seed := int64(0); seed < 200; seed++ {
				in := fanoutEqualInput(seed, v.multi, v.anti)
				out := Compute(in)
				if out.Err != nil {
					t.Fatalf("seed %d: %v", seed, out.Err)
				}
				if len(out.UnassignedPods) != 0 {
					t.Fatalf("seed %d: unassigned = %v, want none", seed, out.UnassignedPods)
				}
				for _, pod := range in.Pods {
					nodes := out.Assignment[pod.Index]
					target := out.Decisions[pod.Index].TargetFanout
					if target != 2 {
						t.Fatalf("seed %d: pod %d target = %d, want 2", seed, pod.Index, target)
					}
					if len(nodes) == 0 || int32(len(nodes)) > target {
						t.Fatalf("seed %d: pod %d got %v, want 1..%d nodes", seed, pod.Index, nodes, target)
					}
					// Without anti-affinity both units can share the same target
					// set, so every Pod fills it. One-pod-per-node or anti-affinity
					// makes the second unit retreat to a single out-of-target
					// fallback, which M14 permits.
					if v.multi && v.anti == "" && int32(len(nodes)) != target {
						t.Fatalf("seed %d: pod %d got %v, want %d nodes", seed, pod.Index, nodes, target)
					}
				}
				if v.anti != "" {
					p0 := nodeSet(out.Assignment[0])
					for _, node := range out.Assignment[1] {
						if _, shared := p0[node]; shared {
							t.Fatalf("seed %d: anti-affinity Pods share node %q: %v / %v",
								seed, node, out.Assignment[0], out.Assignment[1])
						}
					}
				}
				assertPrewarmCoversAssignment(t, out)
				assertAssignmentSubsetOfTarget(t, out)
			}
		})
	}
}

// TestComputeFanoutOneFillsEveryPodAcrossSeeds covers the K=1 form of the
// same early-convergence defect: 2 Pods, 4 equal feasible nodes, so every Pod
// must receive exactly one node for every tie order.
func TestComputeFanoutOneFillsEveryPodAcrossSeeds(t *testing.T) {
	variants := []struct {
		name  string
		multi bool
	}{
		{name: "one-pod-per-node", multi: false},
		{name: "allow-multi", multi: true},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			for seed := int64(0); seed < 200; seed++ {
				in := fanoutEqualInput(seed, v.multi, "")
				in.Fanout = 1
				out := Compute(in)
				if out.Err != nil {
					t.Fatalf("seed %d: %v", seed, out.Err)
				}
				if len(out.UnassignedPods) != 0 {
					t.Fatalf("seed %d: unassigned = %v, want none", seed, out.UnassignedPods)
				}
				for _, pod := range in.Pods {
					nodes := out.Assignment[pod.Index]
					if len(nodes) != 1 {
						t.Fatalf("seed %d: pod %d got %v, want 1 node", seed, pod.Index, nodes)
					}
				}
				if !v.multi {
					assertOnePodPerNode(t, out)
				}
				assertPrewarmCoversAssignment(t, out)
			}
		})
	}
}

func TestComputeFanoutUsesAllFeasibleNodesWhenFewerThanFanout(t *testing.T) {
	// Two of the four candidates have stopped Agents, leaving 2 feasible
	// nodes for Fanout=3. Both Pods share nodes (allowMulti), so each must
	// still receive both feasible nodes.
	nodes := []NodeState{
		{Name: "n0"},
		{Name: "n1"},
		{Name: "n2", Stopped: true},
		{Name: "n3", Stopped: true},
	}
	ready := map[string]int32{"n0": 6, "n1": 2}
	in := Input{
		Pods: []PodState{
			{Index: 0, K: 8, ReadyCountByNode: ready},
			{Index: 1, K: 8, ReadyCountByNode: ready},
		},
		Nodes:                nodes,
		Fanout:               3,
		FanoutMin:            2,
		AllowMultiPodPerNode: true,
	}
	want := []string{"n0", "n1"}
	for seed := int64(0); seed < 200; seed++ {
		in.Rand = rand.New(rand.NewSource(seed))
		out := Compute(in)
		if out.Err != nil {
			t.Fatalf("seed %d: %v", seed, out.Err)
		}
		if len(out.UnassignedPods) != 0 {
			t.Fatalf("seed %d: unassigned = %v, want none", seed, out.UnassignedPods)
		}
		for index := int32(0); index < 2; index++ {
			if got := out.Assignment[index]; !reflect.DeepEqual(got, want) {
				t.Fatalf("seed %d: pod %d got %v, want %v", seed, index, got, want)
			}
		}
		assertPrewarmCoversAssignment(t, out)
	}
}

func feasibleNodeCount(nodes []NodeState) int {
	count := 0
	for _, node := range nodes {
		if !node.Stopped {
			count++
		}
	}
	return count
}

func TestComputeFanoutZeroBehavesAsOne(t *testing.T) {
	zero := tieRichInput(9)
	zero.Fanout = 0
	one := tieRichInput(9)
	one.Fanout = 1
	got := Compute(zero)
	want := Compute(one)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fanout=0 result %v differs from fanout=1 result %v", got, want)
	}
	for index, nodes := range got.Assignment {
		if len(nodes) != 1 {
			t.Fatalf("pod %d got %v, want exactly one node", index, nodes)
		}
	}
}

func TestComputeFanoutOneRegression(t *testing.T) {
	// The M12 placement set, one node per Pod: two nodes, one Pod per node.
	in := Input{
		Pods: []PodState{
			testPod(0, map[string]int32{"n0": 5, "n1": 1}),
			testPod(1, map[string]int32{"n0": 4, "n1": 3}),
		},
		Nodes:  testNodes("n0", "n1"),
		Rand:   rand.New(rand.NewSource(11)),
		Fanout: 1,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if !reflect.DeepEqual(out.Assignment[0], []string{"n0"}) {
		t.Fatalf("pod 0 = %v, want [n0]", out.Assignment[0])
	}
	if !reflect.DeepEqual(out.Assignment[1], []string{"n1"}) {
		t.Fatalf("pod 1 = %v, want [n1]", out.Assignment[1])
	}
	assertPrewarmCoversAssignment(t, out)
}

func TestComputeAffinityGroupSharesAllNodes(t *testing.T) {
	in := Input{
		Pods: []PodState{
			{Index: 0, K: 8, ReadyCountByNode: map[string]int32{"n0": 5, "n1": 1}, AffinityGroup: "g"},
			{Index: 1, K: 8, ReadyCountByNode: map[string]int32{"n0": 2, "n1": 4}, AffinityGroup: "g"},
			testPod(2, map[string]int32{"n0": 0, "n1": 6, "n2": 0}),
		},
		Nodes:                testNodes("n0", "n1", "n2"),
		Rand:                 rand.New(rand.NewSource(7)),
		Fanout:               2,
		AllowMultiPodPerNode: true,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	group := out.Assignment[0]
	if len(group) != 2 {
		t.Fatalf("affinity group got %v, want 2 nodes", group)
	}
	if !reflect.DeepEqual(group, out.Assignment[1]) {
		t.Fatalf("affinity Pods got different node lists: %v vs %v", group, out.Assignment[1])
	}
	assertPrewarmCoversAssignment(t, out)
}

func TestComputeAntiAffinitySeparatesFanoutNodes(t *testing.T) {
	in := Input{
		Pods: []PodState{
			{Index: 0, K: 8, ReadyCountByNode: map[string]int32{"n0": 5, "n1": 4, "n2": 3, "n3": 2}, AntiAffinityGroup: "a"},
			// The second Pod's cache is concentrated on the other two nodes, so
			// the M14 target sets are disjoint and both Pods can fill them.
			{Index: 1, K: 8, ReadyCountByNode: map[string]int32{"n0": 3, "n1": 2, "n2": 5, "n3": 4}, AntiAffinityGroup: "a"},
		},
		Nodes:                testNodes("n0", "n1", "n2", "n3"),
		Rand:                 rand.New(rand.NewSource(3)),
		Fanout:               2,
		AllowMultiPodPerNode: true,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if len(out.Assignment) != 2 {
		t.Fatalf("assignment = %v, want both Pods placed", out.Assignment)
	}
	p0 := nodeSet(out.Assignment[0])
	for _, node := range out.Assignment[1] {
		if _, shared := p0[node]; shared {
			t.Fatalf("anti-affinity Pods share node %q: %v / %v", node, out.Assignment[0], out.Assignment[1])
		}
	}
	if len(out.Assignment[0]) != 2 || len(out.Assignment[1]) != 2 {
		t.Fatalf("fanout not filled: %v", out.Assignment)
	}
}

func TestComputeOnePodPerNode(t *testing.T) {
	in := Input{
		Pods: []PodState{
			testPod(0, map[string]int32{"n0": 5, "n1": 4, "n2": 3}),
			testPod(1, map[string]int32{"n0": 5, "n1": 4, "n2": 3}),
			testPod(2, map[string]int32{"n0": 5, "n1": 4, "n2": 3}),
		},
		Nodes: testNodes("n0", "n1", "n2"),
		Rand:  rand.New(rand.NewSource(11)),
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if len(out.Assignment) != 3 {
		t.Fatalf("assignment = %v, want 3 Pods", out.Assignment)
	}
	assertOnePodPerNode(t, out)
	for index, nodes := range out.Assignment {
		if len(nodes) != 1 {
			t.Fatalf("pod %d got %v, want one node", index, nodes)
		}
	}
}

func TestComputeMultiPodPerNode(t *testing.T) {
	in := Input{
		Pods: []PodState{
			testPod(0, map[string]int32{"n0": 5, "n1": 0}),
			testPod(1, map[string]int32{"n0": 5, "n1": 0}),
			testPod(2, map[string]int32{"n0": 5, "n1": 0}),
		},
		Nodes:                testNodes("n0", "n1"),
		Rand:                 rand.New(rand.NewSource(5)),
		AllowMultiPodPerNode: true,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if len(out.Assignment) != 3 {
		t.Fatalf("assignment = %v, want 3 Pods", out.Assignment)
	}
	for index, nodes := range out.Assignment {
		if len(nodes) != 1 || nodes[0] != "n0" {
			t.Fatalf("pod %d placed on %v, want n0", index, nodes)
		}
	}
	if got := len(out.PreheatSet["n0"]); got != 3 {
		t.Fatalf("preheatSet[n0] has %d Pods, want 3", got)
	}
}

func TestComputeTieUsesInjectedRNG(t *testing.T) {
	in := Input{
		Pods:   []PodState{testPod(0, map[string]int32{"n0": 3, "n1": 3})},
		Nodes:  testNodes("n0", "n1"),
		Fanout: 2, // both tied nodes enter the target set, so their claim order is random
	}
	seen := map[string]struct{}{}
	for seed := int64(0); seed < 32; seed++ {
		in.Rand = rand.New(rand.NewSource(seed))
		first := Compute(in)
		in.Rand = rand.New(rand.NewSource(seed))
		second := Compute(in)
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("seed %d is not reproducible", seed)
		}
		seen[first.Assignment[0][0]] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("tie always resolved to %v across seeds", seen)
	}
}

func TestComputeConflictKeepsLargerDependency(t *testing.T) {
	in := Input{
		Pods: []PodState{
			// p0 prefers n0 by only 1, p1 depends on n0 by 9.
			testPod(0, map[string]int32{"n0": 10, "n1": 9}),
			testPod(1, map[string]int32{"n0": 9, "n1": 0}),
		},
		Nodes: testNodes("n0", "n1"),
		Rand:  rand.New(rand.NewSource(1)),
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if !reflect.DeepEqual(out.Assignment[1], []string{"n0"}) || !reflect.DeepEqual(out.Assignment[0], []string{"n1"}) {
		t.Fatalf("conflict adjudication = %v, want p1=n0 and p0=n1", out.Assignment)
	}
}

func TestComputeFanoutConflictYieldsToNextNode(t *testing.T) {
	in := Input{
		Pods: []PodState{
			// p0 leads n0 by only 1; p1 depends on n0 by 9.
			testPod(0, map[string]int32{"n0": 10, "n1": 9, "n2": 0, "n3": 0}),
			testPod(1, map[string]int32{"n0": 9, "n1": 0, "n2": 0, "n3": 0}),
		},
		Nodes:  testNodes("n0", "n1", "n2", "n3"),
		Rand:   rand.New(rand.NewSource(2)),
		Fanout: 2,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	// Pod 1's cache is concentrated on n0, so its M14 target is just n0.
	p1 := out.Assignment[1]
	if !reflect.DeepEqual(p1, []string{"n0"}) {
		t.Fatalf("winner pod 1 = %v, want [n0]", p1)
	}
	if got := out.Decisions[1].TargetFanout; got != 1 {
		t.Fatalf("pod 1 target = %d, want 1", got)
	}
	// Pod 0 yields n0 and retreats to n1, the next node inside its target set.
	// n2/n3 are outside that set, so M14 does not top up into them.
	if !reflect.DeepEqual(out.Assignment[0], []string{"n1"}) {
		t.Fatalf("yielding pod 0 = %v, want [n1]", out.Assignment[0])
	}
	assertPrewarmCoversAssignment(t, out)
}

func TestComputeNonConvergenceFallsBack(t *testing.T) {
	in := Input{
		Pods: []PodState{
			// Both targets contain n0. Pod 0 keeps n0 by the dependency rule
			// (9 vs 1), while Pod 1 still has n2 to claim, so the probe round
			// keeps moving and the round limit is reported.
			testPod(0, map[string]int32{"n0": 10, "n1": 1}),
			testPod(1, map[string]int32{"n0": 9, "n2": 8}),
		},
		Nodes:     testNodes("n0", "n1", "n2"),
		Rand:      rand.New(rand.NewSource(2)),
		Fanout:    2,
		MaxRounds: 1,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if !out.FellBack {
		t.Fatalf("FellBack = false, want true after round limit")
	}
	if out.Rounds != 1 {
		t.Fatalf("Rounds = %d, want 1", out.Rounds)
	}
	if len(out.Assignment) != 2 || out.Assignment[0][0] == out.Assignment[1][0] {
		t.Fatalf("fallback assignment is not the first greedy result: %v", out.Assignment)
	}

	// With the documented limit (len(Pods)) the same input converges and is
	// not reported as a fallback.
	in.MaxRounds = 0
	converged := Compute(in)
	if converged.FellBack {
		t.Fatalf("converged run reported FellBack")
	}
	if !reflect.DeepEqual(out.Assignment, converged.Assignment) {
		t.Fatalf("fallback %v differs from converged %v", out.Assignment, converged.Assignment)
	}
}

func TestComputeNoFeasibleNodeIsUnassigned(t *testing.T) {
	in := Input{
		Pods:   []PodState{testPod(0, map[string]int32{"n0": 3}), testPod(1, nil)},
		Nodes:  nil,
		Rand:   rand.New(rand.NewSource(0)),
		Fanout: 2,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if len(out.Assignment) != 0 {
		t.Fatalf("assignment = %v, want empty", out.Assignment)
	}
	want := []int32{0, 1}
	if !reflect.DeepEqual(out.UnassignedPods, want) {
		t.Fatalf("unassigned = %v, want %v", out.UnassignedPods, want)
	}
	for _, index := range want {
		if len(out.TargetSet[index]) != 0 {
			t.Fatalf("pod %d target = %v, want empty", index, out.TargetSet[index])
		}
		if !out.Decisions[index].MinUnmet {
			t.Fatalf("pod %d decision = %+v, want MinUnmet", index, out.Decisions[index])
		}
	}

	stopped := Input{
		Pods:   []PodState{testPod(0, map[string]int32{"n0": 3})},
		Nodes:  []NodeState{{Name: "n0", Stopped: true}},
		Rand:   rand.New(rand.NewSource(0)),
		Fanout: 2,
	}
	got := Compute(stopped)
	if len(got.Assignment) != 0 || !reflect.DeepEqual(got.UnassignedPods, []int32{0}) {
		t.Fatalf("stopped node: assignment=%v unassigned=%v", got.Assignment, got.UnassignedPods)
	}
}

func TestComputeOnlyNoFeasibleUnitIsUnassigned(t *testing.T) {
	in := Input{
		Pods: []PodState{
			testPod(0, map[string]int32{"n0": 3, "n1": 2, "n2": 1}),
			// A merged affinity group whose members also share an anti-affinity
			// group contradicts itself and has no feasible node.
			{Index: 1, K: 8, ReadyCountByNode: map[string]int32{"n0": 1}, AffinityGroup: "g", AntiAffinityGroup: "a"},
			{Index: 2, K: 8, ReadyCountByNode: map[string]int32{"n0": 1}, AffinityGroup: "g", AntiAffinityGroup: "a"},
		},
		Nodes:  testNodes("n0", "n1", "n2"),
		Rand:   rand.New(rand.NewSource(0)),
		Fanout: 2,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if len(out.Assignment[0]) != 2 {
		t.Fatalf("feasible pod got %v, want 2 nodes", out.Assignment[0])
	}
	if !reflect.DeepEqual(out.UnassignedPods, []int32{1, 2}) {
		t.Fatalf("unassigned = %v, want [1 2]", out.UnassignedPods)
	}
	if _, ok := out.Assignment[1]; ok {
		t.Fatalf("contradictory unit was assigned: %v", out.Assignment)
	}
}

func TestComputeStoppedNodeExcluded(t *testing.T) {
	in := Input{
		Pods: []PodState{testPod(0, map[string]int32{"n0": 99, "n1": 1})},
		Nodes: []NodeState{
			{Name: "n0", Stopped: true},
			{Name: "n1"},
		},
		Rand: rand.New(rand.NewSource(0)),
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if !reflect.DeepEqual(out.Assignment[0], []string{"n1"}) {
		t.Fatalf("pod assigned to %v, want [n1]", out.Assignment[0])
	}
}

func TestComputeAffinityAntiAffinityContradiction(t *testing.T) {
	in := Input{
		Pods: []PodState{
			{Index: 0, K: 8, ReadyCountByNode: map[string]int32{"n0": 1}, AffinityGroup: "g", AntiAffinityGroup: "a"},
			{Index: 1, K: 8, ReadyCountByNode: map[string]int32{"n0": 1}, AffinityGroup: "g", AntiAffinityGroup: "a"},
		},
		Nodes: testNodes("n0"),
		Rand:  rand.New(rand.NewSource(0)),
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if len(out.Assignment) != 0 || !reflect.DeepEqual(out.UnassignedPods, []int32{0, 1}) {
		t.Fatalf("contradiction: assignment=%v unassigned=%v", out.Assignment, out.UnassignedPods)
	}
}

func TestComputeTooFewNodesLeavesUnassigned(t *testing.T) {
	in := Input{
		Pods: []PodState{
			testPod(0, map[string]int32{"n0": 2}),
			testPod(1, map[string]int32{"n0": 1}),
		},
		Nodes: testNodes("n0"),
		Rand:  rand.New(rand.NewSource(0)),
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if len(out.Assignment) != 1 || len(out.UnassignedPods) != 1 {
		t.Fatalf("assignment=%v unassigned=%v", out.Assignment, out.UnassignedPods)
	}
}

func TestValidate(t *testing.T) {
	tooManyPods := Input{Pods: make([]PodState, MaxPods+1), Nodes: testNodes("n0")}
	if err := Validate(tooManyPods); err == nil {
		t.Fatal("Validate accepted too many Pods")
	}
	tooManyNodes := make([]NodeState, MaxNodes+1)
	for i := range tooManyNodes {
		tooManyNodes[i] = NodeState{Name: string(rune('a' + i%26))}
	}
	if err := Validate(Input{Pods: []PodState{testPod(0, nil)}, Nodes: tooManyNodes}); err == nil {
		t.Fatal("Validate accepted too many Nodes")
	}
	if err := Validate(Input{Pods: []PodState{{Index: 0, K: -1}}, Nodes: testNodes("n0")}); err == nil {
		t.Fatal("Validate accepted negative K")
	}
	if err := Validate(Input{Pods: []PodState{testPod(0, nil)}, Nodes: testNodes("n0"), Fanout: -1}); err == nil {
		t.Fatal("Validate accepted negative Fanout")
	}
	if err := Validate(Input{Pods: []PodState{testPod(0, nil), testPod(0, nil)}, Nodes: testNodes("n0")}); err == nil {
		t.Fatal("Validate accepted duplicate Pod indices")
	}
	if err := Validate(Input{Pods: []PodState{testPod(0, nil)}, Nodes: []NodeState{{Name: "n0"}, {Name: "n0"}}}); err == nil {
		t.Fatal("Validate accepted duplicate Node names")
	}
	if err := Validate(Input{Pods: []PodState{testPod(0, map[string]int32{"n0": -1})}, Nodes: testNodes("n0")}); err == nil {
		t.Fatal("Validate accepted negative readiness")
	}
	out := Compute(Input{Pods: make([]PodState, MaxPods+1), Nodes: testNodes("n0")})
	if out.Err == nil {
		t.Fatal("Compute did not surface validation error")
	}
}

// TestComputeAdaptiveTargetFanoutFromDistribution pins the M14 target-count
// examples from docs/7.2 D39.
func TestComputeAdaptiveTargetFanoutFromDistribution(t *testing.T) {
	five := []string{"n0", "n1", "n2", "n3", "n4"}
	three := []string{"n0", "n1", "n2"}
	two := []string{"n0", "n1"}
	cases := []struct {
		name       string
		ready      map[string]int32
		nodes      []string
		fanout     int32
		fanoutMin  int32
		k          int32
		wantTarget int32
		wantSet    []string
		low        bool
		forced     bool
		minUnmet   bool
	}{
		{
			name: "12,1,0,0,0 min=1", ready: map[string]int32{"n0": 12, "n1": 1}, nodes: five,
			fanout: 3, fanoutMin: 1, k: 16, wantTarget: 1, wantSet: []string{"n0"},
		},
		{
			name: "12,1,0,0,0 min=2", ready: map[string]int32{"n0": 12, "n1": 1}, nodes: five,
			fanout: 3, fanoutMin: 2, k: 16, wantTarget: 2, wantSet: []string{"n0", "n1"},
			forced: true,
		},
		{
			name: "12,1,0,0,0 min=3", ready: map[string]int32{"n0": 12, "n1": 1}, nodes: five,
			fanout: 3, fanoutMin: 3, k: 16, wantTarget: 3, wantSet: []string{"n0", "n1", "n2"},
			forced: true,
		},
		{
			name: "low signal 1,1,0,0,0", ready: map[string]int32{"n0": 1, "n1": 1}, nodes: five,
			fanout: 3, fanoutMin: 1, k: 16, wantTarget: 3, wantSet: []string{"n0", "n1", "n2"},
			low: true,
		},
		{
			name: "11,7,3 keeps two", ready: map[string]int32{"n0": 11, "n1": 7, "n2": 3}, nodes: three,
			fanout: 3, fanoutMin: 1, k: 16, wantTarget: 2, wantSet: []string{"n0", "n1"},
		},
		{
			name: "all zero is low signal", ready: nil, nodes: five,
			fanout: 3, fanoutMin: 1, k: 16, wantTarget: 3, wantSet: []string{"n0", "n1", "n2"},
			low: true,
		},
		{
			name: "fewer feasible than min", ready: map[string]int32{"n0": 10, "n1": 5}, nodes: two,
			fanout: 3, fanoutMin: 3, k: 16, wantTarget: 2, wantSet: []string{"n0", "n1"},
			forced: true, minUnmet: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := Input{
				Pods:      []PodState{{Index: 0, K: tc.k, ReadyCountByNode: tc.ready}},
				Nodes:     testNodes(tc.nodes...),
				Rand:      rand.New(rand.NewSource(1)),
				Fanout:    tc.fanout,
				FanoutMin: tc.fanoutMin,
			}
			out := Compute(in)
			if out.Err != nil {
				t.Fatalf("Compute returned error: %v", out.Err)
			}
			decision := out.Decisions[0]
			if decision.TargetFanout != tc.wantTarget {
				t.Fatalf("TargetFanout = %d, want %d", decision.TargetFanout, tc.wantTarget)
			}
			if !reflect.DeepEqual(out.TargetSet[0], tc.wantSet) {
				t.Fatalf("TargetSet = %v, want %v", out.TargetSet[0], tc.wantSet)
			}
			if decision.LowSignal != tc.low {
				t.Fatalf("LowSignal = %v, want %v", decision.LowSignal, tc.low)
			}
			if decision.ForcedByMin != tc.forced {
				t.Fatalf("ForcedByMin = %v, want %v", decision.ForcedByMin, tc.forced)
			}
			if decision.MinUnmet != tc.minUnmet {
				t.Fatalf("MinUnmet = %v, want %v", decision.MinUnmet, tc.minUnmet)
			}
			assertAssignmentSubsetOfTarget(t, out)
		})
	}
}

// TestComputeTargetSetOrderIsDeterministic checks that TargetSet ignores input
// node order and RNG: c_ij ties break by node name ascending.
func TestComputeTargetSetOrderIsDeterministic(t *testing.T) {
	ready := map[string]int32{"n0": 4, "n1": 4, "n2": 4}
	want := []string{"n0", "n1", "n2"}
	for seed := int64(0); seed < 32; seed++ {
		in := Input{
			Pods:   []PodState{{Index: 0, K: 8, ReadyCountByNode: ready}},
			Nodes:  testNodes("n2", "n0", "n1"),
			Rand:   rand.New(rand.NewSource(seed)),
			Fanout: 3,
		}
		out := Compute(in)
		if out.Err != nil {
			t.Fatalf("seed %d: %v", seed, out.Err)
		}
		if !reflect.DeepEqual(out.TargetSet[0], want) {
			t.Fatalf("seed %d: TargetSet = %v, want %v", seed, out.TargetSet[0], want)
		}
		if out.Decisions[0].TargetFanout != 3 {
			t.Fatalf("seed %d: target = %d, want 3", seed, out.Decisions[0].TargetFanout)
		}
	}
}

// TestComputeFallbackOutsideTarget forces a one-node target for both Pods on
// one-pod-per-node placement: the loser cannot claim a target node, so M14
// allows exactly one out-of-target fallback and flags it.
func TestComputeFallbackOutsideTarget(t *testing.T) {
	in := Input{
		Pods: []PodState{
			{Index: 0, K: 16, ReadyCountByNode: map[string]int32{"n0": 10, "n1": 1}},
			{Index: 1, K: 16, ReadyCountByNode: map[string]int32{"n0": 9, "n1": 1}},
		},
		Nodes:  testNodes("n0", "n1"),
		Rand:   rand.New(rand.NewSource(5)),
		Fanout: 1,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if !reflect.DeepEqual(out.TargetSet[0], []string{"n0"}) || !reflect.DeepEqual(out.TargetSet[1], []string{"n0"}) {
		t.Fatalf("target sets = %v, want both [n0]", out.TargetSet)
	}
	if !reflect.DeepEqual(out.Assignment[0], []string{"n0"}) {
		t.Fatalf("pod 0 = %v, want [n0]", out.Assignment[0])
	}
	if !reflect.DeepEqual(out.Assignment[1], []string{"n1"}) {
		t.Fatalf("pod 1 = %v, want fallback [n1]", out.Assignment[1])
	}
	if out.Decisions[0].FallbackOutOfTarget || !out.Decisions[1].FallbackOutOfTarget {
		t.Fatalf("fallback flags = %+v / %+v, want false / true", out.Decisions[0], out.Decisions[1])
	}
	assertPrewarmCoversAssignment(t, out)
	assertAssignmentSubsetOfTarget(t, out)
}

// TestComputeTopUpStopsAtTarget keeps Fanout at 4 but the adaptive target at 2.
// After losing n0, pod 0 must stop at its remaining target node n1 and never
// top up to n2/n3 just because the fanout ceiling allows them.
func TestComputeTopUpStopsAtTarget(t *testing.T) {
	in := Input{
		Pods: []PodState{
			{Index: 0, K: 16, ReadyCountByNode: map[string]int32{"n0": 10, "n1": 9}},
			{Index: 1, K: 16, ReadyCountByNode: map[string]int32{"n0": 11}},
		},
		Nodes:  testNodes("n0", "n1", "n2", "n3"),
		Rand:   rand.New(rand.NewSource(2)),
		Fanout: 4,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if !reflect.DeepEqual(out.TargetSet[0], []string{"n0", "n1"}) {
		t.Fatalf("pod 0 target = %v, want [n0 n1]", out.TargetSet[0])
	}
	if got := out.Decisions[0].TargetFanout; got != 2 {
		t.Fatalf("pod 0 target count = %d, want 2", got)
	}
	if !reflect.DeepEqual(out.Assignment[0], []string{"n1"}) {
		t.Fatalf("pod 0 = %v, want [n1] only", out.Assignment[0])
	}
	if !reflect.DeepEqual(out.Assignment[1], []string{"n0"}) {
		t.Fatalf("pod 1 = %v, want [n0]", out.Assignment[1])
	}
	assertAssignmentSubsetOfTarget(t, out)
}

// TestComputeAffinityUnitSharesTargetAndDecision checks the merged-unit rule:
// one target set and one decision for every member.
func TestComputeAffinityUnitSharesTargetAndDecision(t *testing.T) {
	in := Input{
		Pods: []PodState{
			{Index: 0, K: 16, ReadyCountByNode: map[string]int32{"n0": 10, "n1": 9}, AffinityGroup: "g"},
			{Index: 1, K: 16, ReadyCountByNode: map[string]int32{"n0": 10, "n1": 9}, AffinityGroup: "g"},
		},
		Nodes:  testNodes("n0", "n1", "n2"),
		Rand:   rand.New(rand.NewSource(9)),
		Fanout: 3,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if !reflect.DeepEqual(out.TargetSet[0], []string{"n0", "n1"}) || !reflect.DeepEqual(out.TargetSet[0], out.TargetSet[1]) {
		t.Fatalf("target sets = %v / %v, want both [n0 n1]", out.TargetSet[0], out.TargetSet[1])
	}
	if out.Decisions[0] != out.Decisions[1] || out.Decisions[0].TargetFanout != 2 {
		t.Fatalf("decisions = %+v / %+v, want equal TargetFanout=2", out.Decisions[0], out.Decisions[1])
	}
	if !reflect.DeepEqual(out.Assignment[0], out.Assignment[1]) || !reflect.DeepEqual(out.Assignment[0], []string{"n0", "n1"}) {
		t.Fatalf("assignments = %v / %v, want both [n0 n1]", out.Assignment[0], out.Assignment[1])
	}
}

func TestComputeFanoutMinAboveFanoutFails(t *testing.T) {
	in := Input{
		Pods:      []PodState{testPod(0, map[string]int32{"n0": 4})},
		Nodes:     testNodes("n0"),
		Fanout:    2,
		FanoutMin: 3,
	}
	if err := Validate(in); err == nil {
		t.Fatal("Validate accepted FanoutMin > Fanout")
	}
	if out := Compute(in); out.Err == nil {
		t.Fatal("Compute accepted FanoutMin > Fanout")
	}

	// Zero means 1 after normalization, so a minimum of 2 against Fanout=0 is
	// still a conflict.
	in.Fanout = 0
	in.FanoutMin = 2
	if out := Compute(in); out.Err == nil {
		t.Fatal("Compute accepted normalized FanoutMin > normalized Fanout")
	}
}

func assertAssignmentSubsetOfTarget(t *testing.T, out Output) {
	t.Helper()
	for index, assigned := range out.Assignment {
		decision, ok := out.Decisions[index]
		if !ok {
			t.Fatalf("pod %d has assignment %v but no decision", index, assigned)
		}
		target := nodeSet(out.TargetSet[index])
		for _, node := range assigned {
			if _, inTarget := target[node]; inTarget {
				continue
			}
			if !decision.FallbackOutOfTarget {
				t.Fatalf("pod %d assigned node %q outside target %v without fallback flag", index, node, out.TargetSet[index])
			}
		}
		if decision.FallbackOutOfTarget && len(assigned) != 1 {
			t.Fatalf("pod %d fallback assignment = %v, want exactly 1 node", index, assigned)
		}
		if !decision.FallbackOutOfTarget && int32(len(assigned)) > decision.TargetFanout {
			t.Fatalf("pod %d assignment = %v exceeds target %d", index, assigned, decision.TargetFanout)
		}
	}
}

func nodeSet(nodes []string) map[string]struct{} {
	set := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		set[node] = struct{}{}
	}
	return set
}

func assertOnePodPerNode(t *testing.T, out Output) {
	t.Helper()
	seen := map[string]int32{}
	for index, nodes := range out.Assignment {
		for _, node := range nodes {
			if other, ok := seen[node]; ok && other != index {
				t.Fatalf("pods %d and %d share node %q", other, index, node)
			}
			seen[node] = index
		}
	}
}

func assertPrewarmCoversAssignment(t *testing.T, out Output) {
	t.Helper()
	want := 0
	for _, nodes := range out.Assignment {
		want += len(nodes)
	}
	total := 0
	for node, pods := range out.PreheatSet {
		if !sort.SliceIsSorted(pods, func(i, j int) bool { return pods[i] < pods[j] }) {
			t.Fatalf("preheatSet[%s] is not sorted: %v", node, pods)
		}
		for i := 1; i < len(pods); i++ {
			if pods[i] == pods[i-1] {
				t.Fatalf("preheatSet[%s] contains duplicate pod %d", node, pods[i])
			}
		}
		for _, index := range pods {
			found := false
			for _, assigned := range out.Assignment[index] {
				if assigned == node {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("preheatSet[%s] contains pod %d assigned to %v", node, index, out.Assignment[index])
			}
			total++
		}
	}
	if total != want {
		t.Fatalf("preheatSet has %d pairs, assignment has %d", total, want)
	}
}

func assignmentKey(out Output) string {
	pods := make([]int32, 0, len(out.Assignment))
	for index := range out.Assignment {
		pods = append(pods, index)
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i] < pods[j] })
	key := ""
	for _, index := range pods {
		for _, node := range out.Assignment[index] {
			key += node + ","
		}
		key += ";"
	}
	return key
}

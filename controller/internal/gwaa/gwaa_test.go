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
	in := Input{
		Pods:   []PodState{testPod(0, map[string]int32{"n0": 5, "n1": 2})},
		Nodes:  testNodes("n0", "n1"),
		Rand:   rand.New(rand.NewSource(0)),
		Fanout: 3,
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
				feasible := feasibleNodeCount(in.Nodes)
				want := int(in.Fanout)
				if feasible < want {
					want = feasible
				}
				for _, pod := range in.Pods {
					nodes := out.Assignment[pod.Index]
					if len(nodes) != want {
						t.Fatalf("seed %d: pod %d got %v, want %d nodes", seed, pod.Index, nodes, want)
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
			{Index: 1, K: 8, ReadyCountByNode: map[string]int32{"n0": 5, "n1": 4, "n2": 3, "n3": 2}, AntiAffinityGroup: "a"},
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
		Pods:  []PodState{testPod(0, map[string]int32{"n0": 3, "n1": 3})},
		Nodes: testNodes("n0", "n1"),
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
	p1 := out.Assignment[1]
	if len(p1) != 2 || p1[0] != "n0" {
		t.Fatalf("winner pod 1 = %v, want [n0 ...]", p1)
	}
	for _, node := range out.Assignment[0] {
		if node == "n0" {
			t.Fatalf("yielding pod 0 kept n0: %v", out.Assignment[0])
		}
	}
	if len(out.Assignment[0]) != 2 {
		t.Fatalf("yielding pod 0 = %v, want 2 nodes", out.Assignment[0])
	}
	assertPrewarmCoversAssignment(t, out)
}

func TestComputeNonConvergenceFallsBack(t *testing.T) {
	in := Input{
		Pods: []PodState{
			testPod(0, map[string]int32{"n0": 10, "n1": 0}),
			testPod(1, map[string]int32{"n0": 9, "n1": 1}),
		},
		Nodes:     testNodes("n0", "n1"),
		Rand:      rand.New(rand.NewSource(2)),
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

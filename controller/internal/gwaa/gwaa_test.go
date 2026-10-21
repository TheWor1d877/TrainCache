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

func TestComputeFixedSeedReproducible(t *testing.T) {
	in := tieRichInput(42)
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

func TestComputeRandomSeedsProduceLegalPlacements(t *testing.T) {
	var first string
	distinct := map[string]struct{}{}
	for seed := int64(0); seed < 64; seed++ {
		out := Compute(tieRichInput(seed))
		if out.Err != nil {
			t.Fatalf("seed %d: %v", seed, out.Err)
		}
		assertOnePodPerNode(t, out)
		assertPrewarmMirrorsAssignment(t, out)
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

func TestComputeAffinityGroupSharesNode(t *testing.T) {
	in := Input{
		Pods: []PodState{
			{Index: 0, K: 8, ReadyCountByNode: map[string]int32{"n0": 5, "n1": 1}, AffinityGroup: "g"},
			{Index: 1, K: 8, ReadyCountByNode: map[string]int32{"n0": 2, "n1": 4}, AffinityGroup: "g"},
			testPod(2, map[string]int32{"n0": 0, "n1": 6}),
		},
		Nodes: testNodes("n0", "n1"),
		Rand:  rand.New(rand.NewSource(7)),
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if out.Assignment[0] == "" || out.Assignment[0] != out.Assignment[1] {
		t.Fatalf("affinity Pods not co-located: %v", out.Assignment)
	}
}

func TestComputeAntiAffinitySeparatesPods(t *testing.T) {
	in := Input{
		Pods: []PodState{
			{Index: 0, K: 8, ReadyCountByNode: map[string]int32{"n0": 5, "n1": 0}, AntiAffinityGroup: "a"},
			{Index: 1, K: 8, ReadyCountByNode: map[string]int32{"n0": 5, "n1": 0}, AntiAffinityGroup: "a"},
		},
		Nodes:                testNodes("n0", "n1"),
		Rand:                 rand.New(rand.NewSource(3)),
		AllowMultiPodPerNode: true,
	}
	out := Compute(in)
	if out.Err != nil {
		t.Fatalf("Compute returned error: %v", out.Err)
	}
	if len(out.Assignment) != 2 {
		t.Fatalf("assignment = %v, want both Pods placed", out.Assignment)
	}
	if out.Assignment[0] == out.Assignment[1] {
		t.Fatalf("anti-affinity Pods share node %q", out.Assignment[0])
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
	for index, node := range out.Assignment {
		if node != "n0" {
			t.Fatalf("pod %d placed on %q, want n0", index, node)
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
		seen[first.Assignment[0]] = struct{}{}
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
	if out.Assignment[1] != "n0" || out.Assignment[0] != "n1" {
		t.Fatalf("conflict adjudication = %v, want p1=n0 and p0=n1", out.Assignment)
	}
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
	if len(out.Assignment) != 2 || out.Assignment[0] == out.Assignment[1] {
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
		Pods:  []PodState{testPod(0, map[string]int32{"n0": 3}), testPod(1, nil)},
		Nodes: nil,
		Rand:  rand.New(rand.NewSource(0)),
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
		Pods:  []PodState{testPod(0, map[string]int32{"n0": 3})},
		Nodes: []NodeState{{Name: "n0", Stopped: true}},
		Rand:  rand.New(rand.NewSource(0)),
	}
	got := Compute(stopped)
	if len(got.Assignment) != 0 || !reflect.DeepEqual(got.UnassignedPods, []int32{0}) {
		t.Fatalf("stopped node: assignment=%v unassigned=%v", got.Assignment, got.UnassignedPods)
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
	if out.Assignment[0] != "n1" {
		t.Fatalf("pod assigned to %q, want n1", out.Assignment[0])
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

func assertOnePodPerNode(t *testing.T, out Output) {
	t.Helper()
	seen := map[string]int32{}
	for index, node := range out.Assignment {
		if other, ok := seen[node]; ok {
			t.Fatalf("pods %d and %d share node %q", other, index, node)
		}
		seen[node] = index
	}
}

func assertPrewarmMirrorsAssignment(t *testing.T, out Output) {
	t.Helper()
	total := 0
	for node, pods := range out.PreheatSet {
		if !sort.SliceIsSorted(pods, func(i, j int) bool { return pods[i] < pods[j] }) {
			t.Fatalf("preheatSet[%s] is not sorted: %v", node, pods)
		}
		for _, index := range pods {
			if out.Assignment[index] != node {
				t.Fatalf("preheatSet[%s] contains pod %d assigned to %q", node, index, out.Assignment[index])
			}
			total++
		}
	}
	if total != len(out.Assignment) {
		t.Fatalf("preheatSet has %d entries, assignment has %d", total, len(out.Assignment))
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
		key += out.Assignment[index] + ";"
	}
	return key
}

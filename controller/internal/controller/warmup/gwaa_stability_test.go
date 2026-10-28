package warmup

// Regression for the M11 e2e cold-start thrash: GWAA tie-breaking must stay
// pinned to one WarmupTask generation. With a fresh random draw per poll, an
// all-zero readiness table reshuffles the preference order every reconcile, so
// the target set flips and the previous submits are cancelled as target_shrink
// until t_max expires (docs/7.3 D32).

import (
	"math/rand"
	"reflect"
	"testing"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// d32ColdTask is a 4-Pod task on 4 candidates whose whole readiness table is
// zero, so every c_ij ties.
func d32ColdTask() *traincachev1alpha1.WarmupTask {
	nodes := []string{"n1", "n2", "n3", "n4"}
	task := fixtureTask(4, 8, nil)
	task.Spec.PreheatFanout = 2
	task.Spec.PreheatFanoutMin = 1
	task.Status.CandidateNodes = nodes
	table := make([][]int32, 4)
	for row := range table {
		table[row] = []int32{0, 0, 0, 0}
	}
	task.Status.ReadinessTable = table
	return task
}

func TestGwaaRandPinnedToTaskGeneration(t *testing.T) {
	r := &Reconciler{}
	task := d32ColdTask()

	first, second := r.gwaaRand(task), r.gwaaRand(task)
	for draw := 0; draw < 64; draw++ {
		if a, b := first.Intn(1000), second.Intn(1000); a != b {
			t.Fatalf("draw %d = %d then %d: one generation must reuse the same shuffle order", draw, a, b)
		}
	}

	other := d32ColdTask()
	other.UID = "uid-2"
	if r.gwaaRand(other).Intn(1<<30) == r.gwaaRand(task).Intn(1<<30) {
		t.Fatal("different tasks share a seed, so the D32 randomization is gone")
	}

	bumped := d32ColdTask()
	bumped.Generation = 4
	if r.gwaaRand(bumped).Intn(1<<30) == r.gwaaRand(task).Intn(1<<30) {
		t.Fatal("a spec update must re-randomize the tie order")
	}

	injected := rand.New(rand.NewSource(42))
	r.Rand = injected
	if got := r.gwaaRand(task); got != injected {
		t.Fatal("Reconciler.Rand must override the derived seed")
	}
}

func TestComputePlacementStableAcrossPolls(t *testing.T) {
	r := &Reconciler{}
	task := d32ColdTask()
	nodes := task.Status.CandidateNodes

	before := r.computePlacement(task, nodes, nil)
	after := r.computePlacement(task, nodes, nil)
	if before.Err != nil || after.Err != nil {
		t.Fatalf("computePlacement errors: %v / %v", before.Err, after.Err)
	}
	if !reflect.DeepEqual(before.Assignment, after.Assignment) {
		t.Fatalf("cold-start assignment flipped between polls: %v -> %v", before.Assignment, after.Assignment)
	}
	if !reflect.DeepEqual(before.TargetSet, after.TargetSet) {
		t.Fatalf("cold-start target set flipped between polls: %v -> %v", before.TargetSet, after.TargetSet)
	}
}

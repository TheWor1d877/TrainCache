package warmup

import (
	"reflect"
	"testing"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/consumeorder"
)

// m12OrderTask builds a task whose PodDetails are indexed in slice order.
func m12OrderTask(orders [][]int32) *traincachev1alpha1.WarmupTask {
	details := make([]traincachev1alpha1.PodDetail, len(orders))
	for i, order := range orders {
		details[i] = traincachev1alpha1.PodDetail{Index: int32(i), InitialConsumeOrder: order, K: 1}
	}
	return &traincachev1alpha1.WarmupTask{
		Spec: traincachev1alpha1.WarmupTaskSpec{
			PodCount:   int32(len(orders)),
			PodDetails: details,
		},
	}
}

// TestM12S17AnyInvalidPodDefaultsWholeGroup mutates exactly one Pod at every
// position and asserts that the entire group is replaced by DefaultSplit, not
// merely the offending Pod.
func TestM12S17AnyInvalidPodDefaultsWholeGroup(t *testing.T) {
	const (
		total = int32(7)
		pods  = int32(3)
	)
	valid := [][]int32{{0, 6}, {1, 5}, {2, 3, 4}}

	if got, src, err := ResolveConsumeOrders(m12OrderTask(valid), total); err != nil ||
		src != traincachev1alpha1.ConsumeOrderSourceSpec || !reflect.DeepEqual(got, valid) {
		t.Fatalf("valid explicit orders: got=%v src=%q err=%v, want Spec", got, src, err)
	}

	want, err := consumeorder.DefaultSplit(total, pods)
	if err != nil {
		t.Fatalf("DefaultSplit(%d,%d): %v", total, pods, err)
	}

	invalid := map[string]func([][]int32){
		"empty":        func(o [][]int32) { o[0] = nil },
		"out of range": func(o [][]int32) { o[0] = []int32{0, 7} },
		"negative":     func(o [][]int32) { o[0] = []int32{0, -1} },
		"duplicate":    func(o [][]int32) { o[0] = []int32{0, 1} },
		"incomplete":   func(o [][]int32) { o[0] = []int32{0} },
	}

	for pos := 0; pos < int(pods); pos++ {
		for name, mutate := range invalid {
			orders := make([][]int32, len(valid))
			for i := range valid {
				orders[i] = append([]int32(nil), valid[i]...)
			}
			mutate(orders)
			// Move the invalid order to every position in turn.
			orders[0], orders[pos] = orders[pos], orders[0]

			got, src, err := ResolveConsumeOrders(m12OrderTask(orders), total)
			if err != nil {
				t.Fatalf("%s at pod %d: unexpected error %v", name, pos, err)
			}
			if src != traincachev1alpha1.ConsumeOrderSourceDefault {
				t.Fatalf("%s at pod %d: source = %q, want Default", name, pos, src)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s at pod %d: orders = %v, want whole-group default %v", name, pos, got, want)
			}
			if len(got) != int(pods) {
				t.Fatalf("%s at pod %d: got %d windows, want %d", name, pos, len(got), pods)
			}
		}
	}
}

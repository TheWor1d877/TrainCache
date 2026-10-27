package warmup

import (
	"reflect"
	"testing"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/consumeorder"
)

// m12OrderTask builds a task whose PodDetails are indexed in slice order.
func m12OrderTask(files [][]string) *traincachev1alpha1.WarmupTask {
	return fileTask(files, 1)
}

func m12Layout() []traincachev1alpha1.BlockLayout {
	return []traincachev1alpha1.BlockLayout{
		{Shard: "a.tar", StartChunk: 0, Count: 3},
		{Shard: "b.tar", StartChunk: 3, Count: 2},
		{Shard: "c.tar", StartChunk: 5, Count: 1},
		{Shard: "d.tar", StartChunk: 6, Count: 1},
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
	valid := [][]string{{"a.tar"}, {"b.tar"}, {"c.tar", "d.tar"}}
	validOrders := [][]int32{{0, 1, 2}, {3, 4}, {5, 6}}

	if got, src, err := ResolveConsumeOrders(m12OrderTask(valid), total, m12Layout()); err != nil ||
		src != traincachev1alpha1.ConsumeOrderSourceSpec || !reflect.DeepEqual(got, validOrders) {
		t.Fatalf("valid explicit files: got=%v src=%q err=%v, want Spec", got, src, err)
	}

	want, err := consumeorder.DefaultSplit(total, pods)
	if err != nil {
		t.Fatalf("DefaultSplit(%d,%d): %v", total, pods, err)
	}

	invalid := map[string]func([][]string){
		"empty":      func(f [][]string) { f[0] = nil },
		"unknown":    func(f [][]string) { f[0] = []string{"missing.tar"} },
		"duplicate":  func(f [][]string) { f[1] = []string{"a.tar"} },
		"incomplete": func(f [][]string) { f[2] = []string{"c.tar"} },
	}

	for pos := 0; pos < int(pods); pos++ {
		for name, mutate := range invalid {
			files := make([][]string, len(valid))
			for i := range valid {
				files[i] = append([]string(nil), valid[i]...)
			}
			mutate(files)
			// Move the invalid list to every position in turn.
			files[0], files[pos] = files[pos], files[0]

			got, src, err := ResolveConsumeOrders(m12OrderTask(files), total, m12Layout())
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

package warmup

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

func orderTask(orders [][]int32, k int32) *traincachev1alpha1.WarmupTask {
	details := make([]traincachev1alpha1.PodDetail, len(orders))
	for i, order := range orders {
		details[i] = traincachev1alpha1.PodDetail{Index: int32(i), InitialConsumeOrder: order, K: k}
	}
	return &traincachev1alpha1.WarmupTask{
		Spec: traincachev1alpha1.WarmupTaskSpec{
			PodCount:   int32(len(orders)),
			PodDetails: details,
		},
	}
}

func TestResolveConsumeOrdersExplicit(t *testing.T) {
	task := orderTask([][]int32{{0, 1, 2}, {3, 4, 5}}, 2)
	orders, source, err := ResolveConsumeOrders(task, 6)
	if err != nil {
		t.Fatalf("ResolveConsumeOrders: %v", err)
	}
	if source != traincachev1alpha1.ConsumeOrderSourceSpec {
		t.Fatalf("source = %q, want Spec", source)
	}
	want := [][]int32{{0, 1, 2}, {3, 4, 5}}
	if !reflect.DeepEqual(orders, want) {
		t.Fatalf("orders = %v, want %v", orders, want)
	}
}

func TestResolveConsumeOrdersFallsBackOnAnyInvalidOrder(t *testing.T) {
	cases := map[string][][]int32{
		"empty":        {{0, 1}, {}},
		"out of range": {{0, 1}, {2, 9}},
		"duplicate":    {{0, 1}, {1, 2}},
		"incomplete":   {{0}, {1}},
	}
	for name, orders := range cases {
		t.Run(name, func(t *testing.T) {
			task := orderTask(orders, 1)
			got, source, err := ResolveConsumeOrders(task, 4)
			if err != nil {
				t.Fatalf("ResolveConsumeOrders: %v", err)
			}
			if source != traincachev1alpha1.ConsumeOrderSourceDefault {
				t.Fatalf("source = %q, want Default", source)
			}
			want := [][]int32{{0, 1}, {2, 3}}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("default orders = %v, want %v", got, want)
			}
		})
	}
}

func TestResolveConsumeOrdersDefaultSplitMatchesPlan(t *testing.T) {
	task := orderTask([][]int32{{0}, {1}, {2}}, 1) // incomplete -> default
	got, source, err := ResolveConsumeOrders(task, 8)
	if err != nil {
		t.Fatalf("ResolveConsumeOrders: %v", err)
	}
	if source != traincachev1alpha1.ConsumeOrderSourceDefault {
		t.Fatalf("source = %q", source)
	}
	want := [][]int32{{0, 1}, {2, 3, 4}, {5, 6, 7}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("default split = %v, want %v", got, want)
	}
}

func TestResolveConsumeOrdersRejectsTotalBelowPods(t *testing.T) {
	task := orderTask([][]int32{{}, {}, {}}, 1)
	_, _, err := ResolveConsumeOrders(task, 2)
	if !errors.Is(err, errInvalidSpec) {
		t.Fatalf("error = %v, want errInvalidSpec", err)
	}
}

func TestPodWindowBoundaries(t *testing.T) {
	order := []int32{10, 11, 12, 13}
	cases := []struct {
		name   string
		k      int32
		cutoff int32
		want   []int32
	}{
		{"K=1", 1, 0, []int32{10}},
		{"cutoff larger than K", 1, 3, []int32{10, 11, 12}},
		{"K larger than cutoff", 3, 2, []int32{10, 11, 12}},
		{"full order", 10, 10, []int32{10, 11, 12, 13}},
		{"zero window", 0, 0, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PodWindow(order, tc.k, tc.cutoff); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("PodWindow() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExpandChunksAcrossShardBoundary(t *testing.T) {
	layout := []traincachev1alpha1.BlockLayout{
		{Shard: "a.tar", StartChunk: 0, Count: 3},
		{Shard: "b.tar", StartChunk: 3, Count: 2},
	}
	chunks, err := ExpandChunks([]int32{0, 2, 3, 4}, layout)
	if err != nil {
		t.Fatalf("ExpandChunks: %v", err)
	}
	want := []struct {
		path  string
		chunk int64
	}{{"a.tar", 0}, {"a.tar", 2}, {"b.tar", 0}, {"b.tar", 1}}
	if len(chunks) != len(want) {
		t.Fatalf("len(chunks) = %d, want %d", len(chunks), len(want))
	}
	for i := range want {
		if chunks[i].Path != want[i].path || chunks[i].Chunk != want[i].chunk {
			t.Fatalf("chunks[%d] = %#v, want %#v", i, chunks[i], want[i])
		}
	}
}

func TestExpandChunksRejectsOutOfLayout(t *testing.T) {
	layout := []traincachev1alpha1.BlockLayout{{Shard: "a.tar", StartChunk: 0, Count: 2}}
	for _, block := range []int32{-1, 2} {
		if _, err := ExpandChunks([]int32{block}, layout); !errors.Is(err, errInvalidSpec) {
			t.Fatalf("block %d error = %v, want errInvalidSpec", block, err)
		}
	}
}

func TestValidateTask(t *testing.T) {
	valid := orderTask([][]int32{{0, 1}, {2, 3}}, 2)
	if reasons := ValidateTask(valid); len(reasons) != 0 {
		t.Fatalf("valid task rejected: %v", reasons)
	}

	duplicate := orderTask([][]int32{{0, 1}, {2, 3}}, 2)
	duplicate.Spec.PodDetails[1].Index = 0
	if reasons := ValidateTask(duplicate); len(reasons) == 0 {
		t.Fatal("duplicate index accepted")
	}

	zeroK := orderTask([][]int32{{0, 1}, {2, 3}}, 0)
	if reasons := ValidateTask(zeroK); len(reasons) == 0 {
		t.Fatal("K=0 accepted")
	}

	tooMany := orderTask(make([][]int32, maxPods+1), 1)
	if reasons := ValidateTask(tooMany); len(reasons) == 0 || !strings.Contains(strings.Join(reasons, ";"), "exceeds limit") {
		t.Fatalf("P limit not enforced: %v", reasons)
	}
}

func TestTaskID(t *testing.T) {
	task := orderTask([][]int32{{0}}, 1)
	task.Namespace = "train"
	task.Name = "t1"
	if got := TaskID(task, 3); got != "train/t1/p3" {
		t.Fatalf("TaskID = %q", got)
	}
}

func TestWindowChunksPreservesOrder(t *testing.T) {
	layout := []traincachev1alpha1.BlockLayout{{Shard: "s.tar", StartChunk: 0, Count: 10}}
	chunks, err := WindowChunks([]int32{5, 1, 9}, 2, 0, layout)
	if err != nil {
		t.Fatalf("WindowChunks: %v", err)
	}
	if len(chunks) != 2 || chunks[0].Chunk != 5 || chunks[1].Chunk != 1 {
		t.Fatalf("chunks = %#v, want [5,1]", chunks)
	}
}

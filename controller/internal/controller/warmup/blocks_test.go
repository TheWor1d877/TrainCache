package warmup

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/config"
)

func fileTask(files [][]string, k int32) *traincachev1alpha1.WarmupTask {
	details := make([]traincachev1alpha1.PodDetail, len(files))
	for i, names := range files {
		details[i] = traincachev1alpha1.PodDetail{Index: int32(i), InitialConsumeFiles: names, K: k}
	}
	return &traincachev1alpha1.WarmupTask{
		Spec: traincachev1alpha1.WarmupTaskSpec{
			PodCount:   int32(len(files)),
			PodDetails: details,
		},
	}
}

// testLayout is three shards covering global chunks [0,6).
func testLayout() []traincachev1alpha1.BlockLayout {
	return []traincachev1alpha1.BlockLayout{
		{Shard: "a.tar", StartChunk: 0, Count: 3},
		{Shard: "b.tar", StartChunk: 3, Count: 2},
		{Shard: "c.tar", StartChunk: 5, Count: 1},
	}
}

func TestResolveConsumeOrdersExplicitFiles(t *testing.T) {
	task := fileTask([][]string{{"a.tar", "c.tar"}, {"b.tar"}}, 2)
	orders, source, err := ResolveConsumeOrders(task, 6, testLayout())
	if err != nil {
		t.Fatalf("ResolveConsumeOrders: %v", err)
	}
	if source != traincachev1alpha1.ConsumeOrderSourceSpec {
		t.Fatalf("source = %q, want Spec", source)
	}
	want := [][]int32{{0, 1, 2, 5}, {3, 4}}
	if !reflect.DeepEqual(orders, want) {
		t.Fatalf("orders = %v, want %v", orders, want)
	}
}

func TestResolveConsumeOrdersFallsBackOnAnyInvalidFiles(t *testing.T) {
	cases := map[string][][]string{
		"empty":            {{"a.tar"}, {}},
		"unknown shard":    {{"a.tar", "missing.tar"}, {"b.tar", "c.tar"}},
		"duplicate shard":  {{"a.tar"}, {"a.tar", "b.tar", "c.tar"}},
		"duplicate in pod": {{"a.tar", "a.tar"}, {"b.tar", "c.tar"}},
		"incomplete":       {{"a.tar"}, {"b.tar"}},
	}
	for name, files := range cases {
		t.Run(name, func(t *testing.T) {
			got, source, err := ResolveConsumeOrders(fileTask(files, 1), 6, testLayout())
			if err != nil {
				t.Fatalf("ResolveConsumeOrders: %v", err)
			}
			if source != traincachev1alpha1.ConsumeOrderSourceDefault {
				t.Fatalf("source = %q, want Default", source)
			}
			want := [][]int32{{0, 1, 2}, {3, 4, 5}}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("default orders = %v, want %v", got, want)
			}
		})
	}
}

func TestResolveConsumeOrdersDefaultSplitMatchesPlan(t *testing.T) {
	task := fileTask([][]string{{"a.tar"}, {"b.tar"}, {"c.tar"}}, 1) // incomplete -> default
	got, source, err := ResolveConsumeOrders(task, 8, []traincachev1alpha1.BlockLayout{{Shard: "all.tar", StartChunk: 0, Count: 8}})
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
	task := fileTask([][]string{nil, nil, nil}, 1)
	_, _, err := ResolveConsumeOrders(task, 2, testLayout())
	if !errors.Is(err, errInvalidSpec) {
		t.Fatalf("error = %v, want errInvalidSpec", err)
	}
}

func TestValidateExplicitK(t *testing.T) {
	orders := [][]int32{{0, 1}, {2, 3, 4}}
	task := fileTask([][]string{{"a.tar"}, {"b.tar"}}, 2)
	if reasons := ValidateExplicitK(task, orders); len(reasons) != 0 {
		t.Fatalf("K within W rejected: %v", reasons)
	}

	task.Spec.PodDetails[0].K = 0 // auto is not an explicit violation
	if reasons := ValidateExplicitK(task, orders); len(reasons) != 0 {
		t.Fatalf("K=0 rejected: %v", reasons)
	}

	task.Spec.PodDetails[0].K = 3
	reasons := ValidateExplicitK(task, orders)
	if len(reasons) != 1 || !strings.Contains(reasons[0], "exceeds") {
		t.Fatalf("K > W reasons = %v", reasons)
	}
}

func kTestDataSet(chunkSize int64) *traincachev1alpha1.DataSet {
	return &traincachev1alpha1.DataSet{
		Spec: traincachev1alpha1.DataSetSpec{ChunkSize: chunkSize},
	}
}

func TestComputeKWindowOnly(t *testing.T) {
	const mib = int64(1 << 20)
	cfg := &config.Config{StartupWindowBytes: 512 * mib, MaxFillFraction: 0.125}
	ds := kTestDataSet(8 * mib)

	cases := []struct {
		name  string
		order []int32
		want  int32
	}{
		{name: "window caps large W", order: make([]int32, 100), want: 64},
		{name: "W caps window", order: make([]int32, 10), want: 10},
		{name: "never below one", order: nil, want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := computeK(cfg, ds, [][]int32{tc.order})
			if err != nil {
				t.Fatalf("computeK: %v", err)
			}
			if got[0] != tc.want {
				t.Fatalf("K = %d, want %d", got[0], tc.want)
			}
		})
	}
}

func TestComputeKCache(t *testing.T) {
	const mib = int64(1 << 20)
	cases := []struct {
		name    string
		cache   int64
		pods    int
		want    int32
		window  int64
		maxFill float64
	}{
		{name: "single pod", cache: 1 << 30, pods: 1, want: 16, window: 512 * mib, maxFill: 0.125},
		{name: "two pods split budget", cache: 1 << 30, pods: 2, want: 8, window: 512 * mib, maxFill: 0.125},
		{name: "cache zero skips kcache", cache: 0, pods: 1, want: 64, window: 512 * mib, maxFill: 0.125},
		{name: "tiny cache clamps to one", cache: 1, pods: 1, want: 1, window: 512 * mib, maxFill: 0.125},
		{name: "small W wins", cache: 1 << 30, pods: 1, want: 3, window: 512 * mib, maxFill: 0.125},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				CacheCapacityBytes: tc.cache,
				StartupWindowBytes: tc.window,
				MaxFillFraction:    tc.maxFill,
			}
			orders := make([][]int32, tc.pods)
			for i := range orders {
				orders[i] = make([]int32, 100)
			}
			if tc.name == "small W wins" {
				orders[0] = make([]int32, tc.want)
			}
			got, err := computeK(cfg, kTestDataSet(8*mib), orders)
			if err != nil {
				t.Fatalf("computeK: %v", err)
			}
			if got[0] != tc.want {
				t.Fatalf("K = %d, want %d", got[0], tc.want)
			}
		})
	}
}

func TestComputeKDefaultsAndErrors(t *testing.T) {
	const mib = int64(1 << 20)
	orders := [][]int32{make([]int32, 100)}
	got, err := computeK(nil, kTestDataSet(8*mib), orders)
	if err != nil {
		t.Fatalf("computeK: %v", err)
	}
	// Default 512MiB / 8MiB = 64 and no cache budget.
	if got[0] != 64 {
		t.Fatalf("default K = %d, want 64", got[0])
	}
	if _, err := computeK(&config.Config{}, kTestDataSet(0), orders); !errors.Is(err, errInvalidSpec) {
		t.Fatalf("chunkSize=0 error = %v, want errInvalidSpec", err)
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
	valid := fileTask([][]string{{"a.tar"}, {"b.tar"}}, 1)
	if reasons := ValidateTask(valid); len(reasons) != 0 {
		t.Fatalf("valid task rejected: %v", reasons)
	}

	duplicate := fileTask([][]string{{"a.tar"}, {"b.tar"}}, 1)
	duplicate.Spec.PodDetails[1].Index = 0
	if reasons := ValidateTask(duplicate); len(reasons) == 0 {
		t.Fatal("duplicate index accepted")
	}

	zeroK := fileTask([][]string{{"a.tar"}, {"b.tar"}}, 0)
	if reasons := ValidateTask(zeroK); len(reasons) != 0 {
		t.Fatalf("K=0 (auto) rejected: %v", reasons)
	}

	negativeK := fileTask([][]string{{"a.tar"}, {"b.tar"}}, -1)
	if reasons := ValidateTask(negativeK); len(reasons) == 0 || !strings.Contains(strings.Join(reasons, ";"), "must be >= 0") {
		t.Fatalf("K=-1 accepted: %v", reasons)
	}

	tooMany := fileTask(make([][]string, maxPods+1), 1)
	if reasons := ValidateTask(tooMany); len(reasons) == 0 || !strings.Contains(strings.Join(reasons, ";"), "exceeds limit") {
		t.Fatalf("P limit not enforced: %v", reasons)
	}
}

func TestTaskID(t *testing.T) {
	task := fileTask([][]string{{"a.tar"}}, 1)
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

func TestKExceedsFill(t *testing.T) {
	const mib = int64(1 << 20)
	cfg := &config.Config{CacheCapacityBytes: 1 << 30, StartupWindowBytes: 512 * mib, MaxFillFraction: 0.125}
	ds := kTestDataSet(8 * mib)
	orders := [][]int32{make([]int32, 100), make([]int32, 100)}

	task := fileTask([][]string{{"a.tar"}, {"b.tar"}}, 2)
	if total, limit, exceeded := KExceedsFill(cfg, ds, task, orders); exceeded {
		t.Fatalf("auto K exceeded cap: total=%d limit=%d", total, limit)
	}

	// Explicit K above the cache share is honoured but reported.
	task.Spec.PodDetails[0].K = 64
	task.Spec.PodDetails[1].K = 64
	total, limit, exceeded := KExceedsFill(cfg, ds, task, orders)
	if !exceeded {
		t.Fatalf("explicit K not reported: total=%d limit=%d", total, limit)
	}
	if total != 128*8*mib || limit != 128*mib {
		t.Fatalf("total=%d limit=%d, want %d/%d", total, limit, 128*8*mib, 128*mib)
	}

	// A cutoff above K counts toward the volume; it is capped by W_i.
	task.Spec.PodDetails[0].K = 0
	task.Spec.PodDetails[1].K = 0
	task.Spec.Score.Cutoff = 64
	if total, _, exceeded := KExceedsFill(cfg, ds, task, orders); !exceeded || total != 128*8*mib {
		t.Fatalf("cutoff not counted: total=%d exceeded=%v", total, exceeded)
	}

	// An unset cache capacity disables the safety check.
	cfg.CacheCapacityBytes = 0
	if _, _, exceeded := KExceedsFill(cfg, ds, task, orders); exceeded {
		t.Fatal("check ran with cacheCapacityBytes=0")
	}
}

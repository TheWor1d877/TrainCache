package dataset

import (
	"math"
	"testing"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

func TestBuildLayoutAccumulatesAcrossShards(t *testing.T) {
	shards := []traincachev1alpha1.ShardSpec{
		{Name: "a", Count: 2, LastChunkSize: 10},
		{Name: "b", Count: 3, LastChunkSize: 20},
		{Name: "c", Count: 2, LastChunkSize: 30},
	}
	total, layout, err := BuildLayout(shards)
	if err != nil {
		t.Fatalf("BuildLayout: %v", err)
	}
	if total != 7 {
		t.Fatalf("total = %d, want 7", total)
	}
	wantStarts := []int32{0, 2, 5}
	next := int32(0)
	for i, block := range layout {
		if block.Shard != shards[i].Name || block.StartChunk != wantStarts[i] || block.Count != shards[i].Count {
			t.Fatalf("layout[%d] = %#v, want shard=%s start=%d count=%d", i, block, shards[i].Name, wantStarts[i], shards[i].Count)
		}
		if block.StartChunk != next {
			t.Fatalf("layout[%d] start = %d, want %d", i, block.StartChunk, next)
		}
		next += block.Count
	}
	if next != total {
		t.Fatalf("layout covers %d chunks, want %d", next, total)
	}
}

func TestBuildLayoutOverflow(t *testing.T) {
	shards := []traincachev1alpha1.ShardSpec{
		{Name: "a", Count: math.MaxInt32, LastChunkSize: 1},
		{Name: "b", Count: 1, LastChunkSize: 1},
	}
	if _, _, err := BuildLayout(shards); err == nil {
		t.Fatal("BuildLayout overflow: got nil error")
	}
}

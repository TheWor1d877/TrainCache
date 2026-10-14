package ordersource

import (
	"encoding/json"
	"reflect"
	"testing"
)

// TestChunkRefJSONRoundtrip ChunkRef 编解码字段一致,JSON 形态冻结为 {"path","chunk"}。
func TestChunkRefJSONRoundtrip(t *testing.T) {
	in := ChunkRef{Path: "shard-007.tar", Chunk: 3}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal ChunkRef: %v", err)
	}
	if got, want := string(b), `{"path":"shard-007.tar","chunk":3}`; got != want {
		t.Errorf("ChunkRef JSON 形态 = %s, want %s(design §5.3 字段冻结)", got, want)
	}
	var out ChunkRef
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("Unmarshal ChunkRef: %v", err)
	}
	if out != in {
		t.Errorf("往返不一致: out=%+v in=%+v", out, in)
	}
}

// TestConsumeOrderJSONRoundtrip 全字段(含 chunks 顺序)写→读一致;
// design §6.1 示例 JSON 逐字段解析断言。
func TestConsumeOrderJSONRoundtrip(t *testing.T) {
	in := ConsumeOrder{
		Dataset:        "b-track",
		SnapshotID:     "snap-20261014",
		StageID:        "epoch-2",
		ShuffleSeed:    20261015,
		BatchSize:      32,
		PrefetchChunks: 256,
		Chunks: []ChunkRef{
			{Path: "shard-000.tar", Chunk: 0},
			{Path: "shard-007.tar", Chunk: 5},
			{Path: "shard-003.tar", Chunk: 1},
		},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal ConsumeOrder: %v", err)
	}
	var out ConsumeOrder
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("Unmarshal ConsumeOrder: %v", err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Errorf("往返不一致:\ngot  %+v\nwant %+v", out, in)
	}
	if len(out.Chunks) != 3 || out.Chunks[1].Path != "shard-007.tar" {
		t.Errorf("chunks 顺序未保持: %+v", out.Chunks)
	}
}

// TestConsumeOrderDesignExample 逐字段解析 design §6.1 的示例 JSON。
func TestConsumeOrderDesignExample(t *testing.T) {
	const example = `{
  "dataset": "b-track", "snapshot_id": "snap-20261014",
  "stage_id": "epoch-2", "shuffle_seed": 20261015,
  "batch_size": 32, "prefetch_chunks": 256,
  "chunks": [ {"path": "shard-000.tar", "chunk": 0}, {"path": "shard-007.tar", "chunk": 0} ]
}`
	var o ConsumeOrder
	if err := json.Unmarshal([]byte(example), &o); err != nil {
		t.Fatalf("解析 design §6.1 示例: %v", err)
	}
	if o.Dataset != "b-track" {
		t.Errorf("dataset = %q, want b-track", o.Dataset)
	}
	if o.SnapshotID != "snap-20261014" {
		t.Errorf("snapshot_id = %q, want snap-20261014", o.SnapshotID)
	}
	if o.StageID != "epoch-2" {
		t.Errorf("stage_id = %q, want epoch-2", o.StageID)
	}
	if o.ShuffleSeed != 20261015 {
		t.Errorf("shuffle_seed = %d, want 20261015", o.ShuffleSeed)
	}
	if o.BatchSize != 32 {
		t.Errorf("batch_size = %d, want 32", o.BatchSize)
	}
	if o.PrefetchChunks != 256 {
		t.Errorf("prefetch_chunks = %d, want 256", o.PrefetchChunks)
	}
	want := []ChunkRef{{Path: "shard-000.tar", Chunk: 0}, {Path: "shard-007.tar", Chunk: 0}}
	if !reflect.DeepEqual(o.Chunks, want) {
		t.Errorf("chunks = %+v, want %+v", o.Chunks, want)
	}
}

package dataset

import (
	"fmt"
	"math"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// BuildLayout freezes the global chunk layout from the ordered shard spec.
// StartChunk is the running sum of previous shard counts; the returned total is
// the sum of all counts. Shard ranges are therefore disjoint and contiguous.
func BuildLayout(shards []traincachev1alpha1.ShardSpec) (int32, []traincachev1alpha1.BlockLayout, error) {
	layout := make([]traincachev1alpha1.BlockLayout, 0, len(shards))
	var total int64
	for _, shard := range shards {
		if total > math.MaxInt32 {
			return 0, nil, fmt.Errorf("layout: total chunk count overflows int32")
		}
		layout = append(layout, traincachev1alpha1.BlockLayout{
			Shard:         shard.Name,
			StartChunk:    int32(total),
			Count:         shard.Count,
			LastChunkSize: shard.LastChunkSize,
		})
		total += int64(shard.Count)
	}
	if total > math.MaxInt32 {
		return 0, nil, fmt.Errorf("layout: total chunk count %d overflows int32", total)
	}
	return int32(total), layout, nil
}

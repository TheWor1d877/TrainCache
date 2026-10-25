package dataset

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// ValidateSpec returns human-readable reasons why spec is invalid. An empty
// result means the spec passed every check in docs/7.1 design.md §5.2 and the
// configured chunk limit.
func ValidateSpec(spec traincachev1alpha1.DataSetSpec, maxChunks int64) []string {
	var reasons []string

	if spec.Endpoint == "" {
		reasons = append(reasons, "spec.endpoint must not be empty")
	}
	if spec.Bucket == "" {
		reasons = append(reasons, "spec.bucket must not be empty")
	}
	if spec.DatasetName == "" {
		reasons = append(reasons, "spec.datasetName must not be empty")
	}
	if spec.SecretRef.Name == "" {
		reasons = append(reasons, "spec.secretRef.name must not be empty")
	}
	if spec.ChunkSize <= 0 {
		reasons = append(reasons, fmt.Sprintf("spec.chunkSize must be > 0, got %d", spec.ChunkSize))
	}
	if len(spec.Shards) == 0 {
		reasons = append(reasons, "spec.shards must not be empty")
	}

	seen := make(map[string]struct{}, len(spec.Shards))
	var total int64
	for i, shard := range spec.Shards {
		switch {
		case shard.Name == "":
			reasons = append(reasons, fmt.Sprintf("spec.shards[%d].name must not be empty", i))
		default:
			if _, dup := seen[shard.Name]; dup {
				reasons = append(reasons, fmt.Sprintf("spec.shards[%d].name %q is duplicated", i, shard.Name))
			}
			seen[shard.Name] = struct{}{}
		}
		if shard.Count <= 0 {
			reasons = append(reasons, fmt.Sprintf("spec.shards[%d].count must be > 0, got %d", i, shard.Count))
		}
		if shard.LastChunkSize <= 0 {
			reasons = append(reasons, fmt.Sprintf("spec.shards[%d].lastChunkSize must be > 0, got %d", i, shard.LastChunkSize))
		} else if spec.ChunkSize > 0 && shard.LastChunkSize > spec.ChunkSize {
			reasons = append(reasons, fmt.Sprintf("spec.shards[%d].lastChunkSize %d exceeds chunkSize %d", i, shard.LastChunkSize, spec.ChunkSize))
		}
		total += int64(shard.Count)
	}
	if maxChunks > 0 && total > maxChunks {
		reasons = append(reasons, fmt.Sprintf("total chunks %d exceed configured maximum %d", total, maxChunks))
	}
	if !selectorEmpty(spec.AccessibleNodeSelector) {
		if _, err := metav1.LabelSelectorAsSelector(spec.AccessibleNodeSelector); err != nil {
			reasons = append(reasons, fmt.Sprintf("spec.accessibleNodeSelector is invalid: %v", err))
		}
	}
	return reasons
}

func selectorEmpty(selector *metav1.LabelSelector) bool {
	return selector == nil || (len(selector.MatchLabels) == 0 && len(selector.MatchExpressions) == 0)
}

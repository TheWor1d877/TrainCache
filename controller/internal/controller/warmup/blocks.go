package warmup

import (
	"errors"
	"fmt"
	"sort"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/consumeorder"
)

const (
	// maxPods and maxCandidateNodes are the Q11 P x N limits. P is validated
	// before submission; N is checked after candidate filtering.
	maxPods           = 64
	maxCandidateNodes = 256
)

// errInvalidSpec marks permanent spec problems. The controller turns it into
// Condition InvalidSpec=False and does not retry (docs/8.1 §7.2).
var errInvalidSpec = errors.New("warmup: invalid spec")

// ValidateTask checks the PodDetail mapping and the Q11 Pod limit. The DataSet
// is validated separately.
func ValidateTask(task *traincachev1alpha1.WarmupTask) []string {
	if task == nil {
		return []string{"task is nil"}
	}
	spec := task.Spec
	var reasons []string
	if spec.PodCount < 1 {
		reasons = append(reasons, fmt.Sprintf("spec.podCount must be >= 1, got %d", spec.PodCount))
	}
	if spec.PodCount > maxPods {
		reasons = append(reasons, fmt.Sprintf("spec.podCount %d exceeds limit %d", spec.PodCount, maxPods))
	}
	if int32(len(spec.PodDetails)) != spec.PodCount {
		reasons = append(reasons, fmt.Sprintf("spec.podDetails has %d entries, want podCount %d", len(spec.PodDetails), spec.PodCount))
	}
	seen := make(map[int32]struct{}, len(spec.PodDetails))
	for i := range spec.PodDetails {
		detail := spec.PodDetails[i]
		if detail.Index < 0 || detail.Index >= spec.PodCount {
			reasons = append(reasons, fmt.Sprintf("spec.podDetails[%d].index %d is outside 0..%d", i, detail.Index, spec.PodCount-1))
		}
		if _, dup := seen[detail.Index]; dup {
			reasons = append(reasons, fmt.Sprintf("spec.podDetails[%d].index %d is duplicated", i, detail.Index))
		}
		seen[detail.Index] = struct{}{}
		if detail.K < 1 {
			reasons = append(reasons, fmt.Sprintf("spec.podDetails[%d].k must be >= 1, got %d", i, detail.K))
		}
	}
	if spec.Score.Cutoff < 0 {
		reasons = append(reasons, fmt.Sprintf("spec.score.cutoff must be >= 0, got %d", spec.Score.Cutoff))
	}
	return reasons
}

// ResolveConsumeOrders returns the effective per-Pod order indexed by
// PodDetail.Index. If any explicit order is empty, out of range, duplicated or
// incomplete, every Pod falls back to consumeorder.DefaultSplit (D29).
func ResolveConsumeOrders(task *traincachev1alpha1.WarmupTask, totalChunks int32) ([][]int32, traincachev1alpha1.ConsumeOrderSource, error) {
	if task == nil {
		return nil, "", fmt.Errorf("%w: task is nil", errInvalidSpec)
	}
	pods := task.Spec.PodCount
	if pods < 1 {
		return nil, "", fmt.Errorf("%w: podCount %d", errInvalidSpec, pods)
	}
	if int32(len(task.Spec.PodDetails)) != pods {
		return nil, "", fmt.Errorf("%w: podDetails count %d != podCount %d", errInvalidSpec, len(task.Spec.PodDetails), pods)
	}

	orders := make([][]int32, pods)
	seen := make([]bool, pods)
	for i := range task.Spec.PodDetails {
		detail := task.Spec.PodDetails[i]
		if detail.Index < 0 || detail.Index >= pods {
			return nil, "", fmt.Errorf("%w: pod index %d outside 0..%d", errInvalidSpec, detail.Index, pods-1)
		}
		if seen[detail.Index] {
			return nil, "", fmt.Errorf("%w: pod index %d is duplicated", errInvalidSpec, detail.Index)
		}
		seen[detail.Index] = true
		orders[detail.Index] = detail.InitialConsumeOrder
	}
	for i := range seen {
		if !seen[i] {
			return nil, "", fmt.Errorf("%w: missing PodDetail for index %d", errInvalidSpec, i)
		}
	}

	if !consumeorder.NeedsDefault(orders, totalChunks) {
		return orders, traincachev1alpha1.ConsumeOrderSourceSpec, nil
	}
	split, err := consumeorder.DefaultSplit(totalChunks, pods)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", errInvalidSpec, err)
	}
	return split, traincachev1alpha1.ConsumeOrderSourceDefault, nil
}

// PodWindow returns the leading global block numbers submitted for one Pod:
// the first max(K, Cutoff) entries of order, truncated to the order length.
// The order is preserved because it defines both request and bitmap bit order.
func PodWindow(order []int32, k, cutoff int32) []int32 {
	n := k
	if cutoff > n {
		n = cutoff
	}
	if n <= 0 {
		return nil
	}
	if int64(n) > int64(len(order)) {
		n = int32(len(order))
	}
	return order[:n]
}

// ExpandChunks maps global block numbers to Agent ChunkRefs through the frozen
// DataSet block layout. A block outside the layout is an invalid spec.
func ExpandChunks(blocks []int32, layout []traincachev1alpha1.BlockLayout) ([]*agentpb.ChunkRef, error) {
	if len(layout) == 0 && len(blocks) > 0 {
		return nil, fmt.Errorf("%w: empty block layout", errInvalidSpec)
	}
	out := make([]*agentpb.ChunkRef, 0, len(blocks))
	for _, block := range blocks {
		if block < 0 {
			return nil, fmt.Errorf("%w: negative global chunk %d", errInvalidSpec, block)
		}
		i := sort.Search(len(layout), func(i int) bool {
			return layout[i].StartChunk > block
		}) - 1
		if i < 0 || block >= layout[i].StartChunk+layout[i].Count {
			return nil, fmt.Errorf("%w: global chunk %d is outside the block layout", errInvalidSpec, block)
		}
		out = append(out, &agentpb.ChunkRef{
			Path:  layout[i].Shard,
			Chunk: int64(block - layout[i].StartChunk),
		})
	}
	return out, nil
}

// WindowChunks composes PodWindow and ExpandChunks for one Pod.
func WindowChunks(order []int32, k, cutoff int32, layout []traincachev1alpha1.BlockLayout) ([]*agentpb.ChunkRef, error) {
	return ExpandChunks(PodWindow(order, k, cutoff), layout)
}

// PodWindows expands every Pod's request window. The returned slice is indexed
// by PodDetail.Index and preserves request order, which is also the bitmap bit
// order used by polling (docs/8.1 §5.9).
func PodWindows(task *traincachev1alpha1.WarmupTask, layout []traincachev1alpha1.BlockLayout, orders [][]int32) ([][]*agentpb.ChunkRef, error) {
	windows := make([][]*agentpb.ChunkRef, len(orders))
	for index := range orders {
		detail, ok := podDetail(task, int32(index))
		if !ok {
			return nil, fmt.Errorf("%w: no PodDetail for index %d", errInvalidSpec, index)
		}
		chunks, err := WindowChunks(orders[index], detail.K, task.Spec.Score.Cutoff, layout)
		if err != nil {
			return nil, err
		}
		windows[index] = chunks
	}
	return windows, nil
}

// TaskID is the idempotency key from docs/8.1 §5.9 / D12.
func TaskID(task *traincachev1alpha1.WarmupTask, podIndex int32) string {
	if task == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s/p%d", task.Namespace, task.Name, podIndex)
}

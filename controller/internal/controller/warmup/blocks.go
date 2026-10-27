package warmup

import (
	"errors"
	"fmt"
	"math"
	"sort"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/config"
	"traincache/controller/internal/consumeorder"
)

const (
	// maxPods and maxCandidateNodes are the Q11 P x N limits. P is validated
	// before submission; N is checked after candidate filtering.
	maxPods           = 64
	maxCandidateNodes = 256
)

// errInvalidSpec marks permanent spec problems. The controller turns it into
// Condition InvalidSpec=False and does not retry (docs/7.1 §7.2).
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
	if spec.PreheatFanout < 0 {
		reasons = append(reasons, fmt.Sprintf("spec.preheatFanout must be >= 0, got %d", spec.PreheatFanout))
	}
	if spec.PreheatFanoutMin < 0 {
		reasons = append(reasons, fmt.Sprintf("spec.preheatFanoutMin must be >= 0, got %d", spec.PreheatFanoutMin))
	}
	// Zero means the default 1 on both bounds. Reject an effective minimum above
	// the effective maximum even though the CRD CEL rule already does (M14).
	if normalizeFanoutMin(spec.PreheatFanoutMin) > normalizeFanout(spec.PreheatFanout) {
		reasons = append(reasons, fmt.Sprintf(
			"spec.preheatFanoutMin %d must not exceed spec.preheatFanout %d",
			normalizeFanoutMin(spec.PreheatFanoutMin), normalizeFanout(spec.PreheatFanout)))
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
		if detail.K < 0 {
			reasons = append(reasons, fmt.Sprintf("spec.podDetails[%d].k must be >= 0, got %d", i, detail.K))
		}
	}
	if spec.Score.Cutoff < 0 {
		reasons = append(reasons, fmt.Sprintf("spec.score.cutoff must be >= 0, got %d", spec.Score.Cutoff))
	}
	return reasons
}

// ResolveConsumeOrders returns the effective per-Pod chunk order indexed by
// PodDetail.Index. Each InitialConsumeFiles entry expands to the whole chunk
// range of that shard in the frozen layout, in list order.
//
// If any Pod's list is empty, names an unknown shard, repeats a shard or chunk,
// or the Pods together do not cover [0,totalChunks) exactly once, every Pod
// falls back to consumeorder.DefaultSplit (D29, D43).
func ResolveConsumeOrders(task *traincachev1alpha1.WarmupTask, totalChunks int32, layout []traincachev1alpha1.BlockLayout) ([][]int32, traincachev1alpha1.ConsumeOrderSource, error) {
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

	shards := make(map[string]traincachev1alpha1.BlockLayout, len(layout))
	for _, block := range layout {
		shards[block.Shard] = block
	}

	orders := make([][]int32, pods)
	seen := make([]bool, pods)
	invalid := false
	for i := range task.Spec.PodDetails {
		detail := task.Spec.PodDetails[i]
		if detail.Index < 0 || detail.Index >= pods {
			return nil, "", fmt.Errorf("%w: pod index %d outside 0..%d", errInvalidSpec, detail.Index, pods-1)
		}
		if seen[detail.Index] {
			return nil, "", fmt.Errorf("%w: pod index %d is duplicated", errInvalidSpec, detail.Index)
		}
		seen[detail.Index] = true

		order := make([]int32, 0, len(detail.InitialConsumeFiles))
		for _, name := range detail.InitialConsumeFiles {
			block, ok := shards[name]
			if !ok || block.Count <= 0 {
				invalid = true
				break
			}
			for chunk := int64(block.StartChunk); chunk < int64(block.StartChunk)+int64(block.Count); chunk++ {
				order = append(order, int32(chunk))
			}
		}
		orders[detail.Index] = order
	}
	for i := range seen {
		if !seen[i] {
			return nil, "", fmt.Errorf("%w: missing PodDetail for index %d", errInvalidSpec, i)
		}
	}

	if !invalid && !consumeorder.NeedsDefault(orders, totalChunks) {
		return orders, traincachev1alpha1.ConsumeOrderSourceSpec, nil
	}
	split, err := consumeorder.DefaultSplit(totalChunks, pods)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", errInvalidSpec, err)
	}
	return split, traincachev1alpha1.ConsumeOrderSourceDefault, nil
}

// ValidateExplicitK checks every non-zero PodDetail.K against that Pod's
// actual window W_i. K=0 is left for the controller to compute.
func ValidateExplicitK(task *traincachev1alpha1.WarmupTask, orders [][]int32) []string {
	if task == nil {
		return []string{"task is nil"}
	}
	var reasons []string
	for i := range task.Spec.PodDetails {
		detail := task.Spec.PodDetails[i]
		if detail.K == 0 {
			continue
		}
		index := detail.Index
		if int(index) < 0 || int(index) >= len(orders) {
			reasons = append(reasons, fmt.Sprintf("spec.podDetails[%d].index %d has no resolved order", i, index))
			continue
		}
		if w := int32(len(orders[index])); detail.K > w {
			reasons = append(reasons, fmt.Sprintf(
				"spec.podDetails[%d].k %d exceeds this Pod's chunk count %d", i, detail.K, w))
		}
	}
	return reasons
}

// KExceedsFill reports whether the effective preheat volume exceeds the
// maxFillFraction safety cap. Explicit K values are honoured as written, so a
// user can deliberately preheat more than the cap; the controller only warns.
// Auto values come from computeK. cacheCapacityBytes <= 0 disables the check.
func KExceedsFill(cfg *config.Config, ds *traincachev1alpha1.DataSet, task *traincachev1alpha1.WarmupTask, orders [][]int32) (totalBytes, limitBytes int64, exceeded bool) {
	if cfg == nil || cfg.CacheCapacityBytes <= 0 || ds == nil || ds.Spec.ChunkSize <= 0 || task == nil {
		return 0, 0, false
	}
	auto, err := computeK(cfg, ds, orders)
	if err != nil {
		return 0, 0, false
	}
	cutoff := task.Spec.Score.Cutoff
	var chunks int64
	for i := range task.Spec.PodDetails {
		detail := task.Spec.PodDetails[i]
		if int(detail.Index) < 0 || int(detail.Index) >= len(orders) {
			continue
		}
		k := detail.K
		if k == 0 && int(detail.Index) < len(auto) {
			k = auto[detail.Index]
		}
		// The submitted window is max(K_i, cutoff), capped by W_i.
		if cutoff > k {
			k = cutoff
		}
		if w := int32(len(orders[detail.Index])); k > w {
			k = w
		}
		chunks += int64(k)
	}
	maxFill := cfg.MaxFillFraction
	if maxFill <= 0 {
		maxFill = config.DefaultMaxFillFraction
	}
	totalBytes = chunks * ds.Spec.ChunkSize
	limitBytes = int64(float64(cfg.CacheCapacityBytes) * maxFill)
	return totalBytes, limitBytes, totalBytes > limitBytes
}

// computeK returns the automatic K_i for every Pod, indexed by PodDetail.Index.
// W_i is the length of the resolved order. K_window bounds the startup bytes;
// K_cache bounds the cache share when cacheCapacityBytes > 0. The result is at
// least 1 so a computed value is never mistaken for "auto".
func computeK(cfg *config.Config, ds *traincachev1alpha1.DataSet, orders [][]int32) ([]int32, error) {
	if ds == nil {
		return nil, fmt.Errorf("%w: dataset is nil", errInvalidSpec)
	}
	chunkSize := ds.Spec.ChunkSize
	if chunkSize <= 0 {
		return nil, fmt.Errorf("%w: dataset chunkSize %d", errInvalidSpec, chunkSize)
	}

	startupWindow := config.DefaultStartupWindowBytes
	maxFill := config.DefaultMaxFillFraction
	var cacheCapacity int64
	if cfg != nil {
		if cfg.StartupWindowBytes > 0 {
			startupWindow = cfg.StartupWindowBytes
		}
		if cfg.MaxFillFraction > 0 {
			maxFill = cfg.MaxFillFraction
		}
		cacheCapacity = cfg.CacheCapacityBytes
	}

	if len(orders) == 0 {
		return nil, nil
	}

	kWindow := (startupWindow + chunkSize - 1) / chunkSize
	hasCache := cacheCapacity > 0
	var kCache int64
	if hasCache {
		kCache = int64(math.Floor(float64(cacheCapacity) * maxFill / (float64(len(orders)) * float64(chunkSize))))
	}

	out := make([]int32, len(orders))
	for i := range orders {
		k := int64(len(orders[i]))
		if kWindow < k {
			k = kWindow
		}
		if hasCache && kCache < k {
			k = kCache
		}
		if k < 1 {
			k = 1
		}
		out[i] = int32(k)
	}
	return out, nil
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
// order used by polling (docs/7.1 §5.9).
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

// TaskID is the idempotency key from docs/7.1 §5.9 / D12.
func TaskID(task *traincachev1alpha1.WarmupTask, podIndex int32) string {
	if task == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s/p%d", task.Namespace, task.Name, podIndex)
}

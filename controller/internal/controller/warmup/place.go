package warmup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
	"traincache/controller/internal/gwaa"
	"traincache/controller/internal/metrics"
)

// PreheatPair identifies one (Pod, node) preheat task. task_id is derived from
// the Pod, so a Pod may legitimately have tasks on several nodes (M4 submits
// every candidate; M6 narrows that to the assigned node).
type PreheatPair struct {
	PodIndex int32
	Node     string
}

// ExistingPreheat is one task an Agent confirmed it holds, the generation it
// accepted and the ready_bitmap length that bounds its window. It is built
// from the Agent's own GetPreheatStatus reply, never from what this Controller
// meant to send: an unconfirmed pair is absent, so the diff cannot cancel or
// re-submit a task it has not seen.
type ExistingPreheat struct {
	Generation  uint64
	BitmapBytes int32
}

// PlacementApplyInput feeds one GWAA result into the diff/RPC orchestration.
// Previous is keyed by pair because M4 may have left tasks on several nodes.
type PlacementApplyInput struct {
	Task      *traincachev1alpha1.WarmupTask
	DataSet   *traincachev1alpha1.DataSet
	Windows   [][]*agentpb.ChunkRef
	Previous  map[PreheatPair]ExistingPreheat
	Placement gwaa.Output
	PodNames  map[int32]string
	// Fanout is the effective upper bound (0 means 1) and is only used for log
	// fields. FanoutMin is the effective lower bound (0 means 1).
	Fanout    int32
	FanoutMin int32
	// Metrics is optional; nil keeps the RPC orchestration metric-free.
	Metrics *metrics.Metrics
}

// PlacementActionKind is the RPC selected for one allocation difference.
type PlacementActionKind string

const (
	ActionSubmit PlacementActionKind = "Submit"
	ActionUpdate PlacementActionKind = "Update"
	ActionCancel PlacementActionKind = "Cancel"
)

// PlacementAction is one difference between Previous and Placement.
type PlacementAction struct {
	Kind     PlacementActionKind
	PodIndex int32
	Node     string
	Chunks   []*agentpb.ChunkRef
}

// PlacementApplyResult records the actions and their per-node outcome.
type PlacementApplyResult struct {
	Actions     []PlacementAction
	Submitted   int
	Updated     int
	Cancelled   int
	Unassigned  []int32
	Unavailable map[string]error
	Permanent   map[string]error
}

// PreheatRPC is the subset of agentclient.Pool used by placement. A fake in
// tests only needs these three methods.
type PreheatRPC interface {
	Submit(ctx context.Context, node string, req *agentpb.SubmitPreheatRequest) error
	Update(ctx context.Context, node string, req *agentpb.UpdatePreheatRequest) error
	Cancel(ctx context.Context, node string, req *agentpb.CancelPreheatRequest) error
}

// PlacementActions compares the desired GWAA assignment with the tasks the
// Agents confirmed they hold and returns only the RPCs needed:
//
//	new pair                     -> Submit
//	same pair, changed window    -> Update
//	same pair, same window       -> nothing
//	pair no longer desired       -> Cancel (including Pod moves)
//
// Pods without a feasible node are not submitted; they are reported through
// Placement.UnassignedPods and left to the caller's fallback (docs/7.0).
func PlacementActions(in PlacementApplyInput) ([]PlacementAction, error) {
	if in.Task == nil {
		return nil, errors.New("warmup: placement task is nil")
	}
	if in.Placement.Err != nil {
		return nil, fmt.Errorf("warmup: placement: %w", in.Placement.Err)
	}

	pods := make([]int32, 0, len(in.Placement.Assignment))
	for index := range in.Placement.Assignment {
		pods = append(pods, index)
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i] < pods[j] })

	desired := make(map[PreheatPair]struct{}, len(pods))
	actions := make([]PlacementAction, 0, len(pods)+len(in.Previous))
	for _, index := range pods {
		chunks, err := placementWindow(in.Windows, index)
		if err != nil {
			return nil, err
		}
		for _, node := range in.Placement.Assignment[index] {
			if node == "" {
				continue
			}
			pair := PreheatPair{PodIndex: index, Node: node}
			desired[pair] = struct{}{}
			if previous, ok := in.Previous[pair]; ok {
				if previous.Generation == uint64(in.Task.Generation) &&
					previous.BitmapBytes == bitmapBytes(len(chunks)) {
					continue
				}
				actions = append(actions, PlacementAction{
					Kind:     ActionUpdate,
					PodIndex: index,
					Node:     node,
					Chunks:   chunks,
				})
				continue
			}
			actions = append(actions, PlacementAction{
				Kind:     ActionSubmit,
				PodIndex: index,
				Node:     node,
				Chunks:   chunks,
			})
		}
	}

	pairs := make([]PreheatPair, 0, len(in.Previous))
	for pair := range in.Previous {
		pairs = append(pairs, pair)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].PodIndex != pairs[j].PodIndex {
			return pairs[i].PodIndex < pairs[j].PodIndex
		}
		return pairs[i].Node < pairs[j].Node
	})
	for _, pair := range pairs {
		if _, ok := desired[pair]; ok {
			continue
		}
		actions = append(actions, PlacementAction{
			Kind:     ActionCancel,
			PodIndex: pair.PodIndex,
			Node:     pair.Node,
		})
	}
	return actions, nil
}

// ApplyPlacement runs only the differential RPCs from PlacementActions. A
// failed node is skipped for the rest of the pass; other nodes continue.
// Unassigned Pods are passed through untouched for the caller's fallback.
func ApplyPlacement(ctx context.Context, rpc PreheatRPC, log *slog.Logger, in PlacementApplyInput) (PlacementApplyResult, error) {
	result := PlacementApplyResult{
		Unavailable: map[string]error{},
		Permanent:   map[string]error{},
	}
	actions, err := PlacementActions(in)
	if err != nil {
		return result, err
	}
	result.Actions = actions
	result.Unassigned = append([]int32(nil), in.Placement.UnassignedPods...)
	if log == nil {
		log = slog.Default()
	}
	if in.DataSet == nil {
		for _, action := range actions {
			if action.Kind == ActionSubmit {
				return result, errors.New("warmup: placement DataSet is nil")
			}
		}
	}

	taskKey := TaskKey(in.Task)
	fanoutMax, fanoutMin, _ := normalizeFanoutRange(in.Fanout, in.FanoutMin)
	logPlacementChange(log, taskKey, in.Placement, fanoutMax, fanoutMin, actions)

	failed := make(map[string]struct{})
	for _, action := range actions {
		if _, skip := failed[action.Node]; skip {
			continue
		}
		taskID := TaskID(in.Task, action.PodIndex)
		fields := []any{
			"warmup_task", taskKey,
			"task_id", taskID,
			"pod", in.PodNames[action.PodIndex],
			"pod_index", action.PodIndex,
			"node", action.Node,
			"generation", in.Task.Generation,
		}
		var callErr error
		switch action.Kind {
		case ActionSubmit:
			// Empty snapshot_id = Agent CURRENT (7.x Agent contract; a
			// concrete id is rejected as InvalidArgument).
			callErr = rpc.Submit(ctx, action.Node, &agentpb.SubmitPreheatRequest{
				TaskId:     taskID,
				Node:       action.Node,
				Dataset:    in.DataSet.Spec.DatasetName,
				SnapshotId: "",
				Chunks:     action.Chunks,
				Priority:   agentpb.PreheatPriority_P0,
				Generation: uint64(in.Task.Generation),
			})
			fields = append(fields, "chunks", len(action.Chunks))
		case ActionUpdate:
			callErr = rpc.Update(ctx, action.Node, &agentpb.UpdatePreheatRequest{
				TaskId:     taskID,
				Node:       action.Node,
				Generation: uint64(in.Task.Generation),
				Chunks:     action.Chunks,
				Priority:   agentpb.PreheatPriority_P0,
			})
			fields = append(fields, "chunks", len(action.Chunks))
		case ActionCancel:
			callErr = rpc.Cancel(ctx, action.Node, &agentpb.CancelPreheatRequest{
				TaskId:     taskID,
				Node:       action.Node,
				Generation: uint64(in.Task.Generation),
			})
			fields = append(fields, "prune_reason", pruneReason(in.Placement, action.PodIndex, action.Node))
		default:
			return result, fmt.Errorf("warmup: unknown placement action %q", action.Kind)
		}
		if callErr != nil && action.Kind == ActionCancel && errors.Is(callErr, agentclient.ErrTaskNotFound) {
			// The Agent already dropped this task (TTL, restart, earlier
			// cancel), so cancelling it again is an idempotent no-op.
			log.Debug("agent cancel skipped",
				"warmup_task", taskKey, "task_id", taskID, "node", action.Node,
				"reason", "task not found on agent")
			callErr = nil
		}
		op, _ := preheatOp(action.Kind)
		if callErr == nil {
			in.Metrics.ObservePreheatRPC(op, metrics.RPCOK)
			countAction(&result, action.Kind)
			log.Info("agent "+strings.ToLower(string(action.Kind)), fields...)
			continue
		}
		if errors.Is(callErr, agentclient.ErrPermanent) {
			in.Metrics.ObservePreheatRPC(op, metrics.RPCPermanent)
		} else {
			in.Metrics.ObservePreheatRPC(op, metrics.RPCUnavailable)
		}

		fields = append(fields, "err", callErr)
		failed[action.Node] = struct{}{}
		if errors.Is(callErr, agentclient.ErrPermanent) {
			result.Permanent[action.Node] = callErr
			log.Error("agent "+strings.ToLower(string(action.Kind))+" rejected", fields...)
		} else {
			result.Unavailable[action.Node] = callErr
			log.Warn("agent "+strings.ToLower(string(action.Kind))+" failed", fields...)
		}
	}

	if len(result.Unavailable) > 0 {
		return result, fmt.Errorf("%w: %d node(s) failed", agentclient.ErrAgentUnavailable, len(result.Unavailable))
	}
	return result, nil
}

// dropResubmittedMarks clears the poll seq high-water mark of every pair this
// round (re)created or resized a task on, as long as the RPC reached the Agent.
// A resubmit makes the Agent restart the task with seq 1; without the reset the
// mark keeps discarding that node's reports as stale and the gate stops seeing
// progress it is really making (docs/7.1 §7.4, D46).
func (r *Reconciler) dropResubmittedMarks(task *traincachev1alpha1.WarmupTask, applied PlacementApplyResult) {
	for _, action := range applied.Actions {
		if action.Kind != ActionSubmit && action.Kind != ActionUpdate {
			continue
		}
		if _, failed := applied.Unavailable[action.Node]; failed {
			continue
		}
		if _, rejected := applied.Permanent[action.Node]; rejected {
			continue
		}
		r.progress.forget(string(task.UID), action.PodIndex, action.Node)
	}
}

func preheatOp(kind PlacementActionKind) (metrics.PreheatOp, bool) {
	switch kind {
	case ActionSubmit:
		return metrics.PreheatSubmit, true
	case ActionUpdate:
		return metrics.PreheatUpdate, true
	case ActionCancel:
		return metrics.PreheatCancel, true
	}
	return "", false
}

func countAction(result *PlacementApplyResult, kind PlacementActionKind) {
	switch kind {
	case ActionSubmit:
		result.Submitted++
	case ActionUpdate:
		result.Updated++
	case ActionCancel:
		result.Cancelled++
	}
}

func placementWindow(windows [][]*agentpb.ChunkRef, index int32) ([]*agentpb.ChunkRef, error) {
	if index < 0 || int(index) >= len(windows) {
		return nil, fmt.Errorf("warmup: no window for pod index %d", index)
	}
	return windows[index], nil
}

// bitmapBytes is the ready_bitmap length an Agent reports for an n-chunk
// window: ceil(n/8), with bit i bound to chunks[i] (docs/6.3 D56).
func bitmapBytes(n int) int32 {
	return int32((n + 7) / 8)
}

// logPlacementChange records the M6/M13/M14 log points: top-K assignment
// changes, rounds, per-Pod warm-node counts, the adaptive fanout decisions and
// the non-convergence fallback reason (docs/7.1 §6.5, §9.3, D39).
func logPlacementChange(log *slog.Logger, taskKey string, placement gwaa.Output, fanoutMax, fanoutMin int32, actions []PlacementAction) {
	assign := assignmentView(placement.Assignment)
	warmCounts := formatWarmNodeCounts(placement.Assignment)
	summary := summarizeFanout(placement, actions)
	base := []any{
		"warmup_task", taskKey,
		"fanout", fanoutMax,
		"fanout_min", fanoutMin,
		"fanout_max", fanoutMax,
		"assign", assign,
		"warm_node_count", warmCounts,
		"rounds", placement.Rounds,
	}
	base = append(base, summary.fields()...)
	if placement.FellBack {
		fields := append(append([]any(nil), base...),
			"reason", "round_limit_exceeded",
			"unassigned", len(placement.UnassignedPods),
		)
		log.Warn("gwaa assignment not converged, keeping first greedy result", fields...)
	}
	if len(actions) > 0 {
		fields := append(append([]any(nil), base...),
			"actions", len(actions),
			"reason", "desired assignment differs from submitted tasks",
			"unassigned", len(placement.UnassignedPods),
		)
		log.Info("gwaa assignment changed", fields...)
	}
}

// assignmentView renders the GWAA assignment as a structured log value:
// one group per Pod holding its node names, e.g. {"p0":["n0","n1"],"p1":["n2"]}.
// Pod index and node stay addressable on their own, so a query can read one
// Pod's nodes or count Pods per node without splitting a formatted string.
func assignmentView(assign map[int32][]string) map[string][]string {
	out := make(map[string][]string, len(assign))
	for index, nodes := range assign {
		out[fmt.Sprintf("p%d", index)] = nodes
	}
	return out
}

// formatWarmNodeCounts renders the per-Pod warm-node count for logs, e.g.
// "p0=2,p1=1". Pods without a feasible node are omitted.
func formatWarmNodeCounts(assign map[int32][]string) string {
	if len(assign) == 0 {
		return "none"
	}
	pods := make([]int32, 0, len(assign))
	for index := range assign {
		pods = append(pods, index)
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i] < pods[j] })
	parts := make([]string, 0, len(pods))
	for _, index := range pods {
		parts = append(parts, fmt.Sprintf("p%d=%d", index, len(assign[index])))
	}
	return strings.Join(parts, ",")
}

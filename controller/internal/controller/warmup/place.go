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
)

// PreheatPair identifies one (Pod, node) preheat task. task_id is derived from
// the Pod, so a Pod may legitimately have tasks on several nodes (M4 submits
// every candidate; M6 narrows that to the assigned node).
type PreheatPair struct {
	PodIndex int32
	Node     string
}

// ExistingPreheat is a previously submitted (Pod, node) task, the window it
// carried and the generation it was submitted with. Chunks and Generation are
// compared against the current request to decide whether an Update is needed;
// a changed generation always requires an Update so the Agent never lags.
// Generation 0 means "unknown" and is treated as current for compatibility.
type ExistingPreheat struct {
	Chunks     []*agentpb.ChunkRef
	Generation uint64
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

// PlacementActions compares the desired GWAA assignment with the tasks already
// submitted and returns only the RPCs needed:
//
//	new pair                     -> Submit
//	same pair, changed window    -> Update
//	same pair, same window       -> nothing
//	pair no longer desired       -> Cancel (including Pod moves)
//
// Pods without a feasible node are not submitted; they are reported through
// Placement.UnassignedPods and left to the caller's fallback (docs/8.0).
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
		node := in.Placement.Assignment[index]
		if node == "" {
			continue
		}
		pair := PreheatPair{PodIndex: index, Node: node}
		desired[pair] = struct{}{}
		chunks, err := placementWindow(in.Windows, index)
		if err != nil {
			return nil, err
		}
		if previous, ok := in.Previous[pair]; ok {
			// Generation 0 means the submitted generation is unknown, so it
			// cannot be trusted as current and forces an Update.
			generationChanged := previous.Generation != uint64(in.Task.Generation)
			if chunkRefsEqual(previous.Chunks, chunks) && !generationChanged {
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
	logPlacementChange(log, taskKey, in.Placement, len(actions))

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
		default:
			return result, fmt.Errorf("warmup: unknown placement action %q", action.Kind)
		}
		if callErr == nil {
			countAction(&result, action.Kind)
			log.Info("agent "+strings.ToLower(string(action.Kind)), fields...)
			continue
		}

		fields = append(fields, "attempt", 1, "err", callErr)
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

func chunkRefsEqual(a, b []*agentpb.ChunkRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].GetPath() != b[i].GetPath() || a[i].GetChunk() != b[i].GetChunk() {
			return false
		}
	}
	return true
}

// logPlacementChange records the M6 log points: assignment changes, rounds and
// the non-convergence fallback reason (docs/8.1 §9.3).
func logPlacementChange(log *slog.Logger, taskKey string, placement gwaa.Output, actions int) {
	assign := formatAssignment(placement.Assignment)
	if placement.FellBack {
		log.Warn("gwaa assignment not converged, keeping first greedy result",
			"warmup_task", taskKey,
			"assign", assign,
			"rounds", placement.Rounds,
			"reason", "round_limit_exceeded",
			"unassigned", len(placement.UnassignedPods),
		)
	}
	if actions > 0 {
		log.Info("gwaa assignment changed",
			"warmup_task", taskKey,
			"assign", assign,
			"rounds", placement.Rounds,
			"actions", actions,
			"reason", "desired assignment differs from submitted tasks",
			"unassigned", len(placement.UnassignedPods),
		)
	}
}

func formatAssignment(assign map[int32]string) string {
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
		parts = append(parts, fmt.Sprintf("p%d=%s", index, assign[index]))
	}
	return strings.Join(parts, ",")
}

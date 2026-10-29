package warmup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
	"traincache/controller/internal/metrics"
)

// submitResult records per-node failures so the controller can report them
// without stopping submission to healthy nodes.
type submitResult struct {
	Submitted   int
	Unavailable map[string]error
	Permanent   map[string]error
}

func newSubmitResult() submitResult {
	return submitResult{
		Unavailable: map[string]error{},
		Permanent:   map[string]error{},
	}
}

// submitToCandidates sends the window of every Pod to every candidate Node.
// A failed Node is skipped for the rest of this pass; other Nodes continue.
// Agent tasks are keyed by task_id, so replaying a reconcile is idempotent.
func submitToCandidates(
	ctx context.Context,
	pool agentclient.Pool,
	metric *metrics.Metrics,
	log *slog.Logger,
	task *traincachev1alpha1.WarmupTask,
	ds *traincachev1alpha1.DataSet,
	windows [][]*agentpb.ChunkRef,
	candidates []string,
	podNames map[int32]string,
) (submitResult, error) {
	result := newSubmitResult()
	if log == nil {
		log = slog.Default()
	}

	taskKey := TaskKey(task)
	for _, node := range candidates {
		for index := range windows {
			taskID := TaskID(task, int32(index))
			podName := podNames[int32(index)]
			// snapshot_id must stay empty: the 7.x Agent contract (M3) only
			// supports CURRENT and rejects any concrete id with
			// InvalidArgument. DataSet validation already resolved and froze
			// ds.Status.SnapshotID against CURRENT.
			req := &agentpb.SubmitPreheatRequest{
				TaskId:     taskID,
				Node:       node,
				Dataset:    ds.Spec.DatasetName,
				SnapshotId: "",
				Chunks:     windows[index],
				Priority:   agentpb.PreheatPriority_P0,
				Generation: uint64(task.Generation),
			}
			if err := pool.Submit(ctx, node, req); err != nil {
				if errors.Is(err, agentclient.ErrPermanent) {
					metric.ObservePreheatRPC(metrics.PreheatSubmit, metrics.RPCPermanent)
				} else {
					metric.ObservePreheatRPC(metrics.PreheatSubmit, metrics.RPCUnavailable)
				}
				logFields := []any{
					"warmup_task", taskKey,
					"task_id", taskID,
					"pod", podName,
					"pod_index", index,
					"node", node,
					"generation", task.Generation,
					"err", err,
				}
				if errors.Is(err, agentclient.ErrPermanent) {
					result.Permanent[node] = err
					log.Error("agent submit rejected", logFields...)
				} else {
					result.Unavailable[node] = err
					log.Warn("agent submit failed", logFields...)
				}
				break
			}
			metric.ObservePreheatRPC(metrics.PreheatSubmit, metrics.RPCOK)
			result.Submitted++
			log.Info("agent submit",
				"warmup_task", taskKey,
				"task_id", taskID,
				"pod", podName,
				"pod_index", index,
				"node", node,
				"generation", task.Generation,
				"chunks", len(req.Chunks),
			)
		}
	}

	if len(result.Unavailable) > 0 {
		return result, fmt.Errorf("%w: %d candidate node(s) failed", agentclient.ErrAgentUnavailable, len(result.Unavailable))
	}
	return result, nil
}

func podDetail(task *traincachev1alpha1.WarmupTask, index int32) (traincachev1alpha1.PodDetail, bool) {
	if task == nil {
		return traincachev1alpha1.PodDetail{}, false
	}
	for i := range task.Spec.PodDetails {
		if task.Spec.PodDetails[i].Index == index {
			return task.Spec.PodDetails[i], true
		}
	}
	return traincachev1alpha1.PodDetail{}, false
}

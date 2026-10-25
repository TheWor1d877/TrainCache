package warmup

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// TestReconcileDeviationRecordsActualNodeAndResubmits covers docs/7.1 §7.8:
// a released Pod on a node other than its assignment writes ActualNode, emits
// NodeDeviation and submits the same window on the actual node.
func TestReconcileDeviationRecordsActualNodeAndResubmits(t *testing.T) {
	task := fixtureTask(1, 1, [][]int32{{0}})
	task.Status.Phase = traincachev1alpha1.WarmupPhaseReleased
	task.Status.PodNodeReadiness = []traincachev1alpha1.PodNodeReadiness{
		{PodIndex: 0, NodeName: "n1"},
	}
	ds := fixtureDataSet(1, singleShardLayout(1), []string{"n1", "n2"})
	pod := fixtureTargetPod(0)
	pod.Spec.NodeName = "n2"
	pool := &fakeAgentPool{}
	recorder := record.NewFakeRecorder(4)
	c := fixtureClient(t, task, ds, pod)
	r := &Reconciler{
		Client:   c,
		Agents:   pool,
		Recorder: recorder,
		Now:      func() time.Time { return time.Unix(100, 0) },
	}

	r.reconcileDeviation(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), task)

	if got := task.Status.PodNodeReadiness[0].ActualNode; got != "n2" {
		t.Fatalf("actualNode = %q, want n2", got)
	}
	records := pool.snapshot()
	if len(records) != 1 || records[0].node != "n2" || records[0].req.TaskId != "train/t1/p0" {
		t.Fatalf("submits = %#v, want one Submit on n2", records)
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, EventNodeDeviation) {
			t.Fatalf("event = %q, want %s", event, EventNodeDeviation)
		}
	default:
		t.Fatalf("no %s event recorded", EventNodeDeviation)
	}

	// A second pass must not emit a duplicate transition event.
	r.reconcileDeviation(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), task)
	select {
	case event := <-recorder.Events:
		t.Fatalf("duplicate event = %q", event)
	default:
	}
}

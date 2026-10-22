package v1alpha1

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	m12StatusPods       = 64  // Q11 upper bound
	m12StatusNodes      = 256 // Q11 upper bound
	m12StatusChunksPerP = 8   // e2e manifest K (1 MiB chunks)
	m12StatusFanout     = 2   // M13 typical spec.preheatFanout
	m12StatusLimit      = 1 << 20
)

// TestM12WarmupTaskStatusSize is S15 (docs/8.2 plan.md): the worst WarmupTask
// status at the Q11 limits P=64, N=256 must serialize below 1 MiB.
//
// Every field that the controller writes to PodNodeReadiness/status is
// populated, with one PodNodeReadiness entry per (Pod, preheat node) pair:
// P x spec.preheatFanout entries (M13); the test deliberately does not trim
// optional fields to make the limit pass. The per-Pod window K is 8
// (controller/e2e/manifests/warmuptask.yaml); larger K only increases
// ReadyBlockBitmap and the table values, so this is a conservative lower bound
// on the worst case.
func TestM12WarmupTaskStatusSize(t *testing.T) {
	status := m12WorstCaseStatus()

	payload, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	tablePayload, err := json.Marshal(status.ReadinessTable)
	if err != nil {
		t.Fatalf("marshal readinessTable: %v", err)
	}
	entriesPayload, err := json.Marshal(status.PodNodeReadiness)
	if err != nil {
		t.Fatalf("marshal podNodeReadiness: %v", err)
	}
	t.Logf("S15 status size: %d bytes (%.2f MiB), limit %d bytes; readinessTable=%d bytes, podNodeReadiness=%d bytes",
		len(payload), float64(len(payload))/(1<<20), m12StatusLimit,
		len(tablePayload), len(entriesPayload))
	if len(payload) >= m12StatusLimit {
		t.Fatalf("S15 FAIL: P=%d N=%d status serializes to %d bytes (%.2f MiB), want < %d bytes",
			m12StatusPods, m12StatusNodes, len(payload),
			float64(len(payload))/(1<<20), m12StatusLimit)
	}
	if len(status.PodNodeReadiness) != m12StatusPods*m12StatusFanout {
		t.Fatalf("podNodeReadiness entries = %d, want P x fanout (%d)",
			len(status.PodNodeReadiness), m12StatusPods*m12StatusFanout)
	}
}

func m12WorstCaseStatus() WarmupTaskStatus {
	now := metav1.NewTime(time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC))
	candidates := make([]string, m12StatusNodes)
	for node := range candidates {
		candidates[node] = fmt.Sprintf("gpu-worker-node-%03d", node)
	}

	table := make([][]int32, m12StatusPods)
	entries := make([]PodNodeReadiness, 0, m12StatusPods*m12StatusFanout)
	rates := make([]PodRate, 0, m12StatusPods)
	bitmap := make([]byte, (m12StatusChunksPerP+7)/8)
	for i := range bitmap {
		bitmap[i] = 0xFF
	}

	for pod := 0; pod < m12StatusPods; pod++ {
		table[pod] = make([]int32, m12StatusNodes)
		for node := range table[pod] {
			table[pod][node] = m12StatusChunksPerP
		}
		actual := candidates[pod%len(candidates)]
		for slot := 0; slot < m12StatusFanout; slot++ {
			node := candidates[(pod*m12StatusFanout+slot)%len(candidates)]
			entries = append(entries, PodNodeReadiness{
				PodIndex:               int32(pod),
				PodName:                fmt.Sprintf("trainer-pod-%02d", pod),
				NodeName:               node,
				ActualNode:             actual,
				ReadyBlockBitmap:       append([]byte(nil), bitmap...),
				StartupWindowReady:     m12StatusChunksPerP,
				ReadyInWindow:          m12StatusChunksPerP,
				RequiredReady:          7,
				StartupWindowReadyRate: 1,
				GlobalDecayScore:       m12StatusChunksPerP,
				State:                  PreheatStateReady,
				LastReportTime:         &now,
			})
		}
		rates = append(rates, PodRate{
			PodIndex: int32(pod),
			Rate:     1,
			Gap:      0.1,
			Slope:    0.001,
		})
	}

	return WarmupTaskStatus{
		ObservedGeneration: 7,
		Conditions: []metav1.Condition{{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "GateHold",
			Message:            "waiting for enough ready blocks on the assigned nodes",
			LastTransitionTime: now,
		}},
		Phase:              WarmupPhaseGating,
		ConsumeOrderSource: ConsumeOrderSourceDefault,
		StartedAt:          &now,
		CandidateNodes:     candidates,
		ReadinessTable:     table,
		PodNodeReadiness:   entries,
		GateDecision: GateDecision{
			State:     GateStateHold,
			Reason:    "consecutive-rate",
			DecidedAt: &now,
			PodRates:  rates,
		},
		LastUpdated: &now,
	}
}

// Package webhook implements the TrainCache Pod mutating admission handler.
// It injects the scheduling gate and WarmupTask binding annotations defined by
// docs/7.1 design.md §5.7/§6.3 (D8-D10). It never rejects a Pod.
package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/controller/warmup"
)

// MutatePodPath is the HTTP path registered on the controller-runtime webhook
// server. The MutatingWebhookConfiguration (owned by deployment manifests)
// must point clientConfig.service.path at this value.
const MutatePodPath = "/mutate-v1-pod"

// PodMutator handles CREATE admission for Pods selected by
// objectSelector traincache.io/warmup=enabled.
//
// Client and Log are the frozen 8.1 fields. Recorder and GateName are optional
// M9 extensions: Recorder emits WarmupTaskMappingFailed events and GateName
// defaults to the frozen traincache.io/warmup gate.
type PodMutator struct {
	Client   client.Client
	Log      *slog.Logger
	Recorder record.EventRecorder
	GateName string
	Now      func() time.Time
}

// Handle implements admission.Handler. Every error path returns Allowed so a
// webhook fault can never block Pod creation (failurePolicy: Ignore).
func (m *PodMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	start := m.now()
	requestID := fmt.Sprintf("%s/%d/%d", req.UID, 0, start.UnixNano())
	log := m.logger().With("request_id", requestID)

	if req.Operation != "" && req.Operation != admissionv1.Create {
		log.Debug("pod admission skipped", "reason", "not a create")
		return admission.Allowed("only CREATE is mutated")
	}

	pod := &corev1.Pod{}
	if len(req.Object.Raw) == 0 {
		log.Warn("pod admission skipped", "reason", "empty object", "namespace", req.Namespace)
		return admission.Allowed("empty object")
	}
	if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
		log.Warn("pod admission skipped", "reason", "decode failed", "namespace", req.Namespace, "err", err)
		return admission.Allowed("cannot decode pod")
	}
	if pod.Namespace == "" {
		pod.Namespace = req.Namespace
	}
	log = log.With("pod", pod.Name, "namespace", pod.Namespace)

	if pod.Labels[warmup.WarmupEnabledLabel] != warmup.WarmupEnabledValue {
		log.Warn("pod injection skipped", "reason", "missing warmup=enabled label")
		return admission.Allowed("pod is not selected")
	}

	list := &traincachev1alpha1.WarmupTaskList{}
	if m.Client == nil {
		log.Warn("pod injection skipped", "reason", "client is nil")
		return admission.Allowed("task lookup unavailable")
	}
	if err := m.Client.List(ctx, list, client.InNamespace(pod.Namespace)); err != nil {
		log.Warn("pod injection skipped", "reason", "task lookup failed", "err", err)
		return admission.Allowed("task lookup failed")
	}

	task := warmup.ReadyTaskForPod(list.Items, pod)
	if task == nil {
		log.Warn("pod injection skipped", "reason", "no matching WarmupTask")
		return admission.Allowed("no matching task")
	}
	taskKey := warmup.TaskKey(task)

	index, err := warmup.ResolvePodIndex(pod)
	if err != nil {
		m.degrade(ctx, log, task, pod, "pod index resolution failed", err)
		return admission.Allowed("pod index resolution failed")
	}
	if _, err := warmup.BindPodIndex(task, index); err != nil {
		m.degrade(ctx, log, task, pod, "pod index does not bind to WarmupTask", err)
		return admission.Allowed("pod index does not bind to task")
	}

	gateName := m.GateName
	if gateName == "" {
		gateName = warmup.DefaultGateName
	}
	changed := warmup.ApplyInjection(pod, task, index, gateName)
	// DataLoader 契约的环境变量与门控一起注入；两者任一改动都要重新编码 Pod。
	if warmup.InjectDataloaderEnv(pod, task, index) {
		changed = true
	}
	if !changed {
		log.Info("gate injection skipped; already injected",
			"warmup_task", taskKey,
			"pod_index", index,
			"gate", gateName,
		)
		return admission.Allowed("already injected")
	}

	mutated, err := json.Marshal(pod)
	if err != nil {
		m.degrade(ctx, log, task, pod, "marshal mutated pod failed", err)
		return admission.Allowed("cannot encode mutation")
	}
	resp := admission.PatchResponseFromRaw(req.Object.Raw, mutated)
	if !resp.Allowed {
		log.Warn("gate injection failed; pod allowed",
			"warmup_task", taskKey,
			"pod_index", index,
			"reason", "patch generation failed",
			"err", resp.Result.Message,
		)
		return admission.Allowed("patch generation failed")
	}

	log.Info("gate injected",
		"warmup_task", taskKey,
		"pod_index", index,
		"gate", gateName,
		"duration_ms", m.now().Sub(start).Milliseconds(),
	)
	return resp
}

// degrade emits the mapping-failure event and records the degraded condition.
// Both paths are best effort and never alter the admission decision.
func (m *PodMutator) degrade(ctx context.Context, log *slog.Logger, task *traincachev1alpha1.WarmupTask, pod *corev1.Pod, reason string, cause error) {
	log.Warn("pod injection degraded",
		"warmup_task", warmup.TaskKey(task),
		"reason", reason,
		"err", cause,
	)
	if m.Recorder != nil {
		m.Recorder.Eventf(pod, corev1.EventTypeWarning, warmup.EventWarmupTaskMappingFailed,
			"%s: %v", reason, cause)
	}
	if m.Client == nil {
		return
	}
	// Status writes must not extend admission latency unboundedly.
	writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := warmup.MarkGateInjectionDegraded(writeCtx, m.Client, log, task, pod.Name, reason+": "+cause.Error()); err != nil {
		log.Warn("record gate injection degraded condition failed",
			"warmup_task", warmup.TaskKey(task),
			"reason", reason,
			"err", err,
		)
	}
}

func (m *PodMutator) logger() *slog.Logger {
	if m.Log != nil {
		return m.Log
	}
	return slog.Default()
}

func (m *PodMutator) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

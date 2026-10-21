package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	evanjsonpatch "github.com/evanphx/json-patch/v5"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/controller/warmup"
)

func webhookScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := traincachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add traincache scheme: %v", err)
	}
	return scheme
}

func trainTask() *traincachev1alpha1.WarmupTask {
	return &traincachev1alpha1.WarmupTask{
		ObjectMeta: metav1.ObjectMeta{Name: "task-1", Namespace: "train"},
		Spec: traincachev1alpha1.WarmupTaskSpec{
			TargetRef: traincachev1alpha1.TargetRef{APIVersion: "batch/v1", Kind: "Job", Name: "trainer"},
			PodCount:  4,
			PodDetails: []traincachev1alpha1.PodDetail{
				{Index: 0, K: 8}, {Index: 1, K: 8}, {Index: 2, K: 8}, {Index: 3, K: 8},
			},
		},
	}
}

func selectedJobPod(index string) *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "trainer-abc",
			Namespace: "train",
			Labels: map[string]string{
				warmup.WarmupEnabledLabel:            warmup.WarmupEnabledValue,
				warmup.JobCompletionIndexLabel:       index,
				"batch.kubernetes.io/job-name":       "trainer",
				"batch.kubernetes.io/controller-uid": "uid",
			},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "trainer"}},
		},
	}
}

func newMutator(t *testing.T, objects ...runtime.Object) (*PodMutator, *record.FakeRecorder) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(webhookScheme(t)).
		WithStatusSubresource(&traincachev1alpha1.WarmupTask{}).
		WithRuntimeObjects(objects...).
		Build()
	recorder := record.NewFakeRecorder(8)
	return &PodMutator{
		Client:   c,
		Recorder: recorder,
		GateName: warmup.DefaultGateName,
		Now:      func() time.Time { return time.Unix(100, 0) },
	}, recorder
}

func admissionRequest(t *testing.T, pod *corev1.Pod) admission.Request {
	t.Helper()
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("marshal pod: %v", err)
	}
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		UID:       types.UID("req-1"),
		Operation: admissionv1.Create,
		Namespace: pod.Namespace,
		Object:    runtime.RawExtension{Raw: raw},
	}}
}

func applyPatches(t *testing.T, pod *corev1.Pod, resp admission.Response) *corev1.Pod {
	t.Helper()
	if !resp.Allowed {
		t.Fatalf("response not allowed: %#v", resp.Result)
	}
	if len(resp.Patches) == 0 {
		t.Fatal("expected JSON patches")
	}
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("marshal pod: %v", err)
	}
	patchBytes, err := json.Marshal(resp.Patches)
	if err != nil {
		t.Fatalf("marshal patches: %v", err)
	}
	patch, err := evanjsonpatch.DecodePatch(patchBytes)
	if err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	mutated, err := patch.Apply(raw)
	if err != nil {
		t.Fatalf("apply patch: %v", err)
	}
	out := &corev1.Pod{}
	if err := json.Unmarshal(mutated, out); err != nil {
		t.Fatalf("unmarshal mutated pod: %v", err)
	}
	return out
}

func TestHandleInjectsGateAndAnnotations(t *testing.T) {
	m, _ := newMutator(t, trainTask())
	pod := selectedJobPod("2")

	resp := m.Handle(context.Background(), admissionRequest(t, pod))
	mutated := applyPatches(t, pod, resp)

	if len(mutated.Spec.SchedulingGates) != 1 || mutated.Spec.SchedulingGates[0].Name != warmup.DefaultGateName {
		t.Fatalf("gates = %#v", mutated.Spec.SchedulingGates)
	}
	if got := mutated.Annotations[warmup.AnnotationWarmupTask]; got != "train/task-1" {
		t.Fatalf("warmup-task = %q", got)
	}
	if got := mutated.Annotations[warmup.AnnotationPodIndex]; got != "2" {
		t.Fatalf("pod-index = %q", got)
	}
}

func TestHandleIsIdempotent(t *testing.T) {
	m, _ := newMutator(t, trainTask())
	pod := selectedJobPod("2")

	first := m.Handle(context.Background(), admissionRequest(t, pod))
	mutated := applyPatches(t, pod, first)

	second := m.Handle(context.Background(), admissionRequest(t, mutated))
	if !second.Allowed || len(second.Patches) != 0 {
		t.Fatalf("second admission = allowed=%v patches=%d", second.Allowed, len(second.Patches))
	}
	if len(mutated.Spec.SchedulingGates) != 1 {
		t.Fatalf("gate duplicated: %#v", mutated.Spec.SchedulingGates)
	}
}

func TestHandleNoMatchingTaskDoesNotMutate(t *testing.T) {
	m, recorder := newMutator(t, trainTask())
	pod := selectedJobPod("0")
	pod.Name = "other-abc"
	pod.Labels["batch.kubernetes.io/job-name"] = "other"
	pod.OwnerReferences[0].Name = "other"

	resp := m.Handle(context.Background(), admissionRequest(t, pod))
	if !resp.Allowed || len(resp.Patches) != 0 {
		t.Fatalf("response = allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	select {
	case event := <-recorder.Events:
		t.Fatalf("unexpected event for unmatched pod: %q", event)
	default:
	}
}

func TestHandleNoTaskDoesNotInject(t *testing.T) {
	m, _ := newMutator(t)
	pod := selectedJobPod("0")

	resp := m.Handle(context.Background(), admissionRequest(t, pod))
	if !resp.Allowed || len(resp.Patches) != 0 {
		t.Fatalf("response = allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
}

func TestHandleSkipsUnselectedPod(t *testing.T) {
	m, _ := newMutator(t, trainTask())
	pod := selectedJobPod("0")
	delete(pod.Labels, warmup.WarmupEnabledLabel)

	resp := m.Handle(context.Background(), admissionRequest(t, pod))
	if !resp.Allowed || len(resp.Patches) != 0 {
		t.Fatalf("response = allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
}

func TestHandleIndexResolutionFailureIsNonBlockingAndDegraded(t *testing.T) {
	cases := []struct {
		name string
		pod  func() *corev1.Pod
	}{
		{
			name: "missing index",
			pod: func() *corev1.Pod {
				p := selectedJobPod("0")
				delete(p.Labels, warmup.JobCompletionIndexLabel)
				p.Name = "trainer"
				return p
			},
		},
		{
			name: "invalid index",
			pod: func() *corev1.Pod {
				return selectedJobPod("not-a-number")
			},
		},
		{
			name: "index outside pod count",
			pod: func() *corev1.Pod {
				return selectedJobPod("9")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, recorder := newMutator(t, trainTask())
			pod := tc.pod()

			resp := m.Handle(context.Background(), admissionRequest(t, pod))
			if !resp.Allowed || len(resp.Patches) != 0 {
				t.Fatalf("response = allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
			}
			select {
			case event := <-recorder.Events:
				if !strings.Contains(event, warmup.EventWarmupTaskMappingFailed) {
					t.Fatalf("event = %q", event)
				}
			default:
				t.Fatal("expected WarmupTaskMappingFailed event")
			}

			task := &traincachev1alpha1.WarmupTask{}
			if err := m.Client.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "task-1"}, task); err != nil {
				t.Fatalf("get task: %v", err)
			}
			cond := meta.FindStatusCondition(task.Status.Conditions, warmup.ConditionGateInjectionDegraded)
			if cond == nil || cond.Status != metav1.ConditionTrue {
				t.Fatalf("condition = %#v", cond)
			}
		})
	}
}

func TestHandleStatefulSetOrdinal(t *testing.T) {
	task := trainTask()
	task.Spec.TargetRef = traincachev1alpha1.TargetRef{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer"}
	m, _ := newMutator(t, task)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "trainer-3",
			Namespace: "train",
			Labels:    map[string]string{warmup.WarmupEnabledLabel: warmup.WarmupEnabledValue},
		},
	}

	mutated := applyPatches(t, pod, m.Handle(context.Background(), admissionRequest(t, pod)))
	if got := mutated.Annotations[warmup.AnnotationPodIndex]; got != "3" {
		t.Fatalf("pod-index = %q", got)
	}
	if got := mutated.Annotations[warmup.AnnotationWarmupTask]; got != "train/task-1" {
		t.Fatalf("warmup-task = %q", got)
	}
}

func TestHandleSkipsNonCreate(t *testing.T) {
	m, _ := newMutator(t, trainTask())
	pod := selectedJobPod("0")
	req := admissionRequest(t, pod)
	req.Operation = admissionv1.Update

	resp := m.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) != 0 {
		t.Fatalf("response = allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
}

func TestHandleLogsRequestFields(t *testing.T) {
	var buf bytes.Buffer
	m, _ := newMutator(t, trainTask())
	m.Log = slog.New(slog.NewJSONHandler(&buf, nil))
	pod := selectedJobPod("2")

	resp := m.Handle(context.Background(), admissionRequest(t, pod))
	if !resp.Allowed {
		t.Fatalf("response not allowed: %#v", resp.Result)
	}

	var injected map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("invalid log line %q: %v", line, err)
		}
		if entry["msg"] == "gate injected" {
			injected = entry
			break
		}
	}
	if injected == nil {
		t.Fatalf("missing gate injected log: %s", buf.String())
	}
	if injected["request_id"] == "" || injected["request_id"] == nil {
		t.Fatalf("missing request_id: %#v", injected)
	}
	if injected["pod"] != pod.Name {
		t.Fatalf("pod = %v", injected["pod"])
	}
	if injected["warmup_task"] != "train/task-1" {
		t.Fatalf("warmup_task = %v", injected["warmup_task"])
	}
}

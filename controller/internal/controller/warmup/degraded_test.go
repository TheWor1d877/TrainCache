package warmup

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

func testScheme(t *testing.T) *runtime.Scheme {
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

func testTask(name string, ref traincachev1alpha1.TargetRef) *traincachev1alpha1.WarmupTask {
	return &traincachev1alpha1.WarmupTask{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "train"},
		Spec: traincachev1alpha1.WarmupTaskSpec{
			TargetRef: ref,
			PodCount:  4,
			PodDetails: []traincachev1alpha1.PodDetail{
				{Index: 0, K: 8},
				{Index: 1, K: 8},
				{Index: 2, K: 8},
				{Index: 3, K: 8},
			},
		},
	}
}

func selectedPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "train",
			Labels:    map[string]string{WarmupEnabledLabel: WarmupEnabledValue},
		},
	}
}

func TestTargetMatchesPodTable(t *testing.T) {
	cases := []struct {
		name string
		ref  traincachev1alpha1.TargetRef
		pod  *corev1.Pod
		want bool
	}{
		{
			name: "job owner reference",
			ref:  traincachev1alpha1.TargetRef{APIVersion: "batch/v1", Kind: "Job", Name: "trainer"},
			pod: func() *corev1.Pod {
				p := selectedPod("trainer-0-abc")
				p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "trainer"}}
				return p
			}(),
			want: true,
		},
		{
			name: "job name label",
			ref:  traincachev1alpha1.TargetRef{Kind: "Job", Name: "trainer"},
			pod: func() *corev1.Pod {
				p := selectedPod("trainer-2-x")
				p.Labels["batch.kubernetes.io/job-name"] = "trainer"
				return p
			}(),
			want: true,
		},
		{
			name: "statefulset ordinal name",
			ref:  traincachev1alpha1.TargetRef{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer"},
			pod:  selectedPod("trainer-3"),
			want: true,
		},
		{
			name: "statefulset owner",
			ref:  traincachev1alpha1.TargetRef{Kind: "StatefulSet", Name: "trainer"},
			pod: func() *corev1.Pod {
				p := selectedPod("trainer-x")
				p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer"}}
				return p
			}(),
			want: true,
		},
		{
			name: "standalone pod",
			ref:  traincachev1alpha1.TargetRef{APIVersion: "v1", Kind: "Pod", Name: "trainer"},
			pod: func() *corev1.Pod {
				p := selectedPod("trainer")
				p.APIVersion = "v1"
				return p
			}(),
			want: true,
		},
		{
			name: "name mismatch",
			ref:  traincachev1alpha1.TargetRef{Kind: "Job", Name: "other"},
			pod: func() *corev1.Pod {
				p := selectedPod("trainer-0")
				p.Labels["batch.kubernetes.io/job-name"] = "trainer"
				return p
			}(),
			want: false,
		},
		{
			name: "kind mismatch",
			ref:  traincachev1alpha1.TargetRef{Kind: "Job", Name: "trainer"},
			pod:  selectedPod("trainer-0"),
			want: false,
		},
		{
			name: "apiVersion mismatch on owner",
			ref:  traincachev1alpha1.TargetRef{APIVersion: "batch/v2", Kind: "Job", Name: "trainer"},
			pod: func() *corev1.Pod {
				p := selectedPod("trainer-0")
				p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "trainer"}}
				return p
			}(),
			want: false,
		},
		{
			name: "pod label selector mismatch",
			ref: traincachev1alpha1.TargetRef{
				Kind:             "Pod",
				Name:             "trainer",
				PodLabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "worker"}},
			},
			pod:  selectedPod("trainer"),
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := testTask("task", tc.ref)
			if got := TargetMatchesPod(task, tc.pod); got != tc.want {
				t.Fatalf("TargetMatchesPod() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReadyTaskForPodFiltersTerminalPhases(t *testing.T) {
	pod := selectedPod("trainer-0")
	pod.Labels["batch.kubernetes.io/job-name"] = "trainer"
	ref := traincachev1alpha1.TargetRef{Kind: "Job", Name: "trainer"}

	pending := testTask("pending", ref)
	if got := ReadyTaskForPod([]traincachev1alpha1.WarmupTask{*pending}, pod); got == nil {
		t.Fatal("empty-status task should be ready")
	}

	released := testTask("released", ref)
	released.Status.Phase = traincachev1alpha1.WarmupPhaseReleased
	if got := ReadyTaskForPod([]traincachev1alpha1.WarmupTask{*released}, pod); got != nil {
		t.Fatalf("released task should not be selected: %#v", got)
	}

	cond := testTask("cond", ref)
	cond.Status.Conditions = []metav1.Condition{{
		Type: "Ready", Status: metav1.ConditionFalse, Reason: "NotReady",
		LastTransitionTime: metav1.Now(),
	}}
	if got := ReadyTaskForPod([]traincachev1alpha1.WarmupTask{*cond}, pod); got != nil {
		t.Fatalf("Ready=False task should not be selected: %#v", got)
	}
}

func TestResolvePodIndex(t *testing.T) {
	cases := []struct {
		name    string
		pod     *corev1.Pod
		want    int32
		wantErr bool
	}{
		{
			name: "job completion index label",
			pod: func() *corev1.Pod {
				p := selectedPod("trainer-x")
				p.Labels[JobCompletionIndexLabel] = "2"
				return p
			}(),
			want: 2,
		},
		{
			name: "statefulset ordinal",
			pod:  selectedPod("trainer-3"),
			want: 3,
		},
		{
			name: "malformed label does not fall back to ordinal",
			pod: func() *corev1.Pod {
				p := selectedPod("trainer-3")
				p.Labels[JobCompletionIndexLabel] = "three"
				return p
			}(),
			wantErr: true,
		},
		{
			name:    "no ordinal suffix",
			pod:     selectedPod("trainer"),
			wantErr: true,
		},
		{
			name:    "non numeric suffix",
			pod:     selectedPod("trainer-abc"),
			wantErr: true,
		},
		{
			name:    "trailing dash",
			pod:     selectedPod("trainer-"),
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolvePodIndex(tc.pod)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ResolvePodIndex() = %d, want error", got)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("ResolvePodIndex() = (%d, %v), want %d", got, err, tc.want)
			}
		})
	}
}

func TestBindPodIndex(t *testing.T) {
	task := testTask("task", traincachev1alpha1.TargetRef{Kind: "Job", Name: "trainer"})
	if detail, err := BindPodIndex(task, 2); err != nil || detail.Index != 2 {
		t.Fatalf("BindPodIndex(2) = (%#v, %v)", detail, err)
	}
	if _, err := BindPodIndex(task, 4); err == nil {
		t.Fatal("BindPodIndex(4) should fail for PodCount=4")
	}
	if _, err := BindPodIndex(task, -1); err == nil {
		t.Fatal("BindPodIndex(-1) should fail")
	}

	// A dense index without a matching PodDetail cannot be bound.
	task.Spec.PodDetails = task.Spec.PodDetails[:2]
	if _, err := BindPodIndex(task, 2); err == nil {
		t.Fatal("BindPodIndex(2) should fail without PodDetail")
	}
}

func TestApplyInjectionIdempotent(t *testing.T) {
	task := testTask("t1", traincachev1alpha1.TargetRef{Kind: "StatefulSet", Name: "trainer"})
	pod := selectedPod("trainer-1")

	if !ApplyInjection(pod, task, 1, DefaultGateName) {
		t.Fatal("first ApplyInjection should change the pod")
	}
	if len(pod.Spec.SchedulingGates) != 1 || pod.Spec.SchedulingGates[0].Name != DefaultGateName {
		t.Fatalf("gates = %#v", pod.Spec.SchedulingGates)
	}
	if pod.Annotations[AnnotationWarmupTask] != "train/t1" || pod.Annotations[AnnotationPodIndex] != "1" {
		t.Fatalf("annotations = %#v", pod.Annotations)
	}
	if ApplyInjection(pod, task, 1, DefaultGateName) {
		t.Fatal("second ApplyInjection should be a no-op")
	}
	if len(pod.Spec.SchedulingGates) != 1 {
		t.Fatalf("gate duplicated: %#v", pod.Spec.SchedulingGates)
	}
}

func TestMarkGateInjectionDegradedIdempotent(t *testing.T) {
	task := testTask("t1", traincachev1alpha1.TargetRef{Kind: "Job", Name: "trainer"})
	task.Generation = 3
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&traincachev1alpha1.WarmupTask{}).
		WithObjects(task).
		Build()

	ctx := context.Background()
	if err := MarkGateInjectionDegraded(ctx, c, nil, task, "trainer-0", "index missing"); err != nil {
		t.Fatalf("first MarkGateInjectionDegraded: %v", err)
	}
	updated := &traincachev1alpha1.WarmupTask{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "train", Name: "t1"}, updated); err != nil {
		t.Fatalf("get task: %v", err)
	}
	cond := meta.FindStatusCondition(updated.Status.Conditions, ConditionGateInjectionDegraded)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.ObservedGeneration != 3 {
		t.Fatalf("condition = %#v", cond)
	}
	if !strings.Contains(cond.Message, "trainer-0") {
		t.Fatalf("condition message = %q", cond.Message)
	}

	before := updated.DeepCopy()
	if err := MarkGateInjectionDegraded(ctx, c, nil, updated, "trainer-0", "index missing"); err != nil {
		t.Fatalf("second MarkGateInjectionDegraded: %v", err)
	}
	again := &traincachev1alpha1.WarmupTask{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "train", Name: "t1"}, again); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if !meta.IsStatusConditionTrue(again.Status.Conditions, ConditionGateInjectionDegraded) ||
		again.Status.Conditions[0].LastTransitionTime != before.Status.Conditions[0].LastTransitionTime {
		t.Fatalf("condition not idempotent: before=%#v after=%#v", before.Status.Conditions, again.Status.Conditions)
	}
}

func TestDegradedReconcilerWritesCondition(t *testing.T) {
	task := testTask("t1", traincachev1alpha1.TargetRef{Kind: "Job", Name: "trainer"})
	pod := selectedPod("trainer-0")
	pod.Labels["batch.kubernetes.io/job-name"] = "trainer"
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&traincachev1alpha1.WarmupTask{}).
		WithObjects(task, pod).
		Build()

	recorder := record.NewFakeRecorder(4)
	r := &DegradedReconciler{Client: c, Recorder: recorder}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "train", Name: "trainer-0"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	updated := &traincachev1alpha1.WarmupTask{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "t1"}, updated); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if !meta.IsStatusConditionTrue(updated.Status.Conditions, ConditionGateInjectionDegraded) {
		t.Fatalf("conditions = %#v", updated.Status.Conditions)
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, ConditionGateInjectionDegraded) {
			t.Fatalf("event = %q", event)
		}
	default:
		t.Fatal("expected degraded event")
	}
}

func TestDegradedReconcilerSkipsInjectedPod(t *testing.T) {
	task := testTask("t1", traincachev1alpha1.TargetRef{Kind: "Job", Name: "trainer"})
	pod := selectedPod("trainer-0")
	pod.Labels["batch.kubernetes.io/job-name"] = "trainer"
	pod.Annotations = map[string]string{AnnotationWarmupTask: "train/t1", AnnotationPodIndex: "0"}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&traincachev1alpha1.WarmupTask{}).
		WithObjects(task, pod).
		Build()

	r := &DegradedReconciler{Client: c}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "train", Name: "trainer-0"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	updated := &traincachev1alpha1.WarmupTask{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "t1"}, updated); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if meta.FindStatusCondition(updated.Status.Conditions, ConditionGateInjectionDegraded) != nil {
		t.Fatalf("injected pod must not be marked degraded: %#v", updated.Status.Conditions)
	}
}

func TestDegradedReconcilerWithoutMatchingTask(t *testing.T) {
	pod := selectedPod("trainer-0")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(pod).Build()

	r := &DegradedReconciler{Client: c}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "train", Name: "trainer-0"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

func TestDegradedReconcilerIgnoresUnselectedPod(t *testing.T) {
	task := testTask("t1", traincachev1alpha1.TargetRef{Kind: "Job", Name: "trainer"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "trainer-0", Namespace: "train"}}
	pod.Labels = map[string]string{"batch.kubernetes.io/job-name": "trainer"}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&traincachev1alpha1.WarmupTask{}).
		WithObjects(task, pod).
		Build()

	r := &DegradedReconciler{Client: c}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "train", Name: "trainer-0"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	updated := &traincachev1alpha1.WarmupTask{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "t1"}, updated); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if meta.FindStatusCondition(updated.Status.Conditions, ConditionGateInjectionDegraded) != nil {
		t.Fatalf("unselected pod must not be marked degraded: %#v", updated.Status.Conditions)
	}
}

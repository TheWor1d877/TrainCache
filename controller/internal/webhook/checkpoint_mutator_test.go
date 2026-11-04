package webhook

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"traincache/controller/internal/controller/warmup"
)

func newCheckpointMutator(t *testing.T, objects ...runtime.Object) *CheckpointMutator {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(webhookScheme(t)).
		WithRuntimeObjects(objects...).
		Build()
	return &CheckpointMutator{Client: c}
}

func checkpointConfigMap(name string, overrides map[string]string) *corev1.ConfigMap {
	data := map[string]string{
		keyBackendEndpoint: "http://minio:9000",
		keyBackendBucket:   "checkpoints",
	}
	for k, v := range overrides {
		data[k] = v
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "train"},
		Data:       data,
	}
}

func checkpointPod() *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "trainer-0",
			Namespace: "train",
			UID:       types.UID("1234567890abcdef"),
			Labels: map[string]string{
				CheckpointEnabledLabel:         CheckpointEnabledValue,
				"batch.kubernetes.io/job-name": "trainer",
			},
			Annotations: map[string]string{
				AnnotationCheckpoint: "ckpt-config",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "trainer", Image: "trainer:dev"}},
		},
	}
}

func sidecarOf(t *testing.T, pod *corev1.Pod) corev1.Container {
	t.Helper()
	for _, c := range pod.Spec.InitContainers {
		if c.Name == checkpointContainerName {
			return c
		}
	}
	t.Fatalf("checkpointd init container not found: %#v", pod.Spec.InitContainers)
	return corev1.Container{}
}

func envOf(c corev1.Container, name string) (corev1.EnvVar, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

func volumeOf(pod *corev1.Pod, name string) (corev1.Volume, bool) {
	for _, v := range pod.Spec.Volumes {
		if v.Name == name {
			return v, true
		}
	}
	return corev1.Volume{}, false
}

func mountOf(c corev1.Container, name string) (corev1.VolumeMount, bool) {
	for _, m := range c.VolumeMounts {
		if m.Name == name {
			return m, true
		}
	}
	return corev1.VolumeMount{}, false
}

func TestCheckpointHandleSelection(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(*corev1.Pod)
		objects    []runtime.Object
		allowed    bool
		patchEmpty bool
		msg        string
	}{
		{
			name:       "missing label",
			mutate:     func(p *corev1.Pod) { delete(p.Labels, CheckpointEnabledLabel) },
			objects:    []runtime.Object{checkpointConfigMap("ckpt-config", nil)},
			allowed:    true,
			patchEmpty: true,
		},
		{
			name:       "label not enabled",
			mutate:     func(p *corev1.Pod) { p.Labels[CheckpointEnabledLabel] = "false" },
			objects:    []runtime.Object{checkpointConfigMap("ckpt-config", nil)},
			allowed:    true,
			patchEmpty: true,
		},
		{
			name:    "missing checkpoint annotation",
			mutate:  func(p *corev1.Pod) { delete(p.Annotations, AnnotationCheckpoint) },
			objects: []runtime.Object{checkpointConfigMap("ckpt-config", nil)},
			msg:     AnnotationCheckpoint,
		},
		{
			name:    "configmap missing",
			mutate:  func(p *corev1.Pod) {},
			objects: nil,
			msg:     "ckpt-config",
		},
		{
			name:   "missing backendBucket",
			mutate: func(p *corev1.Pod) {},
			objects: []runtime.Object{&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "ckpt-config", Namespace: "train"},
				Data:       map[string]string{keyBackendEndpoint: "http://minio:9000"},
			}},
			msg: "backendBucket",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newCheckpointMutator(t, tc.objects...)
			pod := checkpointPod()
			tc.mutate(pod)

			resp := m.Handle(context.Background(), admissionRequest(t, pod))
			if resp.Allowed != tc.allowed {
				t.Fatalf("allowed = %v, want %v (message %q)", resp.Allowed, tc.allowed, resp.Result.Message)
			}
			if tc.patchEmpty && len(resp.Patches) != 0 {
				t.Fatalf("patches = %d, want 0", len(resp.Patches))
			}
			if tc.msg != "" && !strings.Contains(resp.Result.Message, tc.msg) {
				t.Fatalf("message %q does not contain %q", resp.Result.Message, tc.msg)
			}
		})
	}
}

func TestCheckpointHandleInjectsSidecar(t *testing.T) {
	cm := checkpointConfigMap("ckpt-config", map[string]string{keyWorldSize: "4"})
	m := newCheckpointMutator(t, cm)
	pod := checkpointPod()
	pod.Annotations[AnnotationRank] = "1"

	mutated := applyPatches(t, pod, m.Handle(context.Background(), admissionRequest(t, pod)))

	sidecar := sidecarOf(t, mutated)
	if sidecar.Image != DefaultCheckpointImage {
		t.Fatalf("image = %q", sidecar.Image)
	}
	if sidecar.RestartPolicy == nil || *sidecar.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Fatalf("restart policy = %#v", sidecar.RestartPolicy)
	}
	if sidecar.StartupProbe == nil || sidecar.StartupProbe.Exec == nil ||
		len(sidecar.StartupProbe.Exec.Command) != 2 ||
		sidecar.StartupProbe.PeriodSeconds != 10 ||
		sidecar.StartupProbe.FailureThreshold != 360 ||
		sidecar.StartupProbe.TimeoutSeconds != 3 {
		t.Fatalf("startup probe = %#v", sidecar.StartupProbe)
	}

	for _, name := range []string{checkpointWorkVolume, checkpointNVMeVolume, checkpointPluginsVolume, checkpointRunVolume} {
		if _, ok := volumeOf(mutated, name); !ok {
			t.Fatalf("missing volume %q: %#v", name, mutated.Spec.Volumes)
		}
	}
	if len(mutated.Spec.Volumes) != 4 {
		t.Fatalf("volumes = %d, want 4", len(mutated.Spec.Volumes))
	}
	if _, ok := mountOf(sidecar, checkpointPluginsVolume); !ok {
		t.Fatalf("sidecar missing plugins mount: %#v", sidecar.VolumeMounts)
	}
	// NVMe 卷必须真正挂上，否则 NVME_DIR 落在容器可写层，缓存随容器重启丢失。
	if m, ok := mountOf(sidecar, checkpointNVMeVolume); !ok || m.MountPath != checkpointNVMEDir {
		t.Fatalf("sidecar nvme mount = %#v", m)
	}
	// 保存成功后 checkpointd 要删除工作卷副本，只读挂载会让删除失败。
	if m, ok := mountOf(sidecar, checkpointWorkVolume); !ok || m.ReadOnly {
		t.Fatalf("sidecar work mount = %#v, want writable", m)
	}
	// 有代码默认值的键写成字面量，缺键时不能把 Pod 卡在 CreateContainerConfigError。
	if e, ok := envOf(sidecar, "PLUGIN"); !ok || e.ValueFrom != nil || e.Value == "" {
		t.Fatalf("PLUGIN env = %#v, want literal", e)
	}
	if e, ok := envOf(sidecar, "REQUIRE_CHECKPOINT"); !ok || e.ValueFrom != nil || e.Value != "false" {
		t.Fatalf("REQUIRE_CHECKPOINT env = %#v", e)
	}
	if e, ok := envOf(sidecar, "NVME_RETENTION"); !ok || e.ValueFrom != nil || e.Value != "1" {
		t.Fatalf("NVME_RETENTION env = %#v", e)
	}

	for _, tc := range []struct{ env, key string }{
		{"TRAINCACHE_CHECKPOINT_ACCESS_KEY", "accessKey"},
		{"TRAINCACHE_CHECKPOINT_SECRET_KEY", "secretKey"},
	} {
		e, ok := envOf(sidecar, tc.env)
		if !ok || e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
			t.Fatalf("%s is not a secretKeyRef: %#v", tc.env, e)
		}
		if e.ValueFrom.SecretKeyRef.Name != checkpointSecretName || e.ValueFrom.SecretKeyRef.Key != tc.key {
			t.Fatalf("%s secret ref = %#v", tc.env, e.ValueFrom.SecretKeyRef)
		}
	}

	backendEnv, ok := envOf(sidecar, "BACKEND")
	if !ok {
		t.Fatal("missing BACKEND")
	}
	var backend map[string]any
	if err := json.Unmarshal([]byte(backendEnv.Value), &backend); err != nil {
		t.Fatalf("BACKEND is not JSON: %v (%q)", err, backendEnv.Value)
	}
	if _, ok := backend["part_size"].(float64); !ok {
		t.Fatalf("part_size is not a JSON number: %#v", backend["part_size"])
	}
	if backend["part_size"].(float64) != float64(defaultPartSizeBytes) {
		t.Fatalf("part_size = %v", backend["part_size"])
	}
	if _, ok := backend["concurrency"].(float64); !ok {
		t.Fatalf("concurrency is not a JSON number: %#v", backend["concurrency"])
	}
	if backend["endpoint"] != "http://minio:9000" || backend["bucket"] != "checkpoints" || backend["prefix"] != "" {
		t.Fatalf("backend = %#v", backend)
	}

	for _, name := range []string{"TASK_ID", "RANK", "WORLD_SIZE", "TRAINCACHE_CHECKPOINT_DIR", "STATE_DIR", "PROBE_FILE", "SOCKET_PATH", "PLUGIN", "REQUIRE_CHECKPOINT", "NVME_RETENTION", "NVME_DIR"} {
		if _, ok := envOf(sidecar, name); !ok {
			t.Fatalf("missing env %s", name)
		}
	}
	if rank, _ := envOf(sidecar, "RANK"); rank.Value != "1" {
		t.Fatalf("RANK = %q", rank.Value)
	}
	if ws, _ := envOf(sidecar, "WORLD_SIZE"); ws.Value != "4" {
		t.Fatalf("WORLD_SIZE = %q", ws.Value)
	}

	var train corev1.Container
	for _, c := range mutated.Spec.Containers {
		if c.Name == "trainer" {
			train = c
		}
	}
	if dir, ok := envOf(train, "TRAINCACHE_CHECKPOINT_DIR"); !ok || dir.Value != "/checkpoints-work/trainer" {
		t.Fatalf("training env = %#v", train.Env)
	}
	if _, ok := mountOf(train, checkpointWorkVolume); !ok {
		t.Fatalf("training container missing work volume mount: %#v", train.VolumeMounts)
	}
}

func TestCheckpointHandleIsIdempotent(t *testing.T) {
	cm := checkpointConfigMap("ckpt-config", map[string]string{keyWorldSize: "1"})
	m := newCheckpointMutator(t, cm)
	pod := checkpointPod()

	mutated := applyPatches(t, pod, m.Handle(context.Background(), admissionRequest(t, pod)))
	volumes := len(mutated.Spec.Volumes)
	inits := len(mutated.Spec.InitContainers)

	second := m.Handle(context.Background(), admissionRequest(t, mutated))
	if !second.Allowed || len(second.Patches) != 0 {
		t.Fatalf("second admission = allowed=%v patches=%d", second.Allowed, len(second.Patches))
	}
	if len(mutated.Spec.Volumes) != volumes || len(mutated.Spec.InitContainers) != inits {
		t.Fatalf("injection duplicated: volumes=%d inits=%d", len(mutated.Spec.Volumes), len(mutated.Spec.InitContainers))
	}
}

func TestCheckpointHandleDisablesNVMe(t *testing.T) {
	cm := checkpointConfigMap("ckpt-config", map[string]string{keyNVMEEnabled: "false", keyWorldSize: "1"})
	m := newCheckpointMutator(t, cm)
	pod := checkpointPod()

	mutated := applyPatches(t, pod, m.Handle(context.Background(), admissionRequest(t, pod)))
	sidecar := sidecarOf(t, mutated)

	if _, ok := envOf(sidecar, "NVME_DIR"); ok {
		t.Fatal("NVME_DIR must be absent when nvmeEnabled=false")
	}
	if _, ok := mountOf(sidecar, checkpointNVMeVolume); ok {
		t.Fatal("checkpoint-nvme mount must be absent when nvmeEnabled=false")
	}
	if _, ok := volumeOf(mutated, checkpointNVMeVolume); ok {
		t.Fatal("checkpoint-nvme volume must be absent when nvmeEnabled=false")
	}
}

func TestCheckpointHandleWorldSizeFromJob(t *testing.T) {
	parallelism := int32(6)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "trainer", Namespace: "train"},
		Spec:       batchv1.JobSpec{Parallelism: &parallelism},
	}
	m := newCheckpointMutator(t, checkpointConfigMap("ckpt-config", nil), job)
	pod := checkpointPod()
	pod.Annotations["batch.kubernetes.io/job-completion-index"] = "2"

	mutated := applyPatches(t, pod, m.Handle(context.Background(), admissionRequest(t, pod)))
	sidecar := sidecarOf(t, mutated)

	if ws, _ := envOf(sidecar, "WORLD_SIZE"); ws.Value != "6" {
		t.Fatalf("WORLD_SIZE = %q", ws.Value)
	}
	if rank, _ := envOf(sidecar, "RANK"); rank.Value != "2" {
		t.Fatalf("RANK = %q", rank.Value)
	}
}

func TestCheckpointHandleDeniesUnresolvedRank(t *testing.T) {
	parallelism := int32(6)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "trainer", Namespace: "train"},
		Spec:       batchv1.JobSpec{Parallelism: &parallelism},
	}
	m := newCheckpointMutator(t, checkpointConfigMap("ckpt-config", nil), job)
	pod := checkpointPod()

	resp := m.Handle(context.Background(), admissionRequest(t, pod))
	if resp.Allowed {
		t.Fatalf("expected denial, got %#v", resp)
	}
	if !strings.Contains(resp.Result.Message, AnnotationRank) {
		t.Fatalf("message %q does not mention %s", resp.Result.Message, AnnotationRank)
	}
}

func TestCheckpointHandleKeepsWarmupAnnotations(t *testing.T) {
	cm := checkpointConfigMap("ckpt-config", map[string]string{keyWorldSize: "1"})
	m := newCheckpointMutator(t, cm)
	pod := checkpointPod()
	pod.Labels[warmup.WarmupEnabledLabel] = warmup.WarmupEnabledValue
	pod.Annotations[warmup.AnnotationWarmupTask] = "train/task-1"
	pod.Annotations[warmup.AnnotationPodIndex] = "0"

	mutated := applyPatches(t, pod, m.Handle(context.Background(), admissionRequest(t, pod)))
	if mutated.Annotations[warmup.AnnotationWarmupTask] != "train/task-1" {
		t.Fatalf("warmup-task annotation = %q", mutated.Annotations[warmup.AnnotationWarmupTask])
	}
	if mutated.Annotations[warmup.AnnotationPodIndex] != "0" {
		t.Fatalf("pod-index annotation = %q", mutated.Annotations[warmup.AnnotationPodIndex])
	}
}

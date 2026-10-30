package warmup

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

func envMap(env []corev1.EnvVar) map[string]string {
	out := make(map[string]string, len(env))
	for _, e := range env {
		out[e.Name] = e.Value
	}
	return out
}

func TestInjectDataloaderEnvFillsAllContainers(t *testing.T) {
	task := &traincachev1alpha1.WarmupTask{
		ObjectMeta: metav1.ObjectMeta{Name: "warm", Namespace: "train", Generation: 7},
		Spec:       traincachev1alpha1.WarmupTaskSpec{DataSetRef: corev1.LocalObjectReference{Name: "coco"}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "train", Name: "trainer-0"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "trainer"}, {Name: "sidecar"}}},
	}

	if !InjectDataloaderEnv(pod, task, 3) {
		t.Fatalf("首次注入未报告改动")
	}
	want := map[string]string{
		EnvTask:              "train/warm",
		EnvPod:               "train/trainer-0",
		EnvPodIndex:          "3",
		EnvDataset:           "train/coco",
		EnvHookSock:          "/run/traincache/agent.sock",
		EnvGenerationAtStart: "7",
	}
	for _, c := range pod.Spec.Containers {
		got := envMap(c.Env)
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("container %s env %s = %q, want %q", c.Name, k, got[k], v)
			}
		}
	}
}

func TestInjectDataloaderEnvKeepsExistingValues(t *testing.T) {
	task := &traincachev1alpha1.WarmupTask{
		ObjectMeta: metav1.ObjectMeta{Name: "warm", Namespace: "train", Generation: 2},
		Spec:       traincachev1alpha1.WarmupTaskSpec{DataSetRef: corev1.LocalObjectReference{Name: "coco"}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "train"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "trainer",
			Env:  []corev1.EnvVar{{Name: EnvHookSock, Value: "/custom/agent.sock"}},
		}}},
	}

	InjectDataloaderEnv(pod, task, 0)
	if got := envMap(pod.Spec.Containers[0].Env)[EnvHookSock]; got != "/custom/agent.sock" {
		t.Fatalf("%s = %q, want 保留原值", EnvHookSock, got)
	}
	if InjectDataloaderEnv(pod, task, 0) {
		t.Fatalf("重复注入报告了改动")
	}
}

// 准入阶段 generateName 还没生成名字：跳过 TRAINCACHE_POD，其余照常注入。
func TestInjectDataloaderEnvSkipsPodWithoutName(t *testing.T) {
	task := &traincachev1alpha1.WarmupTask{
		ObjectMeta: metav1.ObjectMeta{Name: "warm", Namespace: "train", Generation: 1},
		Spec:       traincachev1alpha1.WarmupTaskSpec{DataSetRef: corev1.LocalObjectReference{Name: "coco"}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "train", GenerateName: "trainer-"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "trainer"}}},
	}

	InjectDataloaderEnv(pod, task, 0)
	got := envMap(pod.Spec.Containers[0].Env)
	if _, ok := got[EnvPod]; ok {
		t.Fatalf("Pod 名为空时注入了 %s=%q", EnvPod, got[EnvPod])
	}
	if got[EnvTask] != "train/warm" {
		t.Fatalf("%s = %q, want train/warm", EnvTask, got[EnvTask])
	}
}

func TestInjectDataloaderEnvNilSafe(t *testing.T) {
	if InjectDataloaderEnv(nil, nil, 0) {
		t.Fatalf("nil 入参应返回 false")
	}
}

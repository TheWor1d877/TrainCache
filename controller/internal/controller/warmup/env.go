package warmup

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// DataLoader 日志与钩子所需的环境变量（docs/8.1 design.md §4.3 冻结契约）。
// webhook 在准入时写入；训练侧只读，缺失即降级为仅本地日志。
const (
	EnvTask              = "TRAINCACHE_TASK"                // <namespace>/<warmupTaskName>
	EnvPod               = "TRAINCACHE_POD"                 // <namespace>/<podName>
	EnvPodIndex          = "TRAINCACHE_POD_INDEX"           // 训练 Pod 序号
	EnvDataset           = "TRAINCACHE_DATASET"             // <namespace>/<dataSetName>
	EnvHookSock          = "TRAINCACHE_HOOK_SOCK"           // agent.sock 绝对路径
	EnvGenerationAtStart = "TRAINCACHE_GENERATION_AT_START" // 准入时看到的 WarmupTask generation
	// DefaultHookSockPath 是 Agent 钩子套接字的冻结路径（docs/6.1 design.md §5.4）。
	DefaultHookSockPath = "/run/traincache/agent.sock"
)

// InjectDataloaderEnv 给 Pod 内每个容器补 DataLoader 契约要求的 6 个环境变量。
// generation 取准入时的 task.Generation：Pod 的环境变量不可变，之后任务再更新，
// 训练侧日志只能记启动值。已存在的同名变量保留原值，返回是否有改动。
// Pod 名在准入阶段可能还没生成（generateName 场景），此时跳过 TRAINCACHE_POD，
// 训练侧回退到主机名，其余变量照常注入。
func InjectDataloaderEnv(pod *corev1.Pod, task *traincachev1alpha1.WarmupTask, index int32) bool {
	if pod == nil || task == nil {
		return false
	}
	values := make([]corev1.EnvVar, 0, 6)
	if pod.Name != "" {
		values = append(values, corev1.EnvVar{Name: EnvPod, Value: dataSetRefKey(pod.Namespace, pod.Name)})
	}
	values = append(values,
		corev1.EnvVar{Name: EnvTask, Value: TaskKey(task)},
		corev1.EnvVar{Name: EnvPodIndex, Value: strconv.FormatInt(int64(index), 10)},
		corev1.EnvVar{Name: EnvDataset, Value: dataSetRefKey(pod.Namespace, task.Spec.DataSetRef.Name)},
		corev1.EnvVar{Name: EnvHookSock, Value: DefaultHookSockPath},
		corev1.EnvVar{Name: EnvGenerationAtStart, Value: strconv.FormatInt(task.Generation, 10)},
	)
	changed := false
	for i := range pod.Spec.Containers {
		for _, v := range values {
			if !hasEnv(pod.Spec.Containers[i].Env, v.Name) {
				pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, v)
				changed = true
			}
		}
	}
	return changed
}

// dataSetRefKey 拼 <namespace>/<dataSetName>；namespace 为空时只留数据集名。
func dataSetRefKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

// hasEnv 判断环境变量名是否已存在。
func hasEnv(env []corev1.EnvVar, name string) bool {
	for _, e := range env {
		if e.Name == name {
			return true
		}
	}
	return false
}

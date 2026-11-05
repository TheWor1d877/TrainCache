package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// MutateCheckpointPath is the HTTP path of the checkpoint injection webhook.
const MutateCheckpointPath = "/mutate-checkpoint"

// Pod selection and injection annotations (docs/10.1 §4.2).
const (
	CheckpointEnabledLabel = "traincache.io/checkpoint"
	CheckpointEnabledValue = "enabled"

	AnnotationCheckpoint     = "traincache.io/checkpoint"
	AnnotationTaskID         = "traincache.io/task-id"
	AnnotationWorldSize      = "traincache.io/world-size"
	AnnotationRank           = "traincache.io/rank"
	AnnotationCheckpointStep = "traincache.io/checkpoint-step"
	AnnotationCheckpointImg  = "traincache.io/checkpoint-image"
)

// The sidecar image comes from the ConfigMap key checkpointImage or the Pod
// annotation traincache.io/checkpoint-image. The sidecar is a release
// artifact whose digest changes with every version, so there is no default
// image to fall back to: a Pod without one is rejected, the same way a Pod
// whose ConfigMap cannot be read is rejected.

const (
	checkpointContainerName = "checkpointd"
	pluginsInitName         = "checkpoint-plugins"

	checkpointWorkVolume    = "checkpoint-work"
	checkpointNVMeVolume    = "checkpoint-nvme"
	checkpointPluginsVolume = "checkpoint-plugins"
	checkpointRunVolume     = "checkpoint-run"

	checkpointWorkDir    = "/checkpoints-work"
	checkpointNVMEDir    = "/checkpoints-nvme"
	checkpointPluginsDir = "/opt/traincache/plugins"
	checkpointRunDir     = "/run/checkpoint"

	checkpointSecretName = "traincache-checkpoint"
)

// ConfigMap keys and defaults (docs/10.1 §4.2).
const (
	keyPlugin            = "plugin"
	keyBackendEndpoint   = "backendEndpoint"
	keyBackendBucket     = "backendBucket"
	keyBackendPrefix     = "backendPrefix"
	keyPartSizeBytes     = "partSizeBytes"
	keyConcurrency       = "concurrency"
	keyNVMEEnabled       = "nvmeEnabled"
	keyNVMERetention     = "nvmeRetention"
	keyRequireCheckpoint = "requireCheckpoint"
	keyWorldSize         = "worldSize"
	keySecure            = "secure"
	keyCheckpointImage   = "checkpointImage"

	defaultPlugin        = "s3"
	defaultPartSizeBytes = 8 * 1024 * 1024
	defaultConcurrency   = 4
	defaultNVMERetention = 1
)

// CheckpointMutator injects a checkpointd sidecar into Pods labeled
// traincache.io/checkpoint=enabled. It reads backend configuration from the
// ConfigMap named by AnnotationCheckpoint. Unlike PodMutator it rejects Pods
// that look enabled but cannot be resolved, because a half-configured sidecar
// would silently lose checkpoints.
type CheckpointMutator struct {
	// Client 直连 API server，不走 manager 的缓存 client：webhook 的 RBAC
	// 只给 configmaps/jobs 的 get，没有 list/watch，缓存 informer 起不来，
	// 读操作会一直阻塞到准入超时。
	Client client.Reader
	Log    *slog.Logger
	Image  string
}

type checkpointConfig struct {
	Plugin            string
	Endpoint          string
	Bucket            string
	Prefix            string
	PartSizeBytes     int64
	Concurrency       int
	NVMEEnabled       bool
	NVMERetention     int
	RequireCheckpoint bool
	WorldSize         int
	Secure            bool
	Image             string
}

type backendJSON struct {
	Endpoint    string `json:"endpoint"`
	Bucket      string `json:"bucket"`
	Prefix      string `json:"prefix"`
	PartSize    int64  `json:"part_size"`
	Concurrency int    `json:"concurrency"`
	Secure      bool   `json:"secure"`
}

// Handle implements admission.Handler for the checkpoint rule.
func (m *CheckpointMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := m.logger().With("namespace", req.Namespace, "request_id", string(req.UID))

	if req.Operation != "" && req.Operation != admissionv1.Create {
		return admission.Allowed("only CREATE is mutated")
	}
	if len(req.Object.Raw) == 0 {
		log.Warn("checkpoint admission skipped", "reason", "empty object")
		return admission.Allowed("empty object")
	}

	pod := &corev1.Pod{}
	if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
		log.Warn("checkpoint admission skipped", "reason", "decode failed", "err", err)
		return admission.Denied("cannot decode pod")
	}
	if pod.Namespace == "" {
		pod.Namespace = req.Namespace
	}
	log = log.With("pod", pod.Name, "namespace", pod.Namespace)

	if pod.Labels[CheckpointEnabledLabel] != CheckpointEnabledValue {
		return admission.Allowed("pod is not selected")
	}

	cmName := strings.TrimSpace(pod.Annotations[AnnotationCheckpoint])
	if cmName == "" {
		return deniedf("pod has %s=%s but is missing annotation %s",
			CheckpointEnabledLabel, CheckpointEnabledValue, AnnotationCheckpoint)
	}

	if m.Client == nil {
		return deniedf("cannot read ConfigMap %q: webhook client unavailable", cmName)
	}
	cm := &corev1.ConfigMap{}
	if err := m.Client.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: cmName}, cm); err != nil {
		log.Warn("checkpoint injection denied", "reason", "configmap lookup failed", "configmap", cmName, "err", err)
		return deniedf("cannot read ConfigMap %q: %v", cmName, err)
	}

	cfg, err := parseCheckpointConfig(cm)
	if err != nil {
		log.Warn("checkpoint injection denied", "reason", "invalid configmap", "configmap", cmName, "err", err)
		return admission.Denied(err.Error())
	}

	image := m.Image
	if image == "" {
		image = cfg.Image
	}
	if override := strings.TrimSpace(pod.Annotations[AnnotationCheckpointImg]); override != "" {
		image = override
	}
	if image == "" {
		return deniedf("checkpoint image is not configured: set ConfigMap %q key %s or Pod annotation %s",
			cmName, keyCheckpointImage, AnnotationCheckpointImg)
	}

	if hasContainer(pod.Spec.Containers, checkpointContainerName) ||
		hasContainer(pod.Spec.InitContainers, checkpointContainerName) {
		return admission.Allowed("already injected")
	}

	taskID := resolveTaskID(pod)
	if err := validateTaskID(taskID); err != nil {
		return admission.Denied(err.Error())
	}
	if step := strings.TrimSpace(pod.Annotations[AnnotationCheckpointStep]); step != "" {
		if n, err := strconv.ParseInt(step, 10, 64); err != nil || n < 0 {
			return deniedf("annotation %s=%q is not a non-negative integer", AnnotationCheckpointStep, step)
		}
	}
	rank, rankResolved, err := resolveRank(pod)
	if err != nil {
		return admission.Denied(err.Error())
	}
	worldSize, err := m.resolveWorldSize(ctx, pod, cfg.WorldSize)
	if err != nil {
		log.Warn("checkpoint injection denied", "reason", "world size unresolved", "err", err)
		return admission.Denied(err.Error())
	}
	if worldSize > 1 && !rankResolved {
		return deniedf("world_size=%d but rank could not be resolved; set annotation %s or %s",
			worldSize, AnnotationRank, "batch.kubernetes.io/job-completion-index")
	}
	if rank >= worldSize {
		return deniedf("rank %d out of range for world_size %d; set annotation %s or %s",
			rank, worldSize, AnnotationRank, AnnotationWorldSize)
	}
	if !rankResolved {
		log.Warn("checkpoint rank unresolved; assuming 0", "world_size", worldSize)
	}

	if err := applyCheckpointInjection(pod, cmName, cfg, taskID, rank, worldSize, image); err != nil {
		log.Warn("checkpoint injection denied", "reason", "assemble failed", "err", err)
		return admission.Denied(err.Error())
	}

	mutated, err := json.Marshal(pod)
	if err != nil {
		return admission.Denied("cannot encode mutation")
	}
	log.Info("checkpoint sidecar injected", "task_id", taskID, "rank", rank, "world_size", worldSize, "configmap", cmName)
	return admission.PatchResponseFromRaw(req.Object.Raw, mutated)
}

func (m *CheckpointMutator) logger() *slog.Logger {
	if m.Log != nil {
		return m.Log
	}
	return slog.Default()
}

func (m *CheckpointMutator) resolveWorldSize(ctx context.Context, pod *corev1.Pod, cmWorldSize int) (int, error) {
	if raw := strings.TrimSpace(pod.Annotations[AnnotationWorldSize]); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return 0, fmt.Errorf("annotation %s=%q is not a positive integer", AnnotationWorldSize, raw)
		}
		return n, nil
	}
	if cmWorldSize > 0 {
		return cmWorldSize, nil
	}

	jobName := pod.Labels["batch.kubernetes.io/job-name"]
	if jobName == "" {
		jobName = pod.Labels["job-name"]
	}
	if jobName == "" {
		return 1, nil
	}

	job := &batchv1.Job{}
	if err := m.Client.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: jobName}, job); err != nil {
		return 0, fmt.Errorf("cannot read Job %q for world_size: %v; set annotation %s", jobName, err, AnnotationWorldSize)
	}
	if job.Spec.Parallelism != nil && *job.Spec.Parallelism > 0 {
		return int(*job.Spec.Parallelism), nil
	}
	return 1, nil
}

func resolveTaskID(pod *corev1.Pod) string {
	if v := strings.TrimSpace(pod.Annotations[AnnotationTaskID]); v != "" {
		return v
	}
	if v := pod.Labels["batch.kubernetes.io/job-name"]; v != "" {
		return v
	}
	if v := pod.Labels["job-name"]; v != "" {
		return v
	}
	uid := string(pod.UID)
	if len(uid) > 8 {
		uid = uid[:8]
	}
	return fmt.Sprintf("%s-%s", pod.Namespace, uid)
}

func resolveRank(pod *corev1.Pod) (int, bool, error) {
	if raw := strings.TrimSpace(pod.Annotations[AnnotationRank]); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return 0, false, fmt.Errorf("annotation %s=%q is not a non-negative integer", AnnotationRank, raw)
		}
		return n, true, nil
	}
	if raw := strings.TrimSpace(pod.Annotations["batch.kubernetes.io/job-completion-index"]); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return 0, false, fmt.Errorf("annotation batch.kubernetes.io/job-completion-index=%q is not a non-negative integer", raw)
		}
		return n, true, nil
	}
	if name := pod.Labels["statefulset.kubernetes.io/pod-name"]; name != "" {
		if n, ok := trailingOrdinal(name); ok {
			return n, true, nil
		}
	}
	return 0, false, nil
}

func trailingOrdinal(name string) (int, bool) {
	i := len(name)
	for i > 0 && name[i-1] >= '0' && name[i-1] <= '9' {
		i--
	}
	if i == len(name) || i == 0 || name[i-1] != '-' {
		return 0, false
	}
	n, err := strconv.Atoi(name[i:])
	if err != nil {
		return 0, false
	}
	return n, true
}

func parseCheckpointConfig(cm *corev1.ConfigMap) (checkpointConfig, error) {
	data := cm.Data
	cfg := checkpointConfig{
		Plugin:        getString(data, keyPlugin, defaultPlugin),
		Prefix:        getString(data, keyBackendPrefix, ""),
		Image:         strings.TrimSpace(getString(data, keyCheckpointImage, "")),
		NVMEEnabled:   true,
		PartSizeBytes: defaultPartSizeBytes,
		Concurrency:   defaultConcurrency,
		NVMERetention: defaultNVMERetention,
	}

	var err error
	if cfg.Endpoint, err = requireString(data, keyBackendEndpoint); err != nil {
		return cfg, configErr(cm.Name, err)
	}
	if cfg.Bucket, err = requireString(data, keyBackendBucket); err != nil {
		return cfg, configErr(cm.Name, err)
	}
	if cfg.PartSizeBytes, err = getInt64(data, keyPartSizeBytes, cfg.PartSizeBytes, 1); err != nil {
		return cfg, configErr(cm.Name, err)
	}
	if cfg.Concurrency, err = getInt(data, keyConcurrency, cfg.Concurrency, 1); err != nil {
		return cfg, configErr(cm.Name, err)
	}
	if cfg.NVMERetention, err = getInt(data, keyNVMERetention, cfg.NVMERetention, 1); err != nil {
		return cfg, configErr(cm.Name, err)
	}
	if cfg.WorldSize, err = getInt(data, keyWorldSize, 0, 0); err != nil {
		return cfg, configErr(cm.Name, err)
	}
	if cfg.NVMEEnabled, err = getBool(data, keyNVMEEnabled, true); err != nil {
		return cfg, configErr(cm.Name, err)
	}
	if cfg.RequireCheckpoint, err = getBool(data, keyRequireCheckpoint, false); err != nil {
		return cfg, configErr(cm.Name, err)
	}
	if cfg.Secure, err = getBool(data, keySecure, false); err != nil {
		return cfg, configErr(cm.Name, err)
	}
	return cfg, nil
}

func configErr(cmName string, err error) error {
	return fmt.Errorf("ConfigMap %q: %v", cmName, err)
}

func requireString(data map[string]string, key string) (string, error) {
	v := strings.TrimSpace(data[key])
	if v == "" {
		return "", fmt.Errorf("missing required key %q", key)
	}
	return v, nil
}

func getString(data map[string]string, key, def string) string {
	if v := strings.TrimSpace(data[key]); v != "" {
		return v
	}
	return def
}

func getInt64(data map[string]string, key string, def, min int64) (int64, error) {
	raw := strings.TrimSpace(data[key])
	if raw == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < min {
		return 0, fmt.Errorf("key %q=%q is not an integer >= %d", key, raw, min)
	}
	return n, nil
}

func getInt(data map[string]string, key string, def, min int) (int, error) {
	raw := strings.TrimSpace(data[key])
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min {
		return 0, fmt.Errorf("key %q=%q is not an integer >= %d", key, raw, min)
	}
	return n, nil
}

// getBool 接受 true/false、1/0、yes/no（大小写不敏感），与 agent 的
// envBool 口径一致；strconv.ParseBool 不认 yes。
func getBool(data map[string]string, key string, def bool) (bool, error) {
	raw := strings.ToLower(strings.TrimSpace(data[key]))
	if raw == "" {
		return def, nil
	}
	switch raw {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no":
		return false, nil
	}
	return false, fmt.Errorf("key %q=%q is not a boolean", key, data[key])
}

func applyCheckpointInjection(pod *corev1.Pod, cmName string, cfg checkpointConfig, taskID string, rank, worldSize int, image string) error {
	if pod.Spec.SecurityContext == nil {
		pod.Spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	if pod.Spec.SecurityContext.FSGroup == nil {
		fsGroup := int64(65532)
		pod.Spec.SecurityContext.FSGroup = &fsGroup
	}
	if pod.Spec.SecurityContext.FSGroupChangePolicy == nil {
		policy := corev1.FSGroupChangeOnRootMismatch
		pod.Spec.SecurityContext.FSGroupChangePolicy = &policy
	}

	ensureVolume(pod, checkpointWorkVolume, corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}})
	if cfg.NVMEEnabled {
		ensureVolume(pod, checkpointNVMeVolume, corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}})
	}
	ensureVolume(pod, checkpointPluginsVolume, corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}})
	ensureVolume(pod, checkpointRunVolume, corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}})

	backend, err := json.Marshal(backendJSON{
		Endpoint:    cfg.Endpoint,
		Bucket:      cfg.Bucket,
		Prefix:      cfg.Prefix,
		PartSize:    cfg.PartSizeBytes,
		Concurrency: cfg.Concurrency,
		Secure:      cfg.Secure,
	})
	if err != nil {
		return fmt.Errorf("encode backend: %w", err)
	}

	taskDir := checkpointWorkDir + "/" + taskID
	pluginsInit := corev1.Container{
		Name:    pluginsInitName,
		Image:   image,
		Command: []string{"cp", "-a", checkpointPluginsDir + "/.", "/mnt/plugins/"},
		VolumeMounts: []corev1.VolumeMount{{
			Name:      checkpointPluginsVolume,
			MountPath: "/mnt/plugins",
		}},
	}

	sidecar := corev1.Container{
		Name:          checkpointContainerName,
		Image:         image,
		Command:       []string{"checkpointd"},
		RestartPolicy: ptr(corev1.ContainerRestartPolicyAlways),
		StartupProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: []string{"checkpointd", "probe"}},
			},
			PeriodSeconds:    10,
			FailureThreshold: 360,
			TimeoutSeconds:   3,
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("50m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			// 保存成功后 checkpointd 要删除工作卷上的副本，必须可写。
			{Name: checkpointWorkVolume, MountPath: checkpointWorkDir},
			{Name: checkpointRunVolume, MountPath: checkpointRunDir},
			{Name: checkpointPluginsVolume, MountPath: checkpointPluginsDir, ReadOnly: true},
		},
		Env: []corev1.EnvVar{
			{Name: "TASK_ID", Value: taskID},
			{Name: "RANK", Value: strconv.Itoa(rank)},
			{Name: "WORLD_SIZE", Value: strconv.Itoa(worldSize)},
			{Name: "TRAINCACHE_CHECKPOINT_DIR", Value: taskDir},
			{Name: "STATE_DIR", Value: checkpointRunDir + "/state"},
			{Name: "PROBE_FILE", Value: checkpointRunDir + "/ready"},
			{Name: "SOCKET_PATH", Value: checkpointRunDir + "/plugin.sock"},
			// 这三个键有代码默认值，写成字面量：configMapKeyRef 在键缺失时会
			// 让 Pod 停在 CreateContainerConfigError，与“可选键”的语义冲突。
			{Name: "PLUGIN", Value: cfg.Plugin},
			{Name: "BACKEND", Value: string(backend)},
			{Name: "REQUIRE_CHECKPOINT", Value: strconv.FormatBool(cfg.RequireCheckpoint)},
			{Name: "NVME_RETENTION", Value: strconv.Itoa(cfg.NVMERetention)},
			{Name: "TRAINCACHE_CHECKPOINT_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: checkpointSecretName},
				Key:                  "accessKey",
			}}},
			{Name: "TRAINCACHE_CHECKPOINT_SECRET_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: checkpointSecretName},
				Key:                  "secretKey",
			}}},
		},
	}
	if cfg.NVMEEnabled {
		sidecar.VolumeMounts = append(sidecar.VolumeMounts, corev1.VolumeMount{
			Name: checkpointNVMeVolume, MountPath: checkpointNVMEDir,
		})
		sidecar.Env = append(sidecar.Env, corev1.EnvVar{Name: "NVME_DIR", Value: checkpointNVMEDir})
	}
	if step := strings.TrimSpace(pod.Annotations[AnnotationCheckpointStep]); step != "" {
		sidecar.Env = append(sidecar.Env, corev1.EnvVar{Name: "CHECKPOINT_STEP", Value: step})
	}

	pod.Spec.InitContainers = append(pod.Spec.InitContainers, pluginsInit, sidecar)

	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == checkpointContainerName {
			continue
		}
		ensureEnv(&pod.Spec.Containers[i], corev1.EnvVar{Name: "TRAINCACHE_CHECKPOINT_DIR", Value: taskDir})
		ensureVolumeMount(&pod.Spec.Containers[i], corev1.VolumeMount{
			Name: checkpointWorkVolume, MountPath: checkpointWorkDir,
		})
	}
	return nil
}

func hasContainer(containers []corev1.Container, name string) bool {
	for i := range containers {
		if containers[i].Name == name {
			return true
		}
	}
	return false
}

func ensureVolume(pod *corev1.Pod, name string, source corev1.VolumeSource) {
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == name {
			return
		}
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: name, VolumeSource: source})
}

func ensureVolumeMount(container *corev1.Container, mount corev1.VolumeMount) {
	for i := range container.VolumeMounts {
		if container.VolumeMounts[i].Name == mount.Name || container.VolumeMounts[i].MountPath == mount.MountPath {
			return
		}
	}
	container.VolumeMounts = append(container.VolumeMounts, mount)
}

func ensureEnv(container *corev1.Container, env corev1.EnvVar) {
	for i := range container.Env {
		if container.Env[i].Name == env.Name {
			return
		}
	}
	container.Env = append(container.Env, env)
}

// validateTaskID 挡住会拼出逃逸路径或非法目录名的取值；task_id 同时进
// 工作目录、缓存目录与后端键。
func validateTaskID(id string) error {
	if id == "" {
		return fmt.Errorf("task_id is empty")
	}
	if id == "." || id == ".." || strings.ContainsAny(id, "/\\") {
		return fmt.Errorf("task_id %q must not be . / .. or contain path separators", id)
	}
	if len(id) > 63 {
		return fmt.Errorf("task_id %q is longer than 63 bytes", id)
	}
	return nil
}

func deniedf(format string, args ...any) admission.Response {
	return admission.Denied(fmt.Sprintf(format, args...))
}

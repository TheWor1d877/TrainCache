package webhook

import (
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"traincache/controller/internal/controller/warmup"
)

func TestNewMutatingWebhookConfigurationContract(t *testing.T) {
	cfg := NewMutatingWebhookConfiguration("traincache-webhook", "train-system", "traincache-webhook", []byte("ca"))
	if len(cfg.Webhooks) != 2 {
		t.Fatalf("webhooks = %d", len(cfg.Webhooks))
	}
	wh := cfg.Webhooks[0]
	if wh.Name != WebhookName {
		t.Fatalf("name = %q", wh.Name)
	}
	if wh.FailurePolicy == nil || *wh.FailurePolicy != admissionregistrationv1.Ignore {
		t.Fatalf("failurePolicy = %#v", wh.FailurePolicy)
	}
	if wh.ClientConfig.Service == nil || wh.ClientConfig.Service.Path == nil || *wh.ClientConfig.Service.Path != MutatePodPath {
		t.Fatalf("service config = %#v", wh.ClientConfig.Service)
	}
	if wh.ObjectSelector == nil ||
		wh.ObjectSelector.MatchLabels[warmup.WarmupEnabledLabel] != warmup.WarmupEnabledValue {
		t.Fatalf("objectSelector = %#v", wh.ObjectSelector)
	}
	if len(wh.Rules) != 1 || len(wh.Rules[0].Operations) != 1 ||
		wh.Rules[0].Operations[0] != admissionregistrationv1.Create {
		t.Fatalf("rules = %#v", wh.Rules)
	}
	if len(wh.Rules[0].Resources) != 1 || wh.Rules[0].Resources[0] != "pods" {
		t.Fatalf("resources = %#v", wh.Rules[0].Resources)
	}
}

// The checkpoint entry rejects Pods it cannot resolve, so its failurePolicy is
// Fail. An empty caBundle turns that rejection into "every checkpoint Pod is
// rejected", which is why the CA is patched into every entry.
func TestCheckpointWebhookContract(t *testing.T) {
	cfg := NewMutatingWebhookConfiguration("traincache-webhook", "train-system", "traincache-webhook", []byte("ca"))
	wh := cfg.Webhooks[1]
	if wh.Name != CheckpointWebhookName {
		t.Fatalf("name = %q", wh.Name)
	}
	if wh.FailurePolicy == nil || *wh.FailurePolicy != admissionregistrationv1.Fail {
		t.Fatalf("failurePolicy = %#v", wh.FailurePolicy)
	}
	if wh.ClientConfig.Service == nil || wh.ClientConfig.Service.Path == nil || *wh.ClientConfig.Service.Path != MutateCheckpointPath {
		t.Fatalf("service config = %#v", wh.ClientConfig.Service)
	}
	// D24：选择器只按 label 键是否存在匹配，值是每个任务的 ConfigMap 名，
	// 不可枚举，限定取值会让写错值的 Pod 静默绕过 webhook。
	if wh.ObjectSelector == nil ||
		len(wh.ObjectSelector.MatchLabels) != 0 ||
		len(wh.ObjectSelector.MatchExpressions) != 1 ||
		wh.ObjectSelector.MatchExpressions[0].Key != CheckpointEnabledLabel ||
		wh.ObjectSelector.MatchExpressions[0].Operator != metav1.LabelSelectorOpExists {
		t.Fatalf("objectSelector = %#v", wh.ObjectSelector)
	}
	if len(wh.ClientConfig.CABundle) != 2 {
		t.Fatalf("caBundle = %q", wh.ClientConfig.CABundle)
	}
}

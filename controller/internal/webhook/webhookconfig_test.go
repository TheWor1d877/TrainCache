package webhook

import (
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"

	"traincache/controller/internal/controller/warmup"
)

func TestNewMutatingWebhookConfigurationContract(t *testing.T) {
	cfg := NewMutatingWebhookConfiguration("traincache-webhook", "train-system", "traincache-webhook", []byte("ca"))
	if len(cfg.Webhooks) != 1 {
		t.Fatalf("webhooks = %d", len(cfg.Webhooks))
	}
	wh := cfg.Webhooks[0]
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

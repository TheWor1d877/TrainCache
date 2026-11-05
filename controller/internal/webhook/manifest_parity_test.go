package webhook

import (
	"os"
	"reflect"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// manifestPath is the shipped copy of the MutatingWebhookConfiguration.
const manifestPath = "../../deploy/webhook/mutatingwebhook.yaml"

// TestManifestParity checks that the shipped manifest and
// NewMutatingWebhookConfiguration describe the same webhook entries. The
// manifest is what users install, so an entry that exists in one copy only
// reaches the cluster with an empty caBundle and rejects the Pods it matches.
func TestManifestParity(t *testing.T) {
	f, err := os.Open(manifestPath)
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	defer f.Close()

	var shipped admissionregistrationv1.MutatingWebhookConfiguration
	if err := yaml.NewYAMLOrJSONDecoder(f, 4096).Decode(&shipped); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	generated := NewMutatingWebhookConfiguration(shipped.Name, "traincache", "traincache-webhook", nil)

	if len(shipped.Webhooks) != len(generated.Webhooks) {
		t.Fatalf("webhook count: manifest %d, generated %d", len(shipped.Webhooks), len(generated.Webhooks))
	}
	for i := range shipped.Webhooks {
		s := shipped.Webhooks[i]
		g := generated.Webhooks[i]
		if s.Name != g.Name {
			t.Fatalf("webhook %d name: manifest %q, generated %q", i, s.Name, g.Name)
		}
		if s.FailurePolicy == nil || g.FailurePolicy == nil || *s.FailurePolicy != *g.FailurePolicy {
			t.Fatalf("webhook %q failurePolicy: manifest %v, generated %v", s.Name, s.FailurePolicy, g.FailurePolicy)
		}
		if s.TimeoutSeconds == nil || g.TimeoutSeconds == nil || *s.TimeoutSeconds != *g.TimeoutSeconds {
			t.Fatalf("webhook %q timeoutSeconds: manifest %v, generated %v", s.Name, s.TimeoutSeconds, g.TimeoutSeconds)
		}
		if s.ClientConfig.Service == nil || g.ClientConfig.Service == nil {
			t.Fatalf("webhook %q service: manifest %v, generated %v", s.Name, s.ClientConfig.Service, g.ClientConfig.Service)
		}
		if *s.ClientConfig.Service.Path != *g.ClientConfig.Service.Path {
			t.Fatalf("webhook %q path: manifest %q, generated %q", s.Name, *s.ClientConfig.Service.Path, *g.ClientConfig.Service.Path)
		}
		if s.ObjectSelector == nil || g.ObjectSelector == nil {
			t.Fatalf("webhook %q objectSelector: manifest %v, generated %v", s.Name, s.ObjectSelector, g.ObjectSelector)
		}
		// matchLabels 与 matchExpressions 都要一致：checkpoint 那条只写
		// matchExpressions Exists（D24），漏比它等于放着一份装不进集群的
		// 清单不管。
		if !reflect.DeepEqual(s.ObjectSelector.MatchLabels, g.ObjectSelector.MatchLabels) {
			t.Fatalf("webhook %q objectSelector matchLabels: manifest %v, generated %v",
				s.Name, s.ObjectSelector.MatchLabels, g.ObjectSelector.MatchLabels)
		}
		if !reflect.DeepEqual(s.ObjectSelector.MatchExpressions, g.ObjectSelector.MatchExpressions) {
			t.Fatalf("webhook %q objectSelector matchExpressions: manifest %v, generated %v",
				s.Name, s.ObjectSelector.MatchExpressions, g.ObjectSelector.MatchExpressions)
		}
	}
}

package webhook

import (
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"traincache/controller/internal/controller/warmup"
)

// WebhookName is the name of the single mutating webhook entry.
const WebhookName = "pods.traincache.io"

// NewMutatingWebhookConfiguration builds the MutatingWebhookConfiguration
// paired with PodMutator. Deployment manifests render the returned object;
// keeping it here makes the objectSelector/failurePolicy contract testable
// without a manifest file (docs/8.1 §5.7, D9).
func NewMutatingWebhookConfiguration(name, namespace, serviceName string, caBundle []byte) *admissionregistrationv1.MutatingWebhookConfiguration {
	ignore := admissionregistrationv1.Ignore
	none := admissionregistrationv1.SideEffectClassNone
	equivalent := admissionregistrationv1.Equivalent
	timeout := int32(5)
	return &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks: []admissionregistrationv1.MutatingWebhook{{
			Name:                    WebhookName,
			AdmissionReviewVersions: []string{"v1"},
			SideEffects:             &none,
			FailurePolicy:           &ignore,
			MatchPolicy:             &equivalent,
			TimeoutSeconds:          &timeout,
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				Service: &admissionregistrationv1.ServiceReference{
					Namespace: namespace,
					Name:      serviceName,
					Path:      ptr(MutatePodPath),
				},
				CABundle: caBundle,
			},
			Rules: []admissionregistrationv1.RuleWithOperations{{
				Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
				Rule: admissionregistrationv1.Rule{
					APIGroups:   []string{""},
					APIVersions: []string{"v1"},
					Resources:   []string{"pods"},
					Scope:       ptr(admissionregistrationv1.AllScopes),
				},
			}},
			ObjectSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
				warmup.WarmupEnabledLabel: warmup.WarmupEnabledValue,
			}},
		}},
	}
}

func ptr[T any](v T) *T { return &v }

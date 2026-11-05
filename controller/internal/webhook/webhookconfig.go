package webhook

import (
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"traincache/controller/internal/controller/warmup"
)

// WebhookName is the name of the warmup gate injection mutating webhook entry.
const WebhookName = "pods.traincache.io"

// CheckpointWebhookName is the name of the checkpoint sidecar injection entry.
const CheckpointWebhookName = "pods-checkpoint.traincache.io"

// NewMutatingWebhookConfiguration builds the MutatingWebhookConfiguration
// paired with PodMutator and CheckpointMutator. The shipped copy of this
// object is controller/deploy/webhook/mutatingwebhook.yaml; both must list the
// same entries. An entry whose caBundle stays empty rejects every Pod it
// matches, because failurePolicy=Fail turns the unverified TLS call into a
// rejection. TestManifestParity keeps the two copies in step.
func NewMutatingWebhookConfiguration(name, namespace, serviceName string, caBundle []byte) *admissionregistrationv1.MutatingWebhookConfiguration {
	none := admissionregistrationv1.SideEffectClassNone
	equivalent := admissionregistrationv1.Equivalent

	entry := func(webhookName, path string, policy admissionregistrationv1.FailurePolicyType, timeout int32, selector *metav1.LabelSelector) admissionregistrationv1.MutatingWebhook {
		return admissionregistrationv1.MutatingWebhook{
			Name:                    webhookName,
			AdmissionReviewVersions: []string{"v1"},
			SideEffects:             &none,
			FailurePolicy:           &policy,
			MatchPolicy:             &equivalent,
			TimeoutSeconds:          &timeout,
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				Service: &admissionregistrationv1.ServiceReference{
					Namespace: namespace,
					Name:      serviceName,
					Path:      ptr(path),
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
			ObjectSelector: selector,
		}
	}

	return &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			entry(WebhookName, MutatePodPath, admissionregistrationv1.Ignore, 5,
				&metav1.LabelSelector{MatchLabels: map[string]string{
					warmup.WarmupEnabledLabel: warmup.WarmupEnabledValue,
				}}),
			// objectSelector 用 Exists 而不是限定取值（D24）：label 值是
			// 每个任务的 ConfigMap 名，不可枚举，限定取值时写错值的 Pod 会
			// 静默绕过 webhook；键存在即进准入，值的合法性由 mutator 判。
			entry(CheckpointWebhookName, MutateCheckpointPath, admissionregistrationv1.Fail, 10,
				&metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      CheckpointEnabledLabel,
					Operator: metav1.LabelSelectorOpExists,
				}}}),
		},
	}
}

func ptr[T any](v T) *T { return &v }

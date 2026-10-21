package warmup

import (
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// FilterInput is the frozen candidate-filter contract from docs/8.1 §5.4.
//
// Accessible is DataSet.status.accessibleNodes: the resolved
// traincache.io/s3-access=true set, optionally narrowed by
// spec.accessibleNodeSelector (D30). The filter only reads; it never writes
// Nodes.
type FilterInput struct {
	Nodes      []corev1.Node
	Accessible sets.Set[string]
	Pods       []traincachev1alpha1.PodDetail
	Policy     traincachev1alpha1.CandidatePolicy
}

// Filter returns candidate Node names in ascending order. Rules are applied
// as exclusions: Ready, accessible, taint-free, and enough allocatable
// resources for the largest PodDetail request. Nodes without a Ready Agent Pod
// are excluded by the caller via agentclient.ReadyAgentNodes.
func Filter(in FilterInput) ([]string, error) {
	candidates := make([]string, 0, len(in.Nodes))
	for i := range in.Nodes {
		node := &in.Nodes[i]
		if !nodeReady(node) {
			continue
		}
		if !in.Accessible.Has(node.Name) {
			continue
		}
		if hasBlockingTaint(node, in.Policy.IgnoreNoSchedule) {
			continue
		}
		if !fitsResources(node, in.Pods) {
			continue
		}
		if !fitsMinFreeDisk(node, in.Policy.MinFreeDiskBytes) {
			continue
		}
		candidates = append(candidates, node.Name)
	}
	sort.Strings(candidates)
	return candidates, nil
}

func nodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func hasBlockingTaint(node *corev1.Node, ignoreNoSchedule bool) bool {
	for _, taint := range node.Spec.Taints {
		switch taint.Effect {
		case corev1.TaintEffectNoExecute:
			return true
		case corev1.TaintEffectNoSchedule:
			if !ignoreNoSchedule {
				return true
			}
		}
	}
	return false
}

// fitsResources checks that allocatable resources cover the largest PodDetail
// request. v1 estimates one Pod per Node (Q6) and does not bin-pack.
func fitsResources(node *corev1.Node, pods []traincachev1alpha1.PodDetail) bool {
	largest := corev1.ResourceList{}
	for i := range pods {
		for name, quantity := range pods[i].Resources.Requests {
			current, ok := largest[name]
			if !ok || quantity.Cmp(current) > 0 {
				largest[name] = quantity.DeepCopy()
			}
		}
	}
	for name, want := range largest {
		allocate, ok := node.Status.Allocatable[name]
		if !ok || allocate.Cmp(want) < 0 {
			return false
		}
	}
	return true
}

// fitsMinFreeDisk compares the policy floor with ephemeral-storage
// allocatable. Node.status does not expose free disk; allocatable is the
// conservative read-only proxy available to the controller.
func fitsMinFreeDisk(node *corev1.Node, minFreeBytes int64) bool {
	if minFreeBytes <= 0 {
		return true
	}
	allocate, ok := node.Status.Allocatable[corev1.ResourceEphemeralStorage]
	if !ok {
		return false
	}
	return allocate.Value() >= minFreeBytes
}

package dataset

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Accessible node labels are maintained by humans and are never written by any
// TrainCache component (decisions D30). The controller only lists Nodes.
const (
	AccessibleNodeLabelKey   = "traincache.io/s3-access"
	AccessibleNodeLabelValue = "true"
)

// ResolveAccessibleNodes lists Nodes carrying the default s3-access label and,
// when selector is non-empty, narrows that set by the additional selector.
// The result is sorted ascending and contains no duplicates.
func ResolveAccessibleNodes(ctx context.Context, reader client.Reader, selector *metav1.LabelSelector) ([]string, error) {
	requirement, err := labels.NewRequirement(AccessibleNodeLabelKey, selection.Equals, []string{AccessibleNodeLabelValue})
	if err != nil {
		return nil, fmt.Errorf("dataset: build default node selector: %w", err)
	}
	combined := labels.NewSelector().Add(*requirement)
	if !selectorEmpty(selector) {
		extra, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			return nil, fmt.Errorf("dataset: invalid accessibleNodeSelector: %w", err)
		}
		extraRequirements, _ := extra.Requirements()
		combined = combined.Add(extraRequirements...)
	}

	var nodes corev1.NodeList
	if err := reader.List(ctx, &nodes, client.MatchingLabelsSelector{Selector: combined}); err != nil {
		return nil, fmt.Errorf("dataset: list nodes: %w", err)
	}

	names := make([]string, 0, len(nodes.Items))
	for i := range nodes.Items {
		names = append(names, nodes.Items[i].Name)
	}
	sort.Strings(names)
	return names, nil
}

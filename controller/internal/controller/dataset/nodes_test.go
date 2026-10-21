package dataset

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestResolveAccessibleNodesDefaultsToS3AccessLabel(t *testing.T) {
	reader := fake.NewClientBuilder().WithScheme(nodeScheme()).WithObjects(
		node("node-b", map[string]string{AccessibleNodeLabelKey: "true"}),
		node("node-a", map[string]string{AccessibleNodeLabelKey: "true"}),
		node("node-c", map[string]string{AccessibleNodeLabelKey: "false"}),
		node("node-d", nil),
	).Build()

	got, err := ResolveAccessibleNodes(context.Background(), reader, nil)
	if err != nil {
		t.Fatalf("ResolveAccessibleNodes: %v", err)
	}
	if want := []string{"node-a", "node-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("nodes = %v, want %v", got, want)
	}
}

func TestResolveAccessibleNodesNarrowsSelector(t *testing.T) {
	reader := fake.NewClientBuilder().WithScheme(nodeScheme()).WithObjects(
		node("node-a", map[string]string{AccessibleNodeLabelKey: "true", "zone": "a"}),
		node("node-b", map[string]string{AccessibleNodeLabelKey: "true", "zone": "b"}),
		node("node-c", map[string]string{AccessibleNodeLabelKey: "false", "zone": "a"}),
	).Build()

	selector := &metav1.LabelSelector{MatchLabels: map[string]string{"zone": "a"}}
	got, err := ResolveAccessibleNodes(context.Background(), reader, selector)
	if err != nil {
		t.Fatalf("ResolveAccessibleNodes: %v", err)
	}
	if want := []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("nodes = %v, want %v", got, want)
	}
}

func TestResolveAccessibleNodesEmptySelectorEqualsDefault(t *testing.T) {
	reader := fake.NewClientBuilder().WithScheme(nodeScheme()).WithObjects(
		node("node-a", map[string]string{AccessibleNodeLabelKey: "true", "zone": "a"}),
	).Build()

	got, err := ResolveAccessibleNodes(context.Background(), reader, &metav1.LabelSelector{})
	if err != nil {
		t.Fatalf("ResolveAccessibleNodes: %v", err)
	}
	if want := []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("nodes = %v, want %v", got, want)
	}
}

func TestResolveAccessibleNodesNoMatches(t *testing.T) {
	reader := fake.NewClientBuilder().WithScheme(nodeScheme()).WithObjects(
		node("node-a", map[string]string{AccessibleNodeLabelKey: "true", "zone": "a"}),
	).Build()

	got, err := ResolveAccessibleNodes(context.Background(), reader, &metav1.LabelSelector{MatchLabels: map[string]string{"zone": "b"}})
	if err != nil {
		t.Fatalf("ResolveAccessibleNodes: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("nodes = %v, want none", got)
	}
}

func TestResolveAccessibleNodesRejectsInvalidSelector(t *testing.T) {
	reader := fake.NewClientBuilder().WithScheme(nodeScheme()).Build()
	selector := &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
		{Key: "zone", Operator: "Bogus"},
	}}
	if _, err := ResolveAccessibleNodes(context.Background(), reader, selector); err == nil {
		t.Fatal("ResolveAccessibleNodes invalid selector: got nil error")
	}
}

func node(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func nodeScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		panic(err)
	}
	return s
}

package warmup

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

func filterNode(name string, ready bool, taints []corev1.Taint, allocatable corev1.ResourceList) corev1.Node {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.NodeSpec{Taints: taints},
		Status: corev1.NodeStatus{
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: status}},
			Allocatable: allocatable,
		},
	}
}

func TestFilterExcludesNotReadyTaintedAndInaccessible(t *testing.T) {
	nodes := []corev1.Node{
		filterNode("ready", true, nil, nil),
		filterNode("not-ready", false, nil, nil),
		filterNode("no-schedule", true, []corev1.Taint{{Key: "dedicated", Effect: corev1.TaintEffectNoSchedule}}, nil),
		filterNode("no-execute", true, []corev1.Taint{{Key: "maintenance", Effect: corev1.TaintEffectNoExecute}}, nil),
		filterNode("not-accessible", true, nil, nil),
	}
	accessible := sets.New[string]("ready", "not-ready", "no-schedule", "no-execute")

	got, err := Filter(FilterInput{Nodes: nodes, Accessible: accessible})
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if want := []string{"ready"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Filter() = %v, want %v", got, want)
	}
}

func TestFilterIgnoreNoSchedule(t *testing.T) {
	nodes := []corev1.Node{
		filterNode("no-schedule", true, []corev1.Taint{{Key: "dedicated", Effect: corev1.TaintEffectNoSchedule}}, nil),
		filterNode("no-execute", true, []corev1.Taint{{Key: "maintenance", Effect: corev1.TaintEffectNoExecute}}, nil),
	}
	accessible := sets.New[string]("no-schedule", "no-execute")

	got, err := Filter(FilterInput{
		Nodes:      nodes,
		Accessible: accessible,
		Policy:     traincachev1alpha1.CandidatePolicy{IgnoreNoSchedule: true},
	})
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if want := []string{"no-schedule"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Filter() = %v, want %v", got, want)
	}
}

func TestFilterResourcesAndDisk(t *testing.T) {
	cpuRequest := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")}
	pods := []traincachev1alpha1.PodDetail{{
		Resources: corev1.ResourceRequirements{Requests: cpuRequest},
	}}
	nodes := []corev1.Node{
		filterNode("small", true, nil, corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}),
		filterNode("large", true, nil, corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}),
	}
	accessible := sets.New[string]("small", "large")

	got, err := Filter(FilterInput{Nodes: nodes, Accessible: accessible, Pods: pods})
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if want := []string{"large"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resource filter = %v, want %v", got, want)
	}

	diskNodes := []corev1.Node{
		filterNode("disk-small", true, nil, corev1.ResourceList{
			corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
		}),
		filterNode("disk-large", true, nil, corev1.ResourceList{
			corev1.ResourceEphemeralStorage: resource.MustParse("0.25Gi"),
		}),
	}
	got, err = Filter(FilterInput{
		Nodes:      diskNodes,
		Accessible: sets.New[string]("disk-small", "disk-large"),
		Policy:     traincachev1alpha1.CandidatePolicy{MinFreeDiskBytes: 1 << 30},
	})
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if want := []string{"disk-small"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("disk filter = %v, want %v", got, want)
	}
}

func TestFilterReturnsAscendingOrder(t *testing.T) {
	nodes := []corev1.Node{
		filterNode("c", true, nil, nil),
		filterNode("a", true, nil, nil),
		filterNode("b", true, nil, nil),
	}
	got, err := Filter(FilterInput{
		Nodes:      nodes,
		Accessible: sets.New[string]("a", "b", "c", "d"),
	})
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Filter() = %v, want %v", got, want)
	}
}

func TestFilterNoCandidates(t *testing.T) {
	got, err := Filter(FilterInput{})
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Filter() = %v, want empty", got)
	}
}

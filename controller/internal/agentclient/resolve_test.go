package agentclient

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	return scheme
}

type countingReader struct {
	client.Client
	listCalls int
	getCalls  int
}

func (c *countingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.listCalls++
	return c.Client.List(ctx, list, opts...)
}

func (c *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.getCalls++
	return c.Client.Get(ctx, key, obj, opts...)
}

func agentPod(name, node, ip string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "traincache-system",
			Labels:    map[string]string{AgentPodLabelKey: AgentPodLabelValue},
		},
		Spec: corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      ip,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
		},
	}
}

func testNode(name, ip string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: ip}},
		},
	}
}

func TestResolverStaticOverrideWins(t *testing.T) {
	r := NewResolver(nil, 9410)
	r.Overrides = map[string]string{"n1": "10.9.8.7:19410"}
	addr, err := r.Resolve(context.Background(), "n1")
	if err != nil || addr != "10.9.8.7:19410" {
		t.Fatalf("Resolve() = (%q, %v)", addr, err)
	}
}

func TestStaticResolver(t *testing.T) {
	if _, err := (StaticResolver{}).Resolve(context.Background(), "n1"); !errors.Is(err, ErrAgentUnavailable) {
		t.Fatalf("missing override error = %v", err)
	}
	if addr, err := (StaticResolver{"n1": "1.2.3.4:9410"}).Resolve(context.Background(), "n1"); err != nil || addr != "1.2.3.4:9410" {
		t.Fatalf("Resolve() = (%q, %v)", addr, err)
	}
}

func TestResolverDiscoversReadyAgentPodAndCaches(t *testing.T) {
	reader := &countingReader{Client: fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(agentPod("agent-a", "n1", "10.0.0.9", false), agentPod("agent-b", "n1", "10.0.0.5", true)).
		Build()}
	now := time.Unix(1000, 0)
	r := NewResolver(reader, 9410)
	r.Now = func() time.Time { return now }

	for i := 0; i < 2; i++ {
		addr, err := r.Resolve(context.Background(), "n1")
		if err != nil || addr != "10.0.0.5:9410" {
			t.Fatalf("Resolve() = (%q, %v), want ready pod address", addr, err)
		}
	}
	if reader.listCalls != 1 {
		t.Fatalf("list calls = %d, want 1 (cached)", reader.listCalls)
	}

	now = now.Add(DefaultResolveCacheTTL + time.Second)
	if _, err := r.Resolve(context.Background(), "n1"); err != nil {
		t.Fatalf("Resolve after TTL: %v", err)
	}
	if reader.listCalls != 2 {
		t.Fatalf("list calls = %d, want 2 after TTL", reader.listCalls)
	}
}

func TestResolverFallsBackToNodeAddress(t *testing.T) {
	reader := &countingReader{Client: fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(testNode("n2", "192.168.1.20")).
		Build()}
	r := NewResolver(reader, 19410)
	addr, err := r.Resolve(context.Background(), "n2")
	if err != nil || addr != "192.168.1.20:19410" {
		t.Fatalf("Resolve() = (%q, %v)", addr, err)
	}
}

func TestResolverUnavailableWithoutPodOrNode(t *testing.T) {
	reader := &countingReader{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build()}
	r := NewResolver(reader, 9410)
	if _, err := r.Resolve(context.Background(), "missing"); !errors.Is(err, ErrAgentUnavailable) {
		t.Fatalf("Resolve() error = %v, want ErrAgentUnavailable", err)
	}
	if _, err := r.Resolve(context.Background(), ""); !errors.Is(err, ErrAgentUnavailable) {
		t.Fatalf("empty node error = %v", err)
	}
}

func TestResolverAcceptsStandardNameLabel(t *testing.T) {
	pod := agentPod("agent-a", "n1", "10.0.0.7", true)
	delete(pod.Labels, AgentPodLabelKey)
	pod.Labels[AgentPodNameLabelKey] = AgentPodLabelValue
	reader := &countingReader{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(pod).Build()}
	r := NewResolver(reader, 9410)
	addr, err := r.Resolve(context.Background(), "n1")
	if err != nil || addr != "10.0.0.7:9410" {
		t.Fatalf("Resolve() = (%q, %v)", addr, err)
	}
}

func TestReadyAgentNodes(t *testing.T) {
	notSelected := agentPod("plain", "n3", "10.0.0.3", true)
	delete(notSelected.Labels, AgentPodLabelKey)
	nodes := ReadyAgentNodes([]corev1.Pod{
		*agentPod("ready", "n1", "10.0.0.1", true),
		*agentPod("not-ready", "n2", "10.0.0.2", false),
		*notSelected,
	})
	if !nodes.Has("n1") {
		t.Fatal("n1 should be ready")
	}
	if nodes.Has("n2") || nodes.Has("n3") {
		t.Fatalf("unexpected ready nodes: %v", nodes)
	}
}

// Package agentclient resolves per-Node Agent addresses and exposes the gRPC
// client pool used by the WarmupTask controller (docs/7.1-design.md《Agent gRPC 客户端》).
package agentclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// AgentPodLabelKey/Value identify the node-local Agent DaemonSet Pods.
	// They are read only: the controller never mutates Agent Pods.
	AgentPodLabelKey   = "app"
	AgentPodLabelValue = "traincache-agentd"

	// AgentPodNameLabelKey is the standard label used by the shipped
	// DaemonSet manifest. Both selectors are accepted so either deployment
	// form resolves to the same Agent Pods.
	AgentPodNameLabelKey = "app.kubernetes.io/name"

	// DefaultAgentPort is the Agent gRPC port. CONTROLLER_AGENT_PORT overrides
	// it; the configured value also acts as the DaemonSet hostPort.
	DefaultAgentPort = 9410

	// DefaultResolveCacheTTL is the address-cache lifetime from《Agent gRPC 客户端》.
	DefaultResolveCacheTTL = 30 * time.Second
)

// AddressResolver maps a Node name to the Agent gRPC endpoint (host:port).
type AddressResolver interface {
	Resolve(ctx context.Context, node string) (string, error)
}

// CandidateResolver maps a Node name to the ordered list of Agent gRPC
// endpoints that may reach the Agent on that Node. The first entry is the
// preferred one.
type CandidateResolver interface {
	ResolveCandidates(ctx context.Context, node string) ([]string, error)
}

// ErrNoAgentAddress means the Node has no Agent address at all: no Agent Pod
// runs on it and the Node itself exposes no usable address.
var ErrNoAgentAddress = errors.New("agentclient: node has no agent address")

// StaticResolver is an explicit node -> address map. It has the highest
// priority and is useful for tests and for deployments that route Agent
// traffic through a fixed address (e.g. a NodePort or a proxy).
type StaticResolver map[string]string

// Resolve implements AddressResolver.
func (s StaticResolver) Resolve(_ context.Context, node string) (string, error) {
	addr, ok := s[node]
	if !ok || addr == "" {
		return "", fmt.Errorf("%w: no configured address for node %s", ErrAgentUnavailable, node)
	}
	return addr, nil
}

// ResolveCandidates implements CandidateResolver. A static map has exactly one
// address per node.
func (s StaticResolver) ResolveCandidates(ctx context.Context, node string) ([]string, error) {
	addr, err := s.Resolve(ctx, node)
	if err != nil {
		return nil, err
	}
	return []string{addr}, nil
}

type resolvedAddresses struct {
	addrs   []string
	expires time.Time
}

// Resolver discovers the Agent endpoint for a Node.
//
// Resolution order (docs/7.1-design.md《Agent gRPC 客户端》):
//  1. an explicit override, which takes priority over discovery;
//  2. the Ready Agent Pod scheduled on the Node, using podIP:Port;
//  3. the Node's InternalIP (falling back to ExternalIP) with Port, which is
//     the DaemonSet hostPort route.
//
// Resolve returns the first candidate; ResolveCandidates returns the whole
// list in the order above, so a client that can try several routes gets the
// podIP route first and the hostPort route second.
//
// Results are cached per Node for CacheTTL (30s by default). Failures are not
// cached. The resolver only reads Nodes and Pods.
type Resolver struct {
	Reader    client.Reader
	Port      int
	CacheTTL  time.Duration
	Now       func() time.Time
	Overrides map[string]string

	mu    sync.Mutex
	cache map[string]resolvedAddresses
}

// NewResolver builds a Kubernetes-backed resolver using port for both the
// Agent Pod port and the Node hostPort route.
func NewResolver(reader client.Reader, port int) *Resolver {
	if port <= 0 {
		port = DefaultAgentPort
	}
	return &Resolver{
		Reader:   reader,
		Port:     port,
		CacheTTL: DefaultResolveCacheTTL,
		Now:      time.Now,
		cache:    map[string]resolvedAddresses{},
	}
}

// Resolve implements AddressResolver. It returns the preferred candidate.
func (r *Resolver) Resolve(ctx context.Context, node string) (string, error) {
	addrs, err := r.ResolveCandidates(ctx, node)
	if err != nil {
		return "", err
	}
	return addrs[0], nil
}

// ResolveCandidates implements CandidateResolver. It returns every Agent
// endpoint known for the Node, in preference order: the Agent Pod IP first,
// the Node hostPort route second. A missing Agent Pod or a missing Node
// address only drops its own candidate; the lookup fails when neither is
// available.
func (r *Resolver) ResolveCandidates(ctx context.Context, node string) ([]string, error) {
	if node == "" {
		return nil, fmt.Errorf("%w: empty node name", ErrAgentUnavailable)
	}
	if addr := r.Overrides[node]; addr != "" {
		return []string{addr}, nil
	}
	if addrs, ok := r.cached(node); ok {
		return addrs, nil
	}
	if r.Reader == nil {
		return nil, fmt.Errorf("%w: no Kubernetes reader for node %s", ErrAgentUnavailable, node)
	}

	podAddr, podErr := r.discoverPodAddress(ctx, node)
	nodeAddr, nodeErr := r.nodeAddress(ctx, node)

	var addrs []string
	if podErr == nil {
		addrs = append(addrs, podAddr)
	}
	if nodeErr == nil {
		addrs = append(addrs, nodeAddr)
	}
	if len(addrs) == 0 {
		// 两路查找各自带自己的原因,合成一条给出:哪个 Pod 查不到、哪个节点查不到。
		return nil, fmt.Errorf("%w: node %s: %w",
			ErrNoAgentAddress, node, errors.Join(podErr, nodeErr))
	}
	r.store(node, addrs)
	return addrs, nil
}

func (r *Resolver) discoverPodAddress(ctx context.Context, node string) (string, error) {
	items, err := ListAgentPods(ctx, r.Reader)
	if err != nil {
		return "", fmt.Errorf("%w: list agent Pods for node %s: %v", ErrAgentUnavailable, node, err)
	}

	ready := make([]*corev1.Pod, 0, 2)
	fallback := make([]*corev1.Pod, 0, 2)
	for i := range items {
		pod := &items[i]
		if pod.Spec.NodeName != node || pod.Status.PodIP == "" {
			continue
		}
		if podReady(pod) {
			ready = append(ready, pod)
		} else {
			fallback = append(fallback, pod)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].Name < ready[j].Name })
	sort.Slice(fallback, func(i, j int) bool { return fallback[i].Name < fallback[j].Name })
	if len(ready) > 0 {
		return net.JoinHostPort(ready[0].Status.PodIP, fmt.Sprint(r.port())), nil
	}
	if len(fallback) > 0 {
		return net.JoinHostPort(fallback[0].Status.PodIP, fmt.Sprint(r.port())), nil
	}
	return "", fmt.Errorf("%w: no agent Pod on node %s", ErrAgentUnavailable, node)
}

func (r *Resolver) nodeAddress(ctx context.Context, node string) (string, error) {
	var n corev1.Node
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: node}, &n); err != nil {
		return "", fmt.Errorf("%w: get node %s: %v", ErrAgentUnavailable, node, err)
	}
	for _, want := range []corev1.NodeAddressType{corev1.NodeInternalIP, corev1.NodeExternalIP} {
		for _, addr := range n.Status.Addresses {
			if addr.Type == want && addr.Address != "" {
				return net.JoinHostPort(addr.Address, fmt.Sprint(r.port())), nil
			}
		}
	}
	return "", fmt.Errorf("%w: node %s has no InternalIP/ExternalIP", ErrAgentUnavailable, node)
}

func (r *Resolver) port() int {
	if r.Port > 0 {
		return r.Port
	}
	return DefaultAgentPort
}

func (r *Resolver) ttl() time.Duration {
	if r.CacheTTL > 0 {
		return r.CacheTTL
	}
	return DefaultResolveCacheTTL
}

func (r *Resolver) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Resolver) cached(node string) ([]string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.cache[node]
	if !ok || !r.now().Before(entry.expires) {
		if ok {
			delete(r.cache, node)
		}
		return nil, false
	}
	return append([]string(nil), entry.addrs...), true
}

func (r *Resolver) store(node string, addrs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cache == nil {
		r.cache = map[string]resolvedAddresses{}
	}
	r.cache[node] = resolvedAddresses{
		addrs:   append([]string(nil), addrs...),
		expires: r.now().Add(r.ttl()),
	}
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// IsAgentPod reports whether the Pod belongs to the Agent DaemonSet under
// either accepted label convention.
func IsAgentPod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	return pod.Labels[AgentPodLabelKey] == AgentPodLabelValue ||
		pod.Labels[AgentPodNameLabelKey] == AgentPodLabelValue
}

// ListAgentPods lists Agent DaemonSet Pods. The shipped manifest uses
// app.kubernetes.io/name while docs/7.1-design.md《Agent gRPC 客户端》names app; the first selector is
// tried first and the standard label is used as a fallback, so a normal
// deployment costs one List. The controller never mutates these Pods.
func ListAgentPods(ctx context.Context, reader client.Reader) ([]corev1.Pod, error) {
	if reader == nil {
		return nil, fmt.Errorf("%w: no Kubernetes reader for agent Pods", ErrAgentUnavailable)
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.MatchingLabels{AgentPodLabelKey: AgentPodLabelValue}); err != nil {
		return nil, err
	}
	if len(pods.Items) > 0 {
		return pods.Items, nil
	}
	if err := reader.List(ctx, &pods, client.MatchingLabels{AgentPodNameLabelKey: AgentPodLabelValue}); err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// ReadyAgentNodes returns the set of Nodes that host a Ready Agent Pod. Nodes
// without one are excluded from the candidate list (docs/7.1-design.md《候选节点筛选》).
func ReadyAgentNodes(pods []corev1.Pod) sets.Set[string] {
	out := sets.New[string]()
	for i := range pods {
		pod := &pods[i]
		if !IsAgentPod(pod) {
			continue
		}
		if pod.Spec.NodeName == "" || !podReady(pod) {
			continue
		}
		out.Insert(pod.Spec.NodeName)
	}
	return out
}

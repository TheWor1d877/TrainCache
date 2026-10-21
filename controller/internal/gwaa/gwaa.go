// Package gwaa implements Greedy Window Affinity Assignment (GWAA) from
// docs/8.0 §GWAA and docs/8.1 §5.5.
//
// Compute is a pure function: it reads only its Input, performs no I/O, does
// not touch Kubernetes types and emits no log records. Callers own logging and
// RPC orchestration. Pods that must share a node (AffinityGroup) are merged
// into one placement unit; every tie is broken through the injected
// *rand.Rand, so a fixed seed reproduces the same placement (D32).
package gwaa

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
)

const (
	// MaxPods and MaxNodes are the Q11 P x N limits, also validated by the
	// warmup controller before submission.
	MaxPods  = 64
	MaxNodes = 256
)

// ErrInvalidInput marks inputs that violate the P x N limits or the basic
// Pod/Node invariants. Compute reports it through Output.Err.
var ErrInvalidInput = errors.New("gwaa: invalid input")

// PodState is one Pod's readiness vector and affinity intent.
type PodState struct {
	// Index is the Pod ordinal; it must be unique.
	Index int32
	// K is the window length. It is validated but not otherwise used by GWAA.
	K int32
	// ReadyCountByNode is c_ij: the number of ready blocks in the window on a
	// node. Missing nodes count as zero.
	ReadyCountByNode map[string]int32

	// AffinityGroup is a non-empty group label. Pods sharing a label must be
	// placed on the same node and are merged into one placement unit.
	AffinityGroup string
	// AntiAffinityGroup is a non-empty group label. Pods sharing a label must
	// not be placed on the same node.
	AntiAffinityGroup string
}

// NodeState is one candidate node. The caller has already applied the coarse
// resource/accessibility filter; Stopped excludes nodes whose Agent failed.
type NodeState struct {
	Name    string
	Stopped bool
}

// Input is the frozen GWAA input. Nodes are the candidate set in the caller's
// column order; Rand is injected so production can use a time seed while tests
// use a fixed seed.
type Input struct {
	Pods  []PodState
	Nodes []NodeState
	Rand  *rand.Rand

	// AllowMultiPodPerNode defaults to false: at most one placement unit per
	// node. When true, units may share a node unless their anti-affinity
	// groups conflict (Q6(b), v1 does not bin-pack resources).
	AllowMultiPodPerNode bool

	// MaxRounds overrides the convergence limit for tests and diagnostics.
	// Zero means len(Pods), the contract from docs/8.2 M6; larger values are
	// clamped to len(Pods).
	MaxRounds int
}

// Output is the GWAA result. Assignment maps podIndex -> nodeName and omits
// unassigned Pods. PreheatSet is the node -> sorted podIndex view of the same
// mapping. UnassignedPods lists Pods for which no feasible placement exists.
type Output struct {
	Assignment     map[int32]string
	PreheatSet     map[string][]int32
	UnassignedPods []int32
	Rounds         int
	FellBack       bool
	Err            error
}

// Placement is the docs/8.1 §5.5 name for Output. New code should use Output.
type Placement = Output

// unit is one placement unit: a singleton Pod or a merged affinity group.
type unit struct {
	pods     []int32 // sorted Pod indices
	c        []float64
	top1     float64
	top2     float64
	top1Hits int
	pref     []int // node positions, best first, ties already randomized
	rank     []int // node position -> pref index, -1 if excluded
	anti     map[string]struct{}
	feasible bool // at least one non-stopped node and no internal contradiction
}

type node struct {
	name    string
	stopped bool
}

type groupSeed struct {
	key  string
	pods []int32
}

// Validate enforces the P x N limits and basic invariants.
func Validate(in Input) error {
	if in.MaxRounds < 0 {
		return fmt.Errorf("%w: max rounds %d is negative", ErrInvalidInput, in.MaxRounds)
	}
	if len(in.Pods) > MaxPods {
		return fmt.Errorf("%w: pod count %d exceeds limit %d", ErrInvalidInput, len(in.Pods), MaxPods)
	}
	if len(in.Nodes) > MaxNodes {
		return fmt.Errorf("%w: node count %d exceeds limit %d", ErrInvalidInput, len(in.Nodes), MaxNodes)
	}
	seenNodes := make(map[string]struct{}, len(in.Nodes))
	for i := range in.Nodes {
		if in.Nodes[i].Name == "" {
			return fmt.Errorf("%w: nodes[%d] has an empty name", ErrInvalidInput, i)
		}
		if _, dup := seenNodes[in.Nodes[i].Name]; dup {
			return fmt.Errorf("%w: node %q is duplicated", ErrInvalidInput, in.Nodes[i].Name)
		}
		seenNodes[in.Nodes[i].Name] = struct{}{}
	}
	seenPods := make(map[int32]struct{}, len(in.Pods))
	for i := range in.Pods {
		pod := in.Pods[i]
		if pod.Index < 0 {
			return fmt.Errorf("%w: pods[%d].index %d is negative", ErrInvalidInput, i, pod.Index)
		}
		if _, dup := seenPods[pod.Index]; dup {
			return fmt.Errorf("%w: pod index %d is duplicated", ErrInvalidInput, pod.Index)
		}
		seenPods[pod.Index] = struct{}{}
		if pod.K < 0 {
			return fmt.Errorf("%w: pod %d has negative K=%d", ErrInvalidInput, pod.Index, pod.K)
		}
		for name, ready := range pod.ReadyCountByNode {
			if ready < 0 {
				return fmt.Errorf("%w: pod %d has negative readiness %d on node %q", ErrInvalidInput, pod.Index, ready, name)
			}
		}
	}
	return nil
}

// Compute runs GWAA. It is deterministic for a fixed Input.Rand seed and
// returns Output.Err instead of panicking on invalid input.
func Compute(in Input) Output {
	if err := Validate(in); err != nil {
		return invalidOutput(in, err)
	}
	rng := in.Rand
	if rng == nil {
		// Production injects a time-seeded source. A fixed fallback keeps the
		// function total and reproducible when a caller forgets.
		rng = rand.New(rand.NewSource(0))
	}

	nodes := make([]node, len(in.Nodes))
	for i := range in.Nodes {
		nodes[i] = node{name: in.Nodes[i].Name, stopped: in.Nodes[i].Stopped}
	}

	units := buildUnits(in, nodes, rng)
	active := make([]int, 0, len(units))
	for i := range units {
		if units[i].feasible {
			active = append(active, i)
		}
	}
	order := orderUnits(units, active, rng)

	allowMulti := in.AllowMultiPodPerNode
	first := newState(len(units), len(nodes))
	for _, u := range order {
		if n := first.bestFeasible(u, -1, units, allowMulti); n >= 0 {
			first.assignUnit(u, n)
		}
	}

	maxRounds := len(in.Pods)
	if in.MaxRounds > 0 && in.MaxRounds < maxRounds {
		maxRounds = in.MaxRounds
	}

	st := newState(len(units), len(nodes))
	nextRank := make([]int, len(units))
	rounds := 0
	stable := maxRounds == 0
	resolve := func() bool {
		changed := false
		for _, u := range order {
			if st.assign[u] >= 0 {
				continue
			}
			for idx := nextRank[u]; idx < len(units[u].pref); idx++ {
				n := units[u].pref[idx]
				blockers := st.blockers(u, n, units, allowMulti)
				if len(blockers) == 0 {
					st.assignUnit(u, n)
					changed = true
					break
				}
				if st.winsAll(u, blockers, n, units, rng) {
					for _, b := range blockers {
						st.unassign(b)
						if rank := units[b].rank[n]; rank >= 0 && rank+1 > nextRank[b] {
							nextRank[b] = rank + 1
						}
					}
					st.assignUnit(u, n)
					changed = true
					break
				}
				// The yielding unit may only retreat to its next-best node
				// (docs/8.0 conflict adjudication).
				nextRank[u] = idx + 1
				break
			}
		}
		return changed
	}

	for rounds < maxRounds {
		if !resolve() {
			stable = true
			break
		}
		rounds++
	}
	fellBack := false
	if !stable && rounds == maxRounds {
		// One probe round distinguishes "stable right at the limit" from a
		// still-moving assignment. A moving probe hits the non-convergence
		// path and discards the state in favour of the first greedy result.
		if resolve() {
			fellBack = true
			st = first
		}
	}

	if fellBack {
		rounds = maxRounds
	}
	return buildOutput(in, units, st, rounds, fellBack)
}

func invalidOutput(in Input, err error) Output {
	pods := make([]int32, 0, len(in.Pods))
	for i := range in.Pods {
		pods = append(pods, in.Pods[i].Index)
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i] < pods[j] })
	return Output{
		Assignment:     map[int32]string{},
		PreheatSet:     map[string][]int32{},
		UnassignedPods: pods,
		Err:            err,
	}
}

// buildUnits merges affinity groups and precomputes the per-node readiness
// aggregate, preference order and dependency (c_ij - alternative) data.
func buildUnits(in Input, nodes []node, rng *rand.Rand) []unit {
	sortedPods := append([]PodState(nil), in.Pods...)
	sort.Slice(sortedPods, func(i, j int) bool { return sortedPods[i].Index < sortedPods[j].Index })

	groups := make(map[string][]int32)
	var singles []int32
	for i := range sortedPods {
		pod := sortedPods[i]
		if pod.AffinityGroup != "" {
			groups[pod.AffinityGroup] = append(groups[pod.AffinityGroup], pod.Index)
		} else {
			singles = append(singles, pod.Index)
		}
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	seeds := make([]groupSeed, 0, len(keys)+len(singles))
	for _, key := range keys {
		seeds = append(seeds, groupSeed{key: key, pods: groups[key]})
	}
	for _, index := range singles {
		seeds = append(seeds, groupSeed{pods: []int32{index}})
	}
	sort.Slice(seeds, func(i, j int) bool { return seeds[i].pods[0] < seeds[j].pods[0] })

	byIndex := make(map[int32]PodState, len(sortedPods))
	for i := range sortedPods {
		byIndex[sortedPods[i].Index] = sortedPods[i]
	}

	units := make([]unit, 0, len(seeds))
	for _, seed := range seeds {
		u := unit{
			pods:     append([]int32(nil), seed.pods...),
			c:        make([]float64, len(nodes)),
			rank:     make([]int, len(nodes)),
			anti:     make(map[string]struct{}),
			feasible: true,
		}
		for i := range u.rank {
			u.rank[i] = -1
		}
		antiSeen := make(map[string]struct{})
		for _, index := range u.pods {
			pod := byIndex[index]
			if pod.AntiAffinityGroup == "" {
				continue
			}
			if _, dup := antiSeen[pod.AntiAffinityGroup]; dup {
				// Affinity and anti-affinity contradict each other: the group
				// cannot be placed anywhere.
				u.feasible = false
				break
			}
			antiSeen[pod.AntiAffinityGroup] = struct{}{}
			u.anti[pod.AntiAffinityGroup] = struct{}{}
		}
		if !u.feasible {
			units = append(units, u)
			continue
		}

		stopped := 0
		for n := range nodes {
			if nodes[n].stopped {
				stopped++
				continue
			}
			sum := int64(0)
			for _, index := range u.pods {
				sum += int64(byIndex[index].ReadyCountByNode[nodes[n].name])
			}
			u.c[n] = float64(sum) / float64(len(u.pods))
		}
		u.feasible = stopped < len(nodes)
		if !u.feasible {
			units = append(units, u)
			continue
		}
		setTops(&u, nodes)
		u.pref = preference(u.c, nodes, rng)
		for i, n := range u.pref {
			u.rank[n] = i
		}
		units = append(units, u)
	}
	return units
}

func setTops(u *unit, nodes []node) {
	for n := range nodes {
		if nodes[n].stopped {
			continue
		}
		c := u.c[n]
		switch {
		case c > u.top1:
			u.top2 = u.top1
			u.top1 = c
			u.top1Hits = 1
		case c == u.top1:
			u.top1Hits++
			if c > u.top2 {
				u.top2 = c
			}
		case c > u.top2:
			u.top2 = c
		}
	}
}

// dependency is c_ij - best alternative c_ik (k != j), the conflict tie-break
// from docs/8.0. A unique maximum has a positive value; a tied maximum has 0.
func (u *unit) dependency(n int) float64 {
	c := u.c[n]
	if c == u.top1 {
		if u.top1Hits > 1 {
			return 0
		}
		return c - u.top2
	}
	return c - u.top1
}

func preference(c []float64, nodes []node, rng *rand.Rand) []int {
	pref := make([]int, 0, len(nodes))
	for n := range nodes {
		if !nodes[n].stopped {
			pref = append(pref, n)
		}
	}
	sort.SliceStable(pref, func(i, j int) bool {
		if c[pref[i]] != c[pref[j]] {
			return c[pref[i]] > c[pref[j]]
		}
		return pref[i] < pref[j]
	})
	for i := 0; i < len(pref); {
		j := i + 1
		for j < len(pref) && c[pref[j]] == c[pref[i]] {
			j++
		}
		shuffle(pref[i:j], rng)
		i = j
	}
	return pref
}

// orderUnits sorts active units by maximum c_ij descending and randomizes ties
// with rng. The shuffle is what makes different seeds produce different (but
// legal) placements.
func orderUnits(units []unit, active []int, rng *rand.Rand) []int {
	if len(active) == 0 {
		return nil
	}
	maxReady := make(map[int]float64, len(active))
	for _, u := range active {
		max := 0.0
		for _, n := range units[u].pref {
			if units[u].c[n] > max {
				max = units[u].c[n]
			}
		}
		maxReady[u] = max
	}
	order := append([]int(nil), active...)
	sort.SliceStable(order, func(i, j int) bool {
		if maxReady[order[i]] != maxReady[order[j]] {
			return maxReady[order[i]] > maxReady[order[j]]
		}
		return order[i] < order[j]
	})
	for i := 0; i < len(order); {
		j := i + 1
		for j < len(order) && maxReady[order[j]] == maxReady[order[i]] {
			j++
		}
		shuffle(order[i:j], rng)
		i = j
	}
	return order
}

func shuffle(ids []int, rng *rand.Rand) {
	for i := len(ids) - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		ids[i], ids[j] = ids[j], ids[i]
	}
}

func shareAnti(a, b *unit) bool {
	if len(a.anti) > len(b.anti) {
		a, b = b, a
	}
	for group := range a.anti {
		if _, ok := b.anti[group]; ok {
			return true
		}
	}
	return false
}

type state struct {
	assign    []int
	nodeUnits [][]int
}

func newState(units, nodes int) *state {
	st := &state{
		assign:    make([]int, units),
		nodeUnits: make([][]int, nodes),
	}
	for i := range st.assign {
		st.assign[i] = -1
	}
	return st
}

func (s *state) assignUnit(u, n int) {
	s.assign[u] = n
	s.nodeUnits[n] = append(s.nodeUnits[n], u)
}

func (s *state) unassign(u int) {
	n := s.assign[u]
	if n < 0 {
		return
	}
	for i, other := range s.nodeUnits[n] {
		if other == u {
			s.nodeUnits[n] = append(s.nodeUnits[n][:i], s.nodeUnits[n][i+1:]...)
			break
		}
	}
	s.assign[u] = -1
}

// bestFeasible returns the highest-c non-stopped node that can host u without
// violating one-unit-per-node or anti-affinity. ignore skips one unit (used by
// conflict relocation); pass -1 otherwise.
func (s *state) bestFeasible(u, ignore int, units []unit, allowMulti bool) int {
	for _, n := range units[u].pref {
		if !s.nodeAccepts(u, n, ignore, units, allowMulti) {
			continue
		}
		return n
	}
	return -1
}

func (s *state) nodeAccepts(u, n, ignore int, units []unit, allowMulti bool) bool {
	for _, other := range s.nodeUnits[n] {
		if other == u || other == ignore {
			continue
		}
		if !allowMulti {
			return false
		}
		if shareAnti(&units[u], &units[other]) {
			return false
		}
	}
	return true
}

// blockers returns the units currently on n that prevent u from joining.
func (s *state) blockers(u, n int, units []unit, allowMulti bool) []int {
	if len(s.nodeUnits[n]) == 0 {
		return nil
	}
	if !allowMulti {
		out := append([]int(nil), s.nodeUnits[n]...)
		sort.Ints(out)
		return out
	}
	var out []int
	for _, other := range s.nodeUnits[n] {
		if other == u {
			continue
		}
		if shareAnti(&units[u], &units[other]) {
			out = append(out, other)
		}
	}
	sort.Ints(out)
	return out
}

// winsAll applies the conflict rule: the unit with the larger c_ij - second_i
// keeps the node; equal differences are decided by the injected RNG.
func (s *state) winsAll(u int, blockers []int, n int, units []unit, rng *rand.Rand) bool {
	want := units[u].dependency(n)
	for _, other := range blockers {
		have := units[other].dependency(n)
		switch {
		case want > have:
		case want < have:
			return false
		default:
			if rng.Intn(2) == 0 {
				return false
			}
		}
	}
	return true
}

func buildOutput(in Input, units []unit, st *state, rounds int, fellBack bool) Output {
	out := Output{
		Assignment: map[int32]string{},
		PreheatSet: map[string][]int32{},
		Rounds:     rounds,
		FellBack:   fellBack,
	}
	for u := range units {
		if st.assign[u] < 0 {
			continue
		}
		name := in.Nodes[st.assign[u]].Name
		for _, index := range units[u].pods {
			out.Assignment[index] = name
			out.PreheatSet[name] = append(out.PreheatSet[name], index)
		}
	}
	out.UnassignedPods = make([]int32, 0, len(in.Pods)-len(out.Assignment))
	for i := range in.Pods {
		index := in.Pods[i].Index
		if _, assigned := out.Assignment[index]; !assigned {
			out.UnassignedPods = append(out.UnassignedPods, index)
		}
	}
	sort.Slice(out.UnassignedPods, func(i, j int) bool { return out.UnassignedPods[i] < out.UnassignedPods[j] })
	for name := range out.PreheatSet {
		sort.Slice(out.PreheatSet[name], func(i, j int) bool {
			return out.PreheatSet[name][i] < out.PreheatSet[name][j]
		})
	}
	return out
}

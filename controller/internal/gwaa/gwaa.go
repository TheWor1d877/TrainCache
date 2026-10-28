// Package gwaa implements Greedy Window Affinity Assignment (GWAA) from
// docs/7.0 §GWAA and docs/7.1 §5.5.
//
// Compute is a pure function: it reads only its Input, performs no I/O, does
// not touch Kubernetes types and emits no log records. Callers own logging and
// RPC orchestration. Pods that must share a node (AffinityGroup) are merged
// into one placement unit.
//
// M14 splits placement into two steps. First each unit gets a deterministic
// target set within [FanoutMin, Fanout], chosen from the window cache
// distribution (docs/7.3 D39). Then claims, conflict adjudication, top-up and
// out-of-target fallback run only inside that target set. Target-set ordering
// is deterministic (c_ij descending, node name ascending); ties during
// assignment are still broken through the injected *rand.Rand, so a fixed seed
// reproduces the same placement (D32).
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
// column order; Rand is injected so the caller controls the tie-breaking seed:
// the warmup controller derives it per WarmupTask generation, tests use a fixed
// seed.
type Input struct {
	Pods  []PodState
	Nodes []NodeState
	Rand  *rand.Rand

	// AllowMultiPodPerNode defaults to false: at most one placement unit per
	// node. When true, units may share a node unless their anti-affinity
	// groups conflict (Q6(b), v1 does not bin-pack resources).
	AllowMultiPodPerNode bool

	// MaxRounds overrides the convergence limit for tests and diagnostics.
	// Zero means len(Pods), the contract from docs/7.2 M6; larger values are
	// clamped to len(Pods).
	MaxRounds int

	// Fanout is the upper bound on the number of preheat nodes each Pod claims
	// (docs/7.1 §5.5). Zero means 1; the CRD rejects zero and negative values,
	// but Compute still treats zero as the single-node default for callers that
	// predate M13.
	Fanout int32

	// FanoutMin is the lower bound on the number of preheat nodes each Pod
	// claims (M14). Zero or negative means 1; Compute rejects a normalized
	// minimum larger than the normalized maximum.
	FanoutMin int32
}

// PodDecision records why GWAA chose a Pod's target fanout (M14). It is the
// per-Pod explanation attached to Output.Decisions.
type PodDecision struct {
	// TargetFanout is the number of nodes chosen for this Pod, within
	// [FanoutMin, Fanout].
	TargetFanout int32
	// LowSignal reports that the readiness signal was too weak to raise the
	// target above the minimum.
	LowSignal bool
	// ForcedByMin reports that the target was raised to FanoutMin.
	ForcedByMin bool
	// MinUnmet reports that the final Assignment claims fewer than FanoutMin
	// nodes. Conflicts, capacity pressure or an out-of-target fallback can leave
	// the actual count below the lower bound even when the planned target was
	// large enough, so the actual claim count is authoritative.
	MinUnmet bool
	// FallbackOutOfTarget reports that a node outside the target set was used
	// as a fallback.
	FallbackOutOfTarget bool
}

// Output is the GWAA result. Assignment maps podIndex -> up to the unit's
// target count node names sorted by c_ij descending and omits Pods with no
// feasible node.
// PreheatSet is the node -> sorted podIndex view and covers every (Pod, node)
// pair in Assignment. UnassignedPods lists Pods for which no feasible node
// exists at all.
//
// TargetSet maps podIndex -> the adaptive target node names, ordered by c_ij
// descending and node name ascending. Assignment is a subset of TargetSet
// except for the single out-of-target fallback described by
// Decisions[pod].FallbackOutOfTarget. Decisions carries the per-Pod target
// explanation.
type Output struct {
	Assignment     map[int32][]string
	PreheatSet     map[string][]int32
	UnassignedPods []int32
	Rounds         int
	FellBack       bool
	Err            error

	TargetSet map[int32][]string
	Decisions map[int32]PodDecision
}

// Placement is the docs/7.1 §5.5 name for Output. New code should use Output.
type Placement = Output

// unit is one placement unit: a singleton Pod or a merged affinity group.
type unit struct {
	pods     []int32 // sorted Pod indices
	c        []float64
	top1     float64
	top2     float64
	top1Hits int
	// target is the M14 target set in deterministic order: c_ij descending,
	// node name ascending.
	target []int
	// pref is the target set in claim order. It keeps the injected RNG for
	// equal-c_ij ties, so conflict adjudication stays reproducible per seed.
	pref []int
	// fallback holds feasible nodes outside the target set in claim order. It
	// is only consulted when a unit cannot claim any target node.
	fallback []int
	rank     []int // node position -> pref/fallback index, -1 if excluded
	anti     map[string]struct{}
	feasible bool // at least one non-stopped node and no internal contradiction
	decision PodDecision
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
	if in.Fanout < 0 {
		return fmt.Errorf("%w: fanout %d is negative", ErrInvalidInput, in.Fanout)
	}
	fanout, fanoutMin := normalizedFanout(in)
	if fanoutMin > fanout {
		return fmt.Errorf("%w: fanout minimum %d exceeds fanout maximum %d", ErrInvalidInput, fanoutMin, fanout)
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

// normalizedFanout applies the M14 defaults: a non-positive maximum or
// minimum means 1. Validate rejects a negative maximum before Compute reaches
// this helper, but the helper stays total for direct callers.
func normalizedFanout(in Input) (fanout, fanoutMin int32) {
	fanout = in.Fanout
	if fanout <= 0 {
		fanout = 1
	}
	fanoutMin = in.FanoutMin
	if fanoutMin <= 0 {
		fanoutMin = 1
	}
	return fanout, fanoutMin
}

// Compute runs GWAA. It is deterministic for a fixed Input.Rand seed and
// returns Output.Err instead of panicking on invalid input.
func Compute(in Input) Output {
	if err := Validate(in); err != nil {
		return invalidOutput(in, err)
	}
	fanout, fanoutMin := normalizedFanout(in)
	rng := in.Rand
	if rng == nil {
		// A fixed fallback keeps the function total and reproducible when a
		// caller forgets to inject a seeded source.
		rng = rand.New(rand.NewSource(0))
	}

	nodes := make([]node, len(in.Nodes))
	for i := range in.Nodes {
		nodes[i] = node{name: in.Nodes[i].Name, stopped: in.Nodes[i].Stopped}
	}

	units, err := buildUnits(in, nodes, rng, fanout, fanoutMin)
	if err != nil {
		return invalidOutput(in, err)
	}
	active := make([]int, 0, len(units))
	for i := range units {
		if units[i].feasible {
			active = append(active, i)
		}
	}
	order := orderUnits(units, active, rng)

	targetLen := func(u int) int { return len(units[u].target) }
	allowMulti := in.AllowMultiPodPerNode
	first := newState(len(units), len(nodes))
	for _, u := range order {
		first.fillUnit(u, targetLen(u), units, allowMulti)
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
			if len(st.claims[u]) >= targetLen(u) {
				continue
			}
			for idx := nextRank[u]; idx < len(units[u].pref); idx++ {
				n := units[u].pref[idx]
				if st.claimed[u][n] {
					nextRank[u] = idx + 1
					continue
				}
				blockers := st.blockers(u, n, units, allowMulti)
				if len(blockers) == 0 {
					st.addClaim(u, n)
					changed = true
					nextRank[u] = idx + 1
					if len(st.claims[u]) >= targetLen(u) {
						break
					}
					continue
				}
				if st.winsAll(u, blockers, n, units, rng) {
					for _, b := range blockers {
						st.removeClaim(b, n)
						if rank := units[b].rank[n]; rank >= 0 && rank+1 > nextRank[b] {
							nextRank[b] = rank + 1
						}
					}
					st.addClaim(u, n)
					changed = true
					nextRank[u] = idx + 1
					if len(st.claims[u]) >= targetLen(u) {
						break
					}
					continue
				}
				// The yielding unit may only retreat to its next-best node
				// (docs/7.0 conflict adjudication). A retreat with untried
				// candidates left is progress: reporting "no change" here
				// would declare convergence while the unit is still below its target.
				nextRank[u] = idx + 1
				if nextRank[u] < len(units[u].pref) {
					changed = true
				}
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
	// The bounded-round conflict resolution may stop with a unit below its
	// target even though acceptable free target nodes remain. Fill those units
	// without evicting anyone. The ceiling is the M14 target, never Fanout.
	for _, u := range order {
		st.topUp(u, targetLen(u), units, allowMulti)
	}
	// A unit that could not claim any target node would otherwise be fully
	// unassigned. M14 allows exactly one out-of-target fallback in that case;
	// only a unit with no feasible node at all stays unassigned.
	fallback := make([]bool, len(units))
	for _, u := range order {
		if len(st.claims[u]) > 0 {
			continue
		}
		for _, n := range units[u].fallback {
			if st.nodeAccepts(u, n, units, allowMulti) {
				st.addClaim(u, n)
				fallback[u] = true
				break
			}
		}
	}
	// MinUnmet describes the final Assignment, not the planned target. Conflict
	// adjudication, top-up and the single out-of-target fallback are all done by
	// now, so the claim count is the actual number of preheat nodes each Pod
	// gets. A zero-claim unit is below any positive minimum too.
	for u := range units {
		units[u].decision.MinUnmet = int32(len(st.claims[u])) < fanoutMin
	}
	return buildOutput(in, units, st, rounds, fellBack, fallback)
}

func invalidOutput(in Input, err error) Output {
	pods := make([]int32, 0, len(in.Pods))
	for i := range in.Pods {
		pods = append(pods, in.Pods[i].Index)
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i] < pods[j] })
	return Output{
		Assignment:     map[int32][]string{},
		PreheatSet:     map[string][]int32{},
		TargetSet:      map[int32][]string{},
		Decisions:      map[int32]PodDecision{},
		UnassignedPods: pods,
		Err:            err,
	}
}

// buildUnits merges affinity groups and precomputes the per-node readiness
// aggregate, target set, preference order and dependency (c_ij - alternative)
// data.
func buildUnits(in Input, nodes []node, rng *rand.Rand, fanout, fanoutMin int32) ([]unit, error) {
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
		unitMin, unitMax, err := memberBounds(seed.pods, fanout, fanoutMin)
		if err != nil {
			return nil, fmt.Errorf("affinity group %q: %w", seed.key, err)
		}
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
		// The unit window length is the largest member K: S_min must cover the
		// widest member window the merged unit has to satisfy. All members share
		// the same target set, so the conservative length is the safe one.
		var unitK int32
		for _, index := range u.pods {
			if k := byIndex[index].K; k > unitK {
				unitK = k
			}
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
			// No feasible node: the target set is empty and the unit is
			// unassigned. Record the unavoidable MinUnmet explanation.
			u.decision = PodDecision{MinUnmet: unitMin > 0}
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
			u.decision = PodDecision{MinUnmet: unitMin > 0}
			units = append(units, u)
			continue
		}
		setTops(&u, nodes)
		fullPref := preference(u.c, nodes, rng)
		planUnit(&u, nodes, fullPref, unitK, unitMax, unitMin)
		units = append(units, u)
	}
	return units, nil
}

// memberBounds computes an affinity unit's fanout bounds: the lower bound is
// the maximum of the members' minima and the upper bound the minimum of their
// maxima. PodState does not carry per-Pod bounds yet, so every member inherits
// Input.FanoutMin/Fanout and both folds collapse to the input values. Keeping
// the folds explicit means a future per-Pod constraint only has to change
// this function.
func memberBounds(pods []int32, fanout, fanoutMin int32) (unitMin, unitMax int32, err error) {
	for i := range pods {
		memberMin, memberMax := fanoutMin, fanout
		if i == 0 || memberMin > unitMin {
			unitMin = memberMin
		}
		if i == 0 || memberMax < unitMax {
			unitMax = memberMax
		}
	}
	if unitMin > unitMax {
		return 0, 0, fmt.Errorf("%w: member fanout minimum %d exceeds member maximum %d", ErrInvalidInput, unitMin, unitMax)
	}
	return unitMin, unitMax, nil
}

// planUnit fills the deterministic target set and the claim orders from the
// per-node readiness aggregate. It implements docs/7.1 §5.5 steps 1-9.
//
// target is ordered by c_ij descending and node name ascending, independent of
// the input node order and of the RNG. pref keeps the injected RNG for equal
// c_ij ties, so assignment order stays reproducible per seed while the target
// set itself is stable. fallback holds the excluded feasible nodes.
func planUnit(u *unit, nodes []node, fullPref []int, unitK, fanout, fanoutMin int32) {
	order := targetOrder(u.c, nodes)
	feasible := len(order)
	u.target = nil
	u.pref = nil
	u.fallback = nil
	if feasible == 0 {
		u.decision = PodDecision{MinUnmet: fanoutMin > 0}
		return
	}

	best := u.c[order[0]]
	sMin := int32(0)
	if unitK > 0 {
		sMin = (unitK + 3) / 4
	}

	band := 0
	lowSignal := false
	if best <= float64(sMin) {
		// Low signal: the cache distribution cannot separate nodes, so keep the
		// widest target the upper bound allows.
		lowSignal = true
		band = int(fanout)
	} else {
		// Retention line: 2*c_ij >= best keeps nodes within half of the best.
		for _, n := range order {
			if 2*u.c[n] >= best {
				band++
			}
		}
		if band < 1 {
			band = 1
		}
	}
	if band > int(fanout) {
		band = int(fanout)
	}
	if band > feasible {
		band = feasible
	}

	target := int(fanoutMin)
	if band > target {
		target = band
	}
	if target > int(fanout) {
		target = int(fanout)
	}
	if target > feasible {
		target = feasible
	}
	if target < 0 {
		target = 0
	}

	u.target = append([]int(nil), order[:target]...)
	u.decision = PodDecision{
		TargetFanout: int32(target),
		LowSignal:    lowSignal,
		ForcedByMin:  fanoutMin > int32(band),
		MinUnmet:     fanoutMin > int32(feasible),
	}

	inTarget := make(map[int]struct{}, len(u.target))
	for _, n := range u.target {
		inTarget[n] = struct{}{}
	}
	for _, n := range fullPref {
		if _, ok := inTarget[n]; ok {
			u.pref = append(u.pref, n)
		}
	}
	// Fallback order stays deterministic (c_ij descending, node name ascending)
	// because it is a new M14 path, not one of the existing RNG tie-breaks.
	u.fallback = append([]int(nil), order[target:]...)
	for i, n := range u.pref {
		u.rank[n] = i
	}
	for i, n := range u.fallback {
		u.rank[n] = len(u.pref) + i
	}
}

// targetOrder lists non-stopped nodes by c_ij descending and node name
// ascending. This is the deterministic M14 target-set order.
func targetOrder(c []float64, nodes []node) []int {
	order := make([]int, 0, len(nodes))
	for n := range nodes {
		if !nodes[n].stopped {
			order = append(order, n)
		}
	}
	sort.Slice(order, func(i, j int) bool {
		if c[order[i]] != c[order[j]] {
			return c[order[i]] > c[order[j]]
		}
		return nodes[order[i]].name < nodes[order[j]].name
	})
	return order
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
	// claims holds, per unit, the node positions it currently claims (at most
	// the unit's target count, in discovery order; buildOutput sorts them by
	// c_ij).
	claims [][]int
	// claimed is a units x nodes membership bitmap. Units are small (<= MaxPods)
	// and nodes bounded by MaxNodes, so O(units*nodes) bools are cheap and keep
	// claim removal O(1) before the nodeUnits list is adjusted.
	claimed   [][]bool
	nodeUnits [][]int
}

func newState(units, nodes int) *state {
	st := &state{
		claims:    make([][]int, units),
		claimed:   make([][]bool, units),
		nodeUnits: make([][]int, nodes),
	}
	for i := range st.claimed {
		st.claimed[i] = make([]bool, nodes)
	}
	return st
}

func (s *state) addClaim(u, n int) {
	if s.claimed[u][n] {
		return
	}
	s.claimed[u][n] = true
	s.claims[u] = append(s.claims[u], n)
	s.nodeUnits[n] = append(s.nodeUnits[n], u)
}

func (s *state) removeClaim(u, n int) {
	if !s.claimed[u][n] {
		return
	}
	s.claimed[u][n] = false
	for i, other := range s.claims[u] {
		if other == n {
			s.claims[u] = append(s.claims[u][:i], s.claims[u][i+1:]...)
			break
		}
	}
	for i, other := range s.nodeUnits[n] {
		if other == u {
			s.nodeUnits[n] = append(s.nodeUnits[n][:i], s.nodeUnits[n][i+1:]...)
			break
		}
	}
}

// fillUnit greedily claims up to k acceptable nodes in preference order. This
// is the non-confrontational first pass; it never evicts anyone.
func (s *state) fillUnit(u, k int, units []unit, allowMulti bool) {
	for _, n := range units[u].pref {
		if len(s.claims[u]) >= k {
			return
		}
		if s.nodeAccepts(u, n, units, allowMulti) {
			s.addClaim(u, n)
		}
	}
}

// topUp completes one unit to k claims by scanning its preference order and
// claiming every node that currently accepts it, without evicting anyone. It
// is the final safety net after convergence or fallback: an underfilled unit
// must take any acceptable free node before output.
func (s *state) topUp(u, k int, units []unit, allowMulti bool) {
	for _, n := range units[u].pref {
		if len(s.claims[u]) >= k {
			return
		}
		if s.claimed[u][n] {
			continue
		}
		if s.nodeAccepts(u, n, units, allowMulti) {
			s.addClaim(u, n)
		}
	}
}

func (s *state) nodeAccepts(u, n int, units []unit, allowMulti bool) bool {
	for _, other := range s.nodeUnits[n] {
		if other == u {
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

func buildOutput(in Input, units []unit, st *state, rounds int, fellBack bool, fallback []bool) Output {
	out := Output{
		Assignment: map[int32][]string{},
		PreheatSet: map[string][]int32{},
		TargetSet:  map[int32][]string{},
		Decisions:  map[int32]PodDecision{},
		Rounds:     rounds,
		FellBack:   fellBack,
	}
	for u := range units {
		// Every member of a unit shares the target set and the decision; the
		// fallback flag is per unit because the unit claims as one placement.
		decision := units[u].decision
		if fallback[u] {
			decision.FallbackOutOfTarget = true
		}
		target := make([]string, len(units[u].target))
		for i, n := range units[u].target {
			target[i] = in.Nodes[n].Name
		}
		for _, index := range units[u].pods {
			out.TargetSet[index] = append([]string(nil), target...)
			out.Decisions[index] = decision
		}
		if len(st.claims[u]) == 0 {
			continue
		}
		// Claim discovery order is not necessarily c_ij order after a unit has
		// yielded and re-claimed; rank encodes the (already randomized) pref.
		claims := append([]int(nil), st.claims[u]...)
		sort.Slice(claims, func(i, j int) bool {
			return units[u].rank[claims[i]] < units[u].rank[claims[j]]
		})
		names := make([]string, len(claims))
		for i, n := range claims {
			names[i] = in.Nodes[n].Name
		}
		for _, index := range units[u].pods {
			out.Assignment[index] = append([]string(nil), names...)
			for _, n := range claims {
				name := in.Nodes[n].Name
				out.PreheatSet[name] = append(out.PreheatSet[name], index)
			}
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

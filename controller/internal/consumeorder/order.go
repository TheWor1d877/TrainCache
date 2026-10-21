// Package consumeorder contains the pure default consume-order split used by
// the WarmupTask controller (docs/8.1 design.md §7.2, decisions D29).
//
// The controller never reads the removed legacy order-reference field: a WarmupTask either supplies a
// valid explicit order for every Pod or the whole group falls back to the
// deterministic split implemented here.
package consumeorder

import (
	"errors"
	"fmt"
)

// ErrTotalBelowPods is returned when a default split cannot cover every Pod.
var ErrTotalBelowPods = errors.New("consumeorder: total chunks is less than pod count")

// NeedsDefault reports whether the explicit per-Pod orders must be ignored in
// favour of DefaultSplit.
//
// Orders are invalid when any of the following holds:
//   - the list is empty, or any Pod's order is empty;
//   - a chunk number is negative or >= total;
//   - a chunk number appears more than once (across all Pods);
//   - the orders do not cover every chunk exactly once, i.e. the summed
//     lengths differ from total ("incomplete specification").
func NeedsDefault(orders [][]int32, total int32) bool {
	if total <= 0 || len(orders) == 0 {
		return true
	}

	seen := make(map[int32]struct{}, total)
	var count int64
	for _, order := range orders {
		if len(order) == 0 {
			return true
		}
		for _, chunk := range order {
			if chunk < 0 || chunk >= total {
				return true
			}
			if _, dup := seen[chunk]; dup {
				return true
			}
			seen[chunk] = struct{}{}
			count++
		}
	}
	return count != int64(total)
}

// DefaultSplit returns the deterministic per-Pod chunk windows
//
//	Pod i -> [floor(i*T/P), floor((i+1)*T/P))
//
// for T total chunks and P pods. The windows are ascending, disjoint and cover
// [0,T) exactly. T must be >= P (and P > 0); otherwise ErrTotalBelowPods is
// returned, matching the contract that an empty window is never generated.
func DefaultSplit(total, pods int32) ([][]int32, error) {
	if pods <= 0 {
		return nil, fmt.Errorf("%w: pods=%d", ErrTotalBelowPods, pods)
	}
	if total < pods {
		return nil, fmt.Errorf("%w: total=%d pods=%d", ErrTotalBelowPods, total, pods)
	}

	out := make([][]int32, pods)
	t, p := int64(total), int64(pods)
	for i := int64(0); i < p; i++ {
		start := i * t / p
		end := (i + 1) * t / p
		window := make([]int32, 0, end-start)
		for chunk := start; chunk < end; chunk++ {
			window = append(window, int32(chunk))
		}
		out[i] = window
	}
	return out, nil
}

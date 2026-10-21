package consumeorder

import (
	"errors"
	"testing"
)

func TestDefaultSplitBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		total int32
		pods  int32
		want  [][]int32
	}{
		{name: "T equals P", total: 3, pods: 3, want: [][]int32{{0}, {1}, {2}}},
		{name: "T equals P plus one", total: 4, pods: 3, want: [][]int32{{0}, {1}, {2, 3}}},
		{name: "single pod", total: 5, pods: 1, want: [][]int32{{0, 1, 2, 3, 4}}},
		{name: "one chunk per pod", total: 1, pods: 1, want: [][]int32{{0}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DefaultSplit(tt.total, tt.pods)
			if err != nil {
				t.Fatalf("DefaultSplit(%d,%d): %v", tt.total, tt.pods, err)
			}
			assertWindows(t, got, tt.want, tt.total)
		})
	}
}

func TestDefaultSplitRejectsTotalBelowPods(t *testing.T) {
	for _, tc := range []struct{ total, pods int32 }{{2, 3}, {0, 1}, {1, 2}} {
		if _, err := DefaultSplit(tc.total, tc.pods); !errors.Is(err, ErrTotalBelowPods) {
			t.Fatalf("DefaultSplit(%d,%d) err = %v, want ErrTotalBelowPods", tc.total, tc.pods, err)
		}
	}
	if _, err := DefaultSplit(3, 0); !errors.Is(err, ErrTotalBelowPods) {
		t.Fatalf("DefaultSplit(3,0) err = %v, want ErrTotalBelowPods", err)
	}
}

func TestDefaultSplitCoversRangeWithoutOverlap(t *testing.T) {
	for total := int32(1); total <= 40; total++ {
		for pods := int32(1); pods <= total; pods++ {
			got, err := DefaultSplit(total, pods)
			if err != nil {
				t.Fatalf("DefaultSplit(%d,%d): %v", total, pods, err)
			}
			if len(got) != int(pods) {
				t.Fatalf("DefaultSplit(%d,%d) = %d windows, want %d", total, pods, len(got), pods)
			}
			seen := make(map[int32]bool, total)
			next := int32(0)
			for i, window := range got {
				if len(window) == 0 {
					t.Fatalf("DefaultSplit(%d,%d) window %d is empty", total, pods, i)
				}
				for _, chunk := range window {
					if chunk != next {
						t.Fatalf("DefaultSplit(%d,%d) window %d has %d, want %d", total, pods, i, chunk, next)
					}
					next++
					if seen[chunk] {
						t.Fatalf("DefaultSplit(%d,%d) duplicate chunk %d", total, pods, chunk)
					}
					seen[chunk] = true
				}
			}
			if next != total {
				t.Fatalf("DefaultSplit(%d,%d) covered %d chunks, want %d", total, pods, next, total)
			}
		}
	}
}

func TestNeedsDefault(t *testing.T) {
	tests := []struct {
		name   string
		orders [][]int32
		total  int32
		want   bool
	}{
		{name: "valid explicit split", orders: [][]int32{{0}, {1, 2}}, total: 3, want: false},
		{name: "valid uneven split", orders: [][]int32{{0, 1, 2}, {3}}, total: 4, want: false},
		{name: "no pods", orders: nil, total: 3, want: true},
		{name: "empty pod order", orders: [][]int32{{0}, {}}, total: 3, want: true},
		{name: "negative chunk", orders: [][]int32{{-1}, {0, 1, 2}}, total: 3, want: true},
		{name: "chunk at total", orders: [][]int32{{0}, {1, 2, 3}}, total: 3, want: true},
		{name: "duplicate chunk across pods", orders: [][]int32{{0, 1}, {1, 2}}, total: 3, want: true},
		{name: "incomplete coverage", orders: [][]int32{{0}, {2}}, total: 3, want: true},
		{name: "zero total", orders: [][]int32{{0}}, total: 0, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NeedsDefault(tt.orders, tt.total); got != tt.want {
				t.Fatalf("NeedsDefault(%v,%d) = %v, want %v", tt.orders, tt.total, got, tt.want)
			}
		})
	}
}

func assertWindows(t *testing.T, got, want [][]int32, total int32) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d windows, want %d: %v", len(got), len(want), got)
	}
	next := int32(0)
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("window %d = %v, want %v", i, got[i], want[i])
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Fatalf("window %d = %v, want %v", i, got[i], want[i])
			}
			if got[i][j] != next {
				t.Fatalf("window %d is not contiguous at %d, want %d", i, got[i][j], next)
			}
			next++
		}
	}
	if next != total {
		t.Fatalf("windows cover %d chunks, want %d", next, total)
	}
}

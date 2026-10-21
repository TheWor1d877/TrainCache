package metrics

import "testing"

func TestNewReturnsEmptyRegistry(t *testing.T) {
	r := New()
	if r == nil {
		t.Fatal("New() = nil")
	}
	if r.Registerer == nil || r.Gatherer == nil {
		t.Fatalf("registry views are nil: registerer=%v gatherer=%v", r.Registerer, r.Gatherer)
	}

	families, err := r.Gatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() error: %v", err)
	}
	if len(families) != 0 {
		t.Fatalf("Gather() returned %d families, want 0", len(families))
	}
}

func TestNewCreatesIndependentRegistries(t *testing.T) {
	first := New()
	second := New()
	if first == second {
		t.Fatal("New() returned the same registry twice")
	}
	if first.Registerer == second.Registerer {
		t.Fatal("separate New() calls share a Registerer")
	}
	if first.Gatherer == second.Gatherer {
		t.Fatal("separate New() calls share a Gatherer")
	}

	// A third call stays non-nil and empty, proving New is repeatable without
	// accumulating state.
	third := New()
	families, err := third.Gatherer.Gather()
	if err != nil {
		t.Fatalf("third Gather() error: %v", err)
	}
	if len(families) != 0 {
		t.Fatalf("third registry has %d families, want 0", len(families))
	}
}

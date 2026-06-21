package dedup

import (
	"encoding/json"
	"math/rand"
	"testing"
)

func TestDenseSequenceCollapsesToOneRange(t *testing.T) {
	var s IntervalSet
	for id := uint64(1); id <= 1000; id++ {
		if !s.Add(id) {
			t.Fatalf("id %d reported as duplicate on first add", id)
		}
	}
	if s.NumRanges() != 1 {
		t.Fatalf("dense sequence should collapse to 1 range, got %d", s.NumRanges())
	}
	if !s.Contains(1) || !s.Contains(500) || !s.Contains(1000) {
		t.Fatal("missing ids after dense add")
	}
	if s.Contains(1001) {
		t.Fatal("contains id never added")
	}
}

func TestDuplicateAddReturnsFalse(t *testing.T) {
	var s IntervalSet
	if !s.Add(5) {
		t.Fatal("first add should be true")
	}
	if s.Add(5) {
		t.Fatal("second add of same id should be false")
	}
}

func TestOutOfOrderFillsGaps(t *testing.T) {
	var s IntervalSet
	for _, id := range []uint64{3, 1, 5, 4, 2} {
		s.Add(id)
	}
	if s.NumRanges() != 1 {
		t.Fatalf("gaps should have filled to 1 range, got %d", s.NumRanges())
	}
	for id := uint64(1); id <= 5; id++ {
		if !s.Contains(id) {
			t.Fatalf("missing %d", id)
		}
	}
}

func TestGapsStayUntilFilled(t *testing.T) {
	var s IntervalSet
	s.Add(1)
	s.Add(2)
	s.Add(5)
	if s.NumRanges() != 2 {
		t.Fatalf("expected 2 ranges with a gap, got %d", s.NumRanges())
	}
	if s.Contains(3) || s.Contains(4) {
		t.Fatal("gap ids should not be present")
	}
	s.Add(4)
	s.Add(3)
	if s.NumRanges() != 1 {
		t.Fatalf("expected 1 range after bridging, got %d", s.NumRanges())
	}
}

func TestAgainstOracleAndRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	var s IntervalSet
	oracle := map[uint64]bool{}
	for i := 0; i < 5000; i++ {
		id := uint64(rng.Intn(300) + 1)
		newOracle := !oracle[id]
		oracle[id] = true
		if got := s.Add(id); got != newOracle {
			t.Fatalf("Add(%d)=%v, oracle expected %v", id, got, newOracle)
		}
	}
	for id := uint64(1); id <= 300; id++ {
		if s.Contains(id) != oracle[id] {
			t.Fatalf("Contains(%d)=%v, oracle %v", id, s.Contains(id), oracle[id])
		}
	}

	data, err := json.Marshal(&s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var restored IntervalSet
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for id := uint64(1); id <= 300; id++ {
		if restored.Contains(id) != oracle[id] {
			t.Fatalf("after round-trip Contains(%d)=%v, oracle %v", id, restored.Contains(id), oracle[id])
		}
	}
}

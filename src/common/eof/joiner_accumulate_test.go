package eof

import "testing"

func TestJoinerAccumulateWaitsUntilExpected(t *testing.T) {
	j := NewJoinerAccumulateCoordinator(3)

	if a := j.OnUpstreamEOF("client-x", 10); a.Kind != ActionNone {
		t.Fatalf("first EOF should be Wait, got %v", a.Kind)
	}
	if a := j.OnUpstreamEOF("client-x", 20); a.Kind != ActionNone {
		t.Fatalf("second EOF should be Wait, got %v", a.Kind)
	}
	a := j.OnUpstreamEOF("client-x", 30)
	if a.Kind != ActionEmitEOFs {
		t.Fatalf("third EOF should emit, got %v", a.Kind)
	}
	if len(a.EOFs) != 1 || a.EOFs[0].OutputIndex != 0 || a.EOFs[0].Total != 60 {
		t.Fatalf("aggregated EOF wrong: %+v", a.EOFs)
	}
}

func TestJoinerAccumulateIndependentPerClient(t *testing.T) {
	j := NewJoinerAccumulateCoordinator(2)

	_ = j.OnUpstreamEOF("client-a", 5)
	_ = j.OnUpstreamEOF("client-b", 7)

	a := j.OnUpstreamEOF("client-a", 9)
	if a.Kind != ActionEmitEOFs || a.EOFs[0].Total != 14 {
		t.Fatalf("client-a aggregation wrong: %+v", a)
	}
	// client-b still pending (only saw 1 of 2 EOFs).
	a = j.OnUpstreamEOF("client-b", 13)
	if a.Kind != ActionEmitEOFs || a.EOFs[0].Total != 20 {
		t.Fatalf("client-b aggregation wrong: %+v", a)
	}
}

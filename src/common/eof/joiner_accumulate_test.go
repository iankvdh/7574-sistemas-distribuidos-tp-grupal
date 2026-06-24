package eof

import (
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

const jacClient = inner.ClientID("6ac219ee-b26f-44d4-8ff3-0de38f2f7ebd")

const enough = uint64(1 << 30)

func TestJACFinalizesWhenAllReceived(t *testing.T) {
	j := NewJoinerAccumulateCoordinator(2, 1)
	if a := j.OnUpstreamEOF(jacClient, 10, 0, 3, enough); a.Kind != ActionNone {
		t.Fatalf("primer EOF: want ActionNone, got %v", a.Kind)
	}
	a := j.OnUpstreamEOF(jacClient, 11, 0, 4, enough)
	if a.Kind != ActionEmitEOFs {
		t.Fatalf("segundo EOF: want ActionEmitEOFs, got %v", a.Kind)
	}
	if len(a.EOFs) != 1 || a.EOFs[0].Total != 7 {
		t.Fatalf("Total agregado mal: %+v", a.EOFs)
	}
}

func TestJACRedeliveryAfterFinalizeIsNoop(t *testing.T) {
	j := NewJoinerAccumulateCoordinator(2, 1)
	j.OnUpstreamEOF(jacClient, 10, 0, 3, enough)
	if a := j.OnUpstreamEOF(jacClient, 11, 0, 4, enough); a.Kind != ActionEmitEOFs {
		t.Fatalf("debería finalizar, got %v", a.Kind)
	}
	if a := j.OnUpstreamEOF(jacClient, 10, 0, 3, enough); a.Kind != ActionNone {
		t.Fatalf("redelivery A tras finalizar: want ActionNone, got %v", a.Kind)
	}
	if a := j.OnUpstreamEOF(jacClient, 11, 0, 4, enough); a.Kind != ActionNone {
		t.Fatalf("redelivery B tras finalizar: want ActionNone, got %v", a.Kind)
	}
}

func TestJACFinalizedSurvivesCheckpoint(t *testing.T) {
	j := NewJoinerAccumulateCoordinator(2, 1)
	j.OnUpstreamEOF(jacClient, 10, 0, 3, enough)
	j.OnUpstreamEOF(jacClient, 11, 0, 4, enough)

	snap := j.GetClientJACState(jacClient)
	if !snap.Finalized {
		t.Fatal("snapshot debería tener Finalized=true")
	}

	j2 := NewJoinerAccumulateCoordinator(2, 1)
	j2.RestoreClientJACState(jacClient, snap)
	if a := j2.OnUpstreamEOF(jacClient, 10, 0, 3, enough); a.Kind != ActionNone {
		t.Fatalf("redelivery tras restore: want ActionNone, got %v", a.Kind)
	}
}

func TestJACDoesNotResetAfterPartialRedelivery(t *testing.T) {
	j := NewJoinerAccumulateCoordinator(2, 1)
	j.OnUpstreamEOF(jacClient, 10, 0, 3, enough)
	j.OnUpstreamEOF(jacClient, 11, 0, 4, enough)
	for i := 0; i < 3; i++ {
		if a := j.OnUpstreamEOF(jacClient, 11, 0, 4, enough); a.Kind != ActionNone {
			t.Fatalf("iter %d: want ActionNone (sin stall ni re-emit), got %v", i, a.Kind)
		}
	}
}

func TestJACWaitsForDataBeforeFinalizing(t *testing.T) {
	j := NewJoinerAccumulateCoordinator(1, 1)
	if a := j.OnUpstreamEOF(jacClient, 10, 0, 5, 3); a.Kind != ActionReenqueueUpstreamEOF {
		t.Fatalf("localCount<AggTotal: want Reenqueue, got %v", a.Kind)
	}
	if a := j.OnUpstreamEOF(jacClient, 10, 0, 5, 5); a.Kind != ActionEmitEOFs {
		t.Fatalf("localCount==AggTotal: want EmitEOFs, got %v", a.Kind)
	}
}

func TestJACSkipsCountCheckWhenTotalZero(t *testing.T) {
	j := NewJoinerAccumulateCoordinator(1, 1)
	if a := j.OnUpstreamEOF(jacClient, 10, 0, 0, 0); a.Kind != ActionEmitEOFs {
		t.Fatalf("AggTotal=0: want EmitEOFs (skip check), got %v", a.Kind)
	}
}

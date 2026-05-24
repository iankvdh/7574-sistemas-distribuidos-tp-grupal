package counter

import (
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

func newCounter(t *testing.T, nPathFinders, nFinalJoiners, minIntermediates int) *Counter {
	t.Helper()
	t.Setenv("N_PATH_FINDERS", itoa(nPathFinders))
	t.Setenv("N_FINAL_JOINERS", itoa(nFinalJoiners))
	t.Setenv("MIN_INTERMEDIATES", itoa(minIntermediates))
	c := New()
	if err := c.Init(strategy.StrategyConfig{OutputCount: 1, NReplicas: 1}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	return c
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func feedTriplet(t *testing.T, c *Counter, srcAcc, midAcc, dstAcc string) []strategy.OutputMessage {
	t.Helper()
	body, err := inner.SerializeSuspiciousPath(&inner.SuspiciousPath{
		SourceBank: 1, SourceAccount: srcAcc,
		IntermediateBank: 1, IntermediateAccount: midAcc,
		DestBank: 1, DestAccount: dstAcc,
	})
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	out, _, err := c.ProcessMessage(&inner.Envelope{
		Kind:     inner.SuspiciousPathMessage,
		ClientID: "client-x",
		Payload:  body,
	})
	if err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	return out
}

func TestCounterEmitsOnceUmbralAlcanzado(t *testing.T) {
	c := newCounter(t, 1, 1, 3)

	// 2 intermediarios distintos: aún por debajo del umbral.
	if out := feedTriplet(t, c, "S", "I1", "D"); len(out) != 0 {
		t.Fatalf("below threshold should not emit, got %d", len(out))
	}
	if out := feedTriplet(t, c, "S", "I2", "D"); len(out) != 0 {
		t.Fatalf("below threshold should not emit, got %d", len(out))
	}
	// El 3er intermediario distinto cierra el umbral; debe emitir.
	out := feedTriplet(t, c, "S", "I3", "D")
	if len(out) != 1 {
		t.Fatalf("must emit when threshold reached, got %d", len(out))
	}
	if out[0].BatchItemKind != inner.Query4PairItem {
		t.Fatalf("expected Query4PairItem, got %d", out[0].BatchItemKind)
	}
	if out[0].BatchQueryID != 4 {
		t.Fatalf("expected BatchQueryID=4, got %d", out[0].BatchQueryID)
	}
	pair, err := inner.DeserializeQuery4Pair(out[0].Body)
	if err != nil {
		t.Fatalf("deserialize pair: %v", err)
	}
	if pair.SourceAccount != "S" || pair.DestAccount != "D" {
		t.Fatalf("unexpected pair: %+v", pair)
	}

	// Más intermediarios para el mismo par: ya está emitido, no debe re-emitir.
	if out := feedTriplet(t, c, "S", "I4", "D"); len(out) != 0 {
		t.Fatalf("after emit, further triplets must not emit, got %d", len(out))
	}
	if out := feedTriplet(t, c, "S", "I5", "D"); len(out) != 0 {
		t.Fatalf("after emit, further triplets must not emit, got %d", len(out))
	}
}

func TestCounterDeduplicatesIntermediates(t *testing.T) {
	c := newCounter(t, 1, 1, 3)

	// Tres mensajes con el mismo intermediario; el set sigue en 1.
	if out := feedTriplet(t, c, "S", "I1", "D"); len(out) != 0 {
		t.Fatalf("must not emit on first, got %d", len(out))
	}
	if out := feedTriplet(t, c, "S", "I1", "D"); len(out) != 0 {
		t.Fatalf("duplicate must not change state, got %d", len(out))
	}
	if out := feedTriplet(t, c, "S", "I1", "D"); len(out) != 0 {
		t.Fatalf("duplicate must not change state, got %d", len(out))
	}
}

func TestCounterEmitsEOFForClient(t *testing.T) {
	c := newCounter(t, 2, 1, 5)

	if out, err := c.OnUpstreamEOF(&inner.Envelope{ClientID: "client-x"}); err != nil || len(out.EOFs) != 0 {
		t.Fatalf("first EOF must not emit downstream: err=%v out=%+v", err, out)
	}
	outcome, err := c.OnUpstreamEOF(&inner.Envelope{ClientID: "client-x"})
	if err != nil {
		t.Fatalf("second EOF: %v", err)
	}
	if len(outcome.EOFs) != 1 {
		t.Fatalf("expected 1 final EOF, got %d", len(outcome.EOFs))
	}
	if outcome.EOFs[0].RoutingKey == "" {
		t.Fatalf("EOF must include the client's routing key")
	}
}

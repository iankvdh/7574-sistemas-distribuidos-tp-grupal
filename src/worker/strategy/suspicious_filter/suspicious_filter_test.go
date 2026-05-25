package suspicious_filter

import (
	"os"
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

func newFilter(t *testing.T, nSharders, kPathFinders, threshold int) *SuspiciousFilter {
	t.Helper()
	t.Setenv("N_SHARDERS", itoa(nSharders))
	t.Setenv("K_PATH_FINDERS", itoa(kPathFinders))
	t.Setenv("SUSPICIOUS_THRESHOLD", itoa(threshold))
	f := New()
	if err := f.Init(strategy.StrategyConfig{OutputCount: 1, NReplicas: 1}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	return f
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

func mkShardedTx(bySource bool, fromBank uint32, fromAcc string, toBank uint32, toAcc string) []byte {
	body, err := inner.SerializeShardedTx(&inner.ShardedTx{
		FromBank:        fromBank,
		FromAccount:     fromAcc,
		ToBank:          toBank,
		ToAccount:       toAcc,
		ShardedBySource: bySource,
	})
	if err != nil {
		panic(err)
	}
	return body
}

func feed(t *testing.T, f *SuspiciousFilter, bySource bool, fromBank uint32, fromAcc string, toBank uint32, toAcc string) []strategy.OutputMessage {
	t.Helper()
	out, _, err := f.ProcessMessage(&inner.Envelope{
		Kind:     inner.ShardedTxMessage,
		ClientID: "client-x",
		Payload:  mkShardedTx(bySource, fromBank, fromAcc, toBank, toAcc),
	})
	if err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	return out
}

func TestNoEmitBelowOutThreshold(t *testing.T) {
	f := newFilter(t, 1, 1, 5)
	for _, acc := range []string{"Y1", "Y2", "Y3", "Y4"} {
		if out := feed(t, f, true, 1, "X", 2, acc); len(out) != 0 {
			t.Fatalf("below threshold should not emit (peer=%s), got %d", acc, len(out))
		}
	}
}

func TestEmitsAllOnReachingOutThreshold(t *testing.T) {
	f := newFilter(t, 1, 1, 5)
	for _, acc := range []string{"Y1", "Y2", "Y3", "Y4"} {
		feed(t, f, true, 1, "X", 2, acc)
	}
	out := feed(t, f, true, 1, "X", 2, "Y5")
	if len(out) != 5 {
		t.Fatalf("threshold reached must emit 5, got %d", len(out))
	}
	seen := map[string]bool{}
	for _, msg := range out {
		stx, err := inner.DeserializeShardedTx(msg.Body)
		if err != nil {
			t.Fatalf("deserialize: %v", err)
		}
		if stx.ShardedBySource {
			t.Fatalf("emitted msg must have ShardedBySource=false (inverted), got true")
		}
		if stx.FromAccount != "X" {
			t.Fatalf("emitted msg must keep source=X, got %s", stx.FromAccount)
		}
		seen[stx.ToAccount] = true
	}
	for _, acc := range []string{"Y1", "Y2", "Y3", "Y4", "Y5"} {
		if !seen[acc] {
			t.Fatalf("expected Y=%s in emitted set", acc)
		}
	}
}

func TestEmitsAllOnReachingInThreshold(t *testing.T) {
	f := newFilter(t, 1, 1, 5)
	for _, acc := range []string{"W1", "W2", "W3", "W4"} {
		if out := feed(t, f, false, 1, acc, 9, "X"); len(out) != 0 {
			t.Fatalf("below threshold (W=%s) should not emit", acc)
		}
	}
	out := feed(t, f, false, 1, "W5", 9, "X")
	if len(out) != 5 {
		t.Fatalf("threshold reached must emit 5, got %d", len(out))
	}
	seen := map[string]bool{}
	for _, msg := range out {
		stx, err := inner.DeserializeShardedTx(msg.Body)
		if err != nil {
			t.Fatalf("deserialize: %v", err)
		}
		if !stx.ShardedBySource {
			t.Fatalf("emitted msg must have ShardedBySource=true (inverted), got false")
		}
		if stx.ToAccount != "X" {
			t.Fatalf("emitted msg must keep dest=X, got %s", stx.ToAccount)
		}
		seen[stx.FromAccount] = true
	}
	for _, acc := range []string{"W1", "W2", "W3", "W4", "W5"} {
		if !seen[acc] {
			t.Fatalf("expected W=%s in emitted set", acc)
		}
	}
}

func TestAfterSuspiciousReemitsEveryTx(t *testing.T) {
	f := newFilter(t, 1, 1, 3)
	for _, acc := range []string{"Y1", "Y2", "Y3"} {
		feed(t, f, true, 1, "X", 2, acc) // tercera dispara umbral; emite 3
	}
	// Nueva tx X→Y4 — la cuenta X ya es suspicious en out.
	out := feed(t, f, true, 1, "X", 2, "Y4")
	if len(out) != 1 {
		t.Fatalf("post-suspicious new tx must emit 1, got %d", len(out))
	}
	stx, _ := inner.DeserializeShardedTx(out[0].Body)
	if stx.ShardedBySource {
		t.Fatalf("post-suspicious emit must have flag inverted (false)")
	}
	if stx.ToAccount != "Y4" {
		t.Fatalf("expected ToAccount=Y4, got %s", stx.ToAccount)
	}

	// Repetida: Y1 ya estaba — igual se reenvía.
	out = feed(t, f, true, 1, "X", 2, "Y1")
	if len(out) != 1 {
		t.Fatalf("post-suspicious duplicate must emit 1 (no dedup), got %d", len(out))
	}
}

func TestDiscardsBeforeThresholdDuplicate(t *testing.T) {
	f := newFilter(t, 1, 1, 5)
	feed(t, f, true, 1, "X", 2, "Y1")
	if out := feed(t, f, true, 1, "X", 2, "Y1"); len(out) != 0 {
		t.Fatalf("duplicate before threshold must not emit, got %d", len(out))
	}
	// Y1 + Y2..Y4 = 4 cuentas: aún por debajo de 5.
	for _, acc := range []string{"Y2", "Y3", "Y4"} {
		if out := feed(t, f, true, 1, "X", 2, acc); len(out) != 0 {
			t.Fatalf("4 cuentas distintas no deberían disparar umbral=5, got %d", len(out))
		}
	}
}

func TestOutAndInIndependent(t *testing.T) {
	f := newFilter(t, 1, 1, 5)
	// Llenamos outAccounts(X) hasta el umbral.
	for _, acc := range []string{"Y1", "Y2", "Y3", "Y4", "Y5"} {
		feed(t, f, true, 1, "X", 2, acc)
	}
	// inAccounts(X) sigue intacto: una nueva tx W→X no debe disparar nada
	// (aún por debajo del umbral en in).
	if out := feed(t, f, false, 7, "W1", 1, "X"); len(out) != 0 {
		t.Fatalf("inAccounts(X) sigue por debajo del umbral: no debe emitir, got %d", len(out))
	}
}

func TestEmitsEOFsAcrossAllPathFinders(t *testing.T) {
	f := newFilter(t, 2, 3, 5)
	outcome, err := f.OnUpstreamEOF(&inner.Envelope{ClientID: "client-x", Total: 0})
	if err != nil {
		t.Fatalf("OnUpstreamEOF #1: %v", err)
	}
	if len(outcome.EOFs) != 0 {
		t.Fatalf("first EOF must not emit downstream, got %d", len(outcome.EOFs))
	}

	outcome, err = f.OnUpstreamEOF(&inner.Envelope{ClientID: "client-x", Total: 0})
	if err != nil {
		t.Fatalf("OnUpstreamEOF #2: %v", err)
	}
	if len(outcome.EOFs) != 3 {
		t.Fatalf("expected 3 EOFEmits, got %d", len(outcome.EOFs))
	}
	seenKeys := map[string]bool{}
	for _, e := range outcome.EOFs {
		if e.OutputIndex != 0 {
			t.Fatalf("emit expected OutputIndex=0, got %d", e.OutputIndex)
		}
		seenKeys[e.RoutingKey] = true
	}
	for _, key := range []string{"0", "1", "2"} {
		if !seenKeys[key] {
			t.Fatalf("missing routing key %q in EOFs", key)
		}
	}
}

func TestInitRequiresEnv(t *testing.T) {
	os.Unsetenv("N_SHARDERS")
	os.Unsetenv("K_PATH_FINDERS")
	f := New()
	if err := f.Init(strategy.StrategyConfig{OutputCount: 1, NReplicas: 1}); err == nil {
		t.Fatalf("expected error when N_SHARDERS/K_PATH_FINDERS missing")
	}

	t.Setenv("N_SHARDERS", "1")
	f = New()
	if err := f.Init(strategy.StrategyConfig{OutputCount: 1, NReplicas: 1}); err == nil {
		t.Fatalf("expected error when K_PATH_FINDERS missing")
	}
}

func TestInitDefaultsThreshold(t *testing.T) {
	os.Unsetenv("SUSPICIOUS_THRESHOLD")
	t.Setenv("N_SHARDERS", "1")
	t.Setenv("K_PATH_FINDERS", "1")
	f := New()
	if err := f.Init(strategy.StrategyConfig{OutputCount: 1, NReplicas: 1}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	if f.threshold != 5 {
		t.Fatalf("expected default threshold 5, got %d", f.threshold)
	}
}

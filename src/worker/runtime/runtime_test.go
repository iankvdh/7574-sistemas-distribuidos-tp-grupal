package runtime

import (
	"testing"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

func TestRetryUpstreamEOFMessageBumpsSeqPastLastReceived(t *testing.T) {
	clientID := inner.ClientID("ef87cae8-a5dd-416e-9fe0-f9f4c9e8c279")
	original := &inner.EOFMessage{
		Header: inner.Header{
			GatewayID:       3,
			ClientID:        clientID,
			SeqID:           7,
			SenderStageType: inner.StageFilterCurrencyUsdP2,
			SenderReplicaID: 1,
		},
		QueryID: 3,
		Total:   42,
	}
	msg, err := serialize(original)
	if err != nil {
		t.Fatalf("serialize original EOF: %v", err)
	}

	worker := &Worker{
		lastRecvSeqID: map[inner.SeqKey]uint64{
			{
				ClientID:  clientID,
				StageType: inner.StageFilterCurrencyUsdP2,
				ReplicaID: 1,
			}: 9,
		},
	}

	retry, err := worker.retryUpstreamEOFMessage(clientID, *msg)
	if err != nil {
		t.Fatalf("retryUpstreamEOFMessage: %v", err)
	}
	parsed, err := inner.NewFromSerializedData([]byte(retry.Body))
	if err != nil {
		t.Fatalf("parse retry EOF: %v", err)
	}
	eofMessage, ok := parsed.(*inner.EOFMessage)
	if !ok {
		t.Fatalf("retry message type = %T, want *inner.EOFMessage", parsed)
	}

	if eofMessage.SeqID != 10 {
		t.Fatalf("retry SeqID = %d, want 10", eofMessage.SeqID)
	}
	if eofMessage.ClientID != clientID ||
		eofMessage.GatewayID != original.GatewayID ||
		eofMessage.SenderStageType != original.SenderStageType ||
		eofMessage.SenderReplicaID != original.SenderReplicaID ||
		eofMessage.QueryID != original.QueryID ||
		eofMessage.Total != original.Total {
		t.Fatalf("retry EOF changed fields: got %+v want original %+v with bumped SeqID", eofMessage, original)
	}
}

func TestDirectExchangeInputQueueNameIsStable(t *testing.T) {
	got := directExchangeInputQueueName("results", "0")
	if got != "direct_results_0" {
		t.Fatalf("directExchangeInputQueueName() = %q, want %q", got, "direct_results_0")
	}
}

func TestContentHashDataShardIsStableForSameMessage(t *testing.T) {
	clientID := inner.ClientID("6ac219ee-b26f-44d4-8ff3-0de38f2f7ebd")
	worker := &Worker{}
	target := OutputTarget{
		Kind:       config.KindContentHashQueues,
		ShardCount: 3,
	}
	output := strategy.OutputMessage{
		ClientID:      clientID,
		Body:          []byte("tx|account-a|account-b|123.45"),
		RoutingKey:    "matched",
		BatchItemKind: inner.TransactionMessage,
		BatchQueryID:  1,
	}

	firstShard := worker.dataShard(2, target, output)
	for i := 0; i < 10; i++ {
		if got := worker.dataShard(2, target, output); got != firstShard {
			t.Fatalf("dataShard() changed from %d to %d for the same message", firstShard, got)
		}
	}
}

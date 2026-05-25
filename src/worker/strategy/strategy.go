package strategy

import (
	"iter"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

type OutputMessage struct {
	OutputIndices []int // output target indices to send the message to
	Body          []byte
	ClientID      inner.ClientID // used for sharding; ignored if output is not sharded
	RoutingKey    string
	BatchItemKind inner.MsgKind
	BatchQueryID  uint8
}

type LocalCounts struct {
	Processed  uint64
	Matched    uint64
	NotMatched uint64
}

type StrategyConfig struct {
	OutputCount  int
	ReplicaID    int
	NReplicas    int
	StrategyName string
	// MatchCount splits outputs into match [0, MatchCount) and no-match
	// [MatchCount, OutputCount). Filters use it; other strategies ignore it.
	MatchCount int
	// NumInputs is the number of INPUT queues/exchanges the worker is consuming
	// from. Strategies that bind multiple streams (e.g. filter_q3) use it to
	// validate their wiring; single-input strategies can ignore it.
	NumInputs    int
	RingQueueIn  string
	RingQueueOut string
}

type EOFOutcome struct {
	Action     eof.Action
	EOFs       []eof.EOFEmit
	Outputs    []OutputMessage // data messages to emit before downstream EOFs
	OutputsSeq iter.Seq[OutputMessage]
}

type Strategy interface {
	Init(cfg StrategyConfig) error
	ProcessMessage(env *inner.Envelope) ([]OutputMessage, LocalCounts, error)
	OnUpstreamEOF(env *inner.Envelope) (EOFOutcome, error)
	OnRingToken(token *eof.Token) (EOFOutcome, error)
	Name() string
}

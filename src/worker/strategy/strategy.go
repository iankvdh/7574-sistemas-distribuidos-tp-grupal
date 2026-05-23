package strategy

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

// Decision is one item the runtime must publish. OutputIndices reference
// Context outputs by position; ClientID picks the shard when an output is
// sharded by client. Body carries the raw payload bytes (e.g. the serialized
// transaction). The runtime batches multiple Decisions into a single InnerBatch
// envelope per (output, shard, client).
type Decision struct {
	OutputIndices []int
	Body          string
	ClientID      inner.ClientID
}

type LocalCounts struct {
	Processed  uint64
	Matched    uint64
	NotMatched uint64
}

type Context struct {
	OutputCount  int
	ReplicaID    int
	NReplicas    int
	StrategyName string
	// MatchCount splits outputs into match [0, MatchCount) and no-match
	// [MatchCount, OutputCount). Filters use it; other strategies ignore it.
	MatchCount int
}

type EOFOutcome struct {
	Action eof.Action
	EOFs   []eof.EOFEmit
}

type Strategy interface {
	Init(ctx Context) error
	ProcessMessage(env *inner.Envelope) ([]Decision, LocalCounts, error)
	OnUpstreamEOF(env *inner.Envelope) (EOFOutcome, error)
	OnRingToken(token *eof.Token) (EOFOutcome, error)
	Name() string
}

package strategy

import (
	"log/slog"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

// Decision describes one payload that the runtime must publish to one or more outputs.
// OutputIndices reference Context outputs by position. ClientID is used by the runtime
// to pick the right shard when an output is sharded by client_id.
type Decision struct {
	OutputIndices []int
	Body          string
	ClientID      inner.ClientID
}

// LocalCounts are per-message counts produced by the strategy. The runtime aggregates them per
// (clientID) to feed the ring token.
type LocalCounts struct {
	Processed  uint64
	Matched    uint64
	NotMatched uint64
}

// Context is the immutable handle a strategy receives at Init time.
type Context struct {
	Logger       *slog.Logger
	OutputCount  int
	ReplicaID    int
	NReplicas    int
	StrategyName string
	// MatchCount is the number of outputs in [0, MatchCount) that the strategy
	// must treat as "match" destinations. The remaining outputs [MatchCount, OutputCount)
	// are "no-match" destinations. Filter strategies use this to split semantically;
	// non-filter strategies typically ignore it (the runtime defaults it to OutputCount).
	MatchCount int
}

// EOFOutcome bundles what the runtime needs to act on an EOF-related event: the
// action returned by the strategy's EOF topology plus, if the action is EmitEOFs,
// the list of EOFEmit entries to publish.
type EOFOutcome struct {
	Action eof.Action
	EOFs   []eof.EOFEmit
}

// Strategy is the contract every worker behaviour implements.
type Strategy interface {
	Init(ctx Context) error
	// ProcessMessage handles a single inner.Envelope (one transaction / account).
	ProcessMessage(env *inner.Envelope) ([]Decision, LocalCounts, error)
	// OnUpstreamEOF handles an EOF arriving on the input queue. The strategy delegates
	// to its EOF topology and returns the resulting outcome.
	OnUpstreamEOF(env *inner.Envelope) (EOFOutcome, error)
	// OnRingToken handles a ring token arriving on the private ring queue. Only
	// strategies whose EOF topology is "ring" use this; others should return
	// EOFOutcome{Action: {Kind: ActionNone}} without error.
	OnRingToken(token *eof.Token) (EOFOutcome, error)
	Name() string
}

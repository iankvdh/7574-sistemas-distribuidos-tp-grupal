package strategy

import (
	"errors"
	"iter"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

var ErrInvalidData = errors.New("strategy: invalid data")

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
	MatchCount   int
	NumInputs    int
	RingQueueIn  string
	RingQueueOut string
}

type EOFOutcome struct {
	Action              eof.Action
	EOFs                []eof.EOFEmit
	Outputs             []OutputMessage // data messages to emit before downstream EOFs
	OutputsIterator     iter.Seq[OutputMessage]
	StartDeferredInputs []int
	ClientCompleted     bool
}

type NoopValidator struct{}

func (NoopValidator) Validate(_ *inner.Envelope) error { return nil }

type Strategy interface {
	Init(cfg StrategyConfig) error
	Validate(env *inner.Envelope) error
	ProcessMessage(env *inner.Envelope) ([]OutputMessage, LocalCounts, error)
	OnUpstreamEOF(env *inner.Envelope) (EOFOutcome, error)
	OnRingToken(token *eof.Token, localCount uint64) (EOFOutcome, error)
}

type DeferredInputProvider interface {
	DeferredInputs() []int
}

type ReadyEOFEmitter interface {
	ReadyEOFs(env *inner.Envelope) (EOFOutcome, bool)
}

type RecoverableStrategy interface {
	Strategy
	MarshalClientState(clientID inner.ClientID) ([]byte, error)
	UnmarshalClientState(clientID inner.ClientID, data []byte) error
	CleanupClient(clientID inner.ClientID)
}

type GlobalStateProvider interface {
	MarshalGlobalState() ([]byte, error)
	UnmarshalGlobalState(data []byte) error
}

type RawBatchStrategy interface {
	Strategy
	ProcessRawBatch(batch *inner.BatchMessage, rawBatch []byte, inputIndex int) (
		outputs []OutputMessage, counts LocalCounts, handled bool, err error,
	)
}

type DedupRecoverer interface {
	RecoveredDedupHeaders() []inner.Header
}

type BufferFlusher interface {
	CloseAllBuffers()
}

type DeltaCheckpointer interface {
	RecoverableStrategy
	TakeDelta(clientID inner.ClientID) []byte
	ApplyDelta(clientID inner.ClientID, delta []byte) error
}

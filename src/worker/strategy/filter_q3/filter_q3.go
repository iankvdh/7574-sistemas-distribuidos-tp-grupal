package filter_q3

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/eof"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/hashing"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const queryID uint8 = 3
const bufferPerm = 0o755

const (
	inputIndexP2       = 0
	inputIndexAverages = 1
)

type clientState struct {
	averages            map[string]float64
	averagesEOFReceived bool
	p2EOFReceived       bool
	p2Seen              uint64
	bufferPath          string
	bufferFile          *os.File
	bufferWriter        *bufio.Writer
	bufferCount         uint64
}

type FilterQ3 struct {
	cfg           strategy.StrategyConfig
	nFinalJoiners int
	bufferDir     string
	thresholdPct  float64
	avgCoord      *eof.JoinerAccumulateCoordinator
	p2Ring        *eof.RingCoordinator
	state         map[inner.ClientID]*clientState
}

func New() *FilterQ3 {
	return &FilterQ3{state: map[inner.ClientID]*clientState{}}
}

func (f *FilterQ3) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("filter_q3 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	if cfg.NumInputs != 2 {
		return fmt.Errorf("filter_q3 expects exactly 2 inputs (P2 queue + averages exchange), got %d", cfg.NumInputs)
	}
	if cfg.NReplicas > 1 && (cfg.RingQueueIn == "" || cfg.RingQueueOut == "") {
		return fmt.Errorf("filter_q3 requires RING_QUEUE_IN/RING_QUEUE_OUT when N_REPLICAS>1")
	}
	nFinal, err := env.RequiredInt("N_FINAL_JOINERS", true)
	if err != nil {
		return err
	}
	pctStr := env.StringWithDefault("AMOUNT_THRESHOLD_PCT", "0.01")
	pct, err := strconv.ParseFloat(pctStr, 64)
	if err != nil || pct <= 0 {
		return fmt.Errorf("AMOUNT_THRESHOLD_PCT must be a positive float, got %q", pctStr)
	}
	dir := env.StringWithDefault("BUFFER_DIR", "/tmp")
	if err := os.MkdirAll(dir, bufferPerm); err != nil {
		return fmt.Errorf("create buffer dir %q: %w", dir, err)
	}
	f.cfg = cfg
	f.nFinalJoiners = nFinal
	f.thresholdPct = pct
	f.bufferDir = dir
	f.avgCoord = eof.NewJoinerAccumulateCoordinator(1, 1)
	f.p2Ring = eof.NewBroadcastRingCoordinator(cfg.ReplicaID, cfg.NReplicas)
	return nil
}

func (f *FilterQ3) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	switch envelope.InputIndex {
	case inputIndexAverages:
		if envelope.Kind != inner.Q3AverageItem {
			return nil, strategy.LocalCounts{}, fmt.Errorf("filter_q3 averages input: unexpected kind=%d", envelope.Kind)
		}
		avg, err := inner.DeserializeQ3Average(envelope.Payload)
		if err != nil {
			return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize Q3Average: %w", err)
		}
		st := f.stateFor(envelope.ClientID)
		st.averages[avg.PaymentFormat] = avg.Average
		return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil

	case inputIndexP2:
		if envelope.Kind != inner.TransactionMessage {
			return nil, strategy.LocalCounts{}, fmt.Errorf("filter_q3 P2 input: unexpected kind=%d", envelope.Kind)
		}
		tx, err := external.DeserializeTransaction(envelope.Payload)
		if err != nil {
			return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize transaction: %w", err)
		}
		st := f.stateFor(envelope.ClientID)
		st.p2Seen++
		if st.averagesEOFReceived {
			if msg, emitted := f.inlineFilter(envelope.ClientID, st, tx); emitted {
				return []strategy.OutputMessage{msg}, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
			}
			return nil, strategy.LocalCounts{Processed: 1, NotMatched: 1}, nil
		}
		if err := f.appendToBuffer(st, envelope.Payload); err != nil {
			return nil, strategy.LocalCounts{}, fmt.Errorf("buffer P2 tx: %w", err)
		}
		return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil

	default:
		return nil, strategy.LocalCounts{}, fmt.Errorf("filter_q3 unexpected InputIndex=%d", envelope.InputIndex)
	}
}

func (f *FilterQ3) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	st := f.stateFor(envelope.ClientID)

	if envelope.InputIndex == inputIndexAverages {
		action := f.avgCoord.OnUpstreamEOF(envelope.ClientID, envelope.SenderStageType, envelope.SenderReplicaID, envelope.Total)
		if action.Kind != eof.ActionEmitEOFs {
			return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
		}
		st.averagesEOFReceived = true
		if st.p2EOFReceived {
			return f.finalizeAndEmit(envelope.ClientID, st, false, nil)
		}
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
	}

	// EOF of stream P2 (InputIndex == 0)
	action, _ := f.p2Ring.OnUpstreamEOF(envelope.ClientID, envelope.Total, st.p2Seen, 0)
	return f.handleRingAction(envelope.ClientID, st, action)
}

func (f *FilterQ3) OnRingToken(token *eof.Token) (strategy.EOFOutcome, error) {
	st := f.stateFor(token.ClientID)
	action, _ := f.p2Ring.OnRingToken(token, st.p2Seen, 0)
	return f.handleRingAction(token.ClientID, st, action)
}

func (f *FilterQ3) handleRingAction(clientID inner.ClientID, st *clientState, action eof.Action) (strategy.EOFOutcome, error) {
	switch action.Kind {
	case eof.ActionNone, eof.ActionReenqueueUpstreamEOF:
		return strategy.EOFOutcome{Action: action}, nil

	case eof.ActionForwardToken:
		return strategy.EOFOutcome{Action: action}, nil

	case eof.ActionEmitEOFs:
		st.p2EOFReceived = true
		if st.averagesEOFReceived {
			return f.finalizeAndEmit(clientID, st, false, nil)
		}
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil

	case eof.ActionEmitEOFsAndForwardToken:
		st.p2EOFReceived = true
		if st.averagesEOFReceived {
			return f.finalizeAndEmit(clientID, st, true, action.Token)
		}
		return strategy.EOFOutcome{
			Action: eof.Action{Kind: eof.ActionForwardToken, Token: action.Token},
		}, nil

	default:
		return strategy.EOFOutcome{}, fmt.Errorf("filter_q3: unexpected ring action kind=%d", action.Kind)
	}
}

func (f *FilterQ3) finalizeAndEmit(clientID inner.ClientID, st *clientState, forwardToken bool, token *eof.Token) (strategy.EOFOutcome, error) {
	if err := f.closeBufferForRead(st); err != nil {
		return strategy.EOFOutcome{}, err
	}
	seq, err := f.drainIterator(clientID, st)
	if err != nil {
		return strategy.EOFOutcome{}, err
	}

	rk := f.routingKeyFor(clientID)
	delete(f.state, clientID)

	outcome := strategy.EOFOutcome{
		OutputsIterator: seq,
		EOFs: []eof.EOFEmit{{
			OutputIndex: 0,
			RoutingKey:  rk,
			QueryID:     queryID,
		}},
	}
	if forwardToken {
		outcome.Action = eof.Action{Kind: eof.ActionEmitEOFsAndForwardToken, Token: token}
	} else {
		outcome.Action = eof.Action{Kind: eof.ActionEmitEOFs}
	}
	return outcome, nil
}

func (f *FilterQ3) inlineFilter(clientID inner.ClientID, st *clientState, tx *transaction.Transaction) (strategy.OutputMessage, bool) {
	avg, ok := st.averages[tx.PaymentFormat]
	if !ok {
		return strategy.OutputMessage{}, false
	}
	if tx.AmountPaid >= avg*f.thresholdPct {
		return strategy.OutputMessage{}, false
	}
	body, err := inner.SerializeQuery3Row(&inner.Query3Row{
		SourceBank:    tx.FromBank,
		SourceAccount: tx.FromAccount,
		Amount:        tx.AmountPaid,
	})
	if err != nil {
		return strategy.OutputMessage{}, false
	}
	return strategy.OutputMessage{
		OutputIndices: []int{0},
		Body:          body,
		ClientID:      clientID,
		RoutingKey:    f.routingKeyFor(clientID),
		BatchItemKind: inner.Query3RowItem,
		BatchQueryID:  queryID,
	}, true
}

func (f *FilterQ3) appendToBuffer(st *clientState, payload []byte) error {
	if st.bufferFile == nil {
		file, err := os.Create(st.bufferPath)
		if err != nil {
			return fmt.Errorf("open buffer file %q: %w", st.bufferPath, err)
		}
		st.bufferFile = file
		st.bufferWriter = bufio.NewWriter(file)
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := st.bufferWriter.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := st.bufferWriter.Write(payload); err != nil {
		return err
	}
	st.bufferCount++
	return nil
}

func (f *FilterQ3) closeBufferForRead(st *clientState) error {
	if st.bufferFile == nil && st.bufferCount == 0 {
		return nil
	}
	if st.bufferWriter != nil {
		if err := st.bufferWriter.Flush(); err != nil {
			return fmt.Errorf("flush buffer: %w", err)
		}
	}
	if st.bufferFile != nil {
		if err := st.bufferFile.Close(); err != nil {
			return fmt.Errorf("close buffer for write: %w", err)
		}
	}
	st.bufferFile = nil
	st.bufferWriter = nil
	return nil
}

func (f *FilterQ3) drainIterator(clientID inner.ClientID, st *clientState) (iter.Seq[strategy.OutputMessage], error) {
	if st.bufferCount == 0 {
		return func(yield func(strategy.OutputMessage) bool) {}, nil
	}
	path := st.bufferPath
	rk := f.routingKeyFor(clientID)
	averages := st.averages

	return func(yield func(strategy.OutputMessage) bool) {
		file, err := os.Open(path)
		if err != nil {
			slog.Error("filter_q3 reopen buffer file failed", "client_id", clientID, "path", path, "err", err)
			return
		}
		defer func() {
			_ = file.Close()
			_ = os.Remove(path)
		}()

		reader := bufio.NewReader(file)
		for {
			var lenBuf [4]byte
			if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
				if err == io.EOF {
					return
				}
				slog.Error("filter_q3 read buffer length failed", "client_id", clientID, "err", err)
				return
			}
			payloadLen := binary.BigEndian.Uint32(lenBuf[:])
			payload := make([]byte, payloadLen)
			if _, err := io.ReadFull(reader, payload); err != nil {
				slog.Error("filter_q3 read buffer payload failed", "client_id", clientID, "err", err)
				return
			}
			tx, err := external.DeserializeTransaction(payload)
			if err != nil {
				slog.Error("filter_q3 deserialize buffered tx failed", "client_id", clientID, "err", err)
				return
			}
			avg, ok := averages[tx.PaymentFormat]
			if !ok || tx.AmountPaid >= avg*f.thresholdPct {
				continue
			}
			body, err := inner.SerializeQuery3Row(&inner.Query3Row{
				SourceBank:    tx.FromBank,
				SourceAccount: tx.FromAccount,
				Amount:        tx.AmountPaid,
			})
			if err != nil {
				slog.Error("filter_q3 serialize query3 row failed", "client_id", clientID, "err", err)
				return
			}
			if !yield(strategy.OutputMessage{
				OutputIndices: []int{0},
				Body:          body,
				ClientID:      clientID,
				RoutingKey:    rk,
				BatchItemKind: inner.Query3RowItem,
				BatchQueryID:  queryID,
			}) {
				return
			}
		}
	}, nil
}

func (f *FilterQ3) routingKeyFor(clientID inner.ClientID) string {
	return strconv.Itoa(hashing.Shard(string(clientID), f.nFinalJoiners))
}

func (f *FilterQ3) stateFor(clientID inner.ClientID) *clientState {
	st, ok := f.state[clientID]
	if !ok {
		st = &clientState{
			averages:   map[string]float64{},
			bufferPath: filepath.Join(f.bufferDir, "q3_buf_"+string(clientID)+".bin"),
		}
		f.state[clientID] = st
	}
	return st
}


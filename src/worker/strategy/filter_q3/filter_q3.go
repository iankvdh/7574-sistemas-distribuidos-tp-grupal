package filter_q3

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
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
	bufferedSeen        uint64
	averagesSeen        uint64
	recoveredHeaders []inner.Header
}

type filterQ3Checkpoint struct {
	AveragesEOFReceived bool                  `json:"avgEOF"`
	P2EOFReceived       bool                  `json:"p2EOF"`
	P2Seen              uint64                `json:"p2Seen"`
	AveragesSeen        uint64                `json:"avgSeen,omitempty"`
	BufferedSeen        *uint64               `json:"bufSeen,omitempty"`
	Averages            map[string]float64    `json:"avg,omitempty"`
	BufferPath          string                `json:"buf"`
	P2Ring              eof.RingStateSnapshot `json:"p2Ring"`
	AvgCoord            eof.JACStateSnapshot  `json:"avgCoord"`
}

type FilterQ3 struct {
	strategy.NoopValidator
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

func (f *FilterQ3) ProcessRawBatch(
	batch *inner.BatchMessage,
	rawBatch []byte,
	inputIndex int,
) (outputs []strategy.OutputMessage, counts strategy.LocalCounts, handled bool, err error) {
	if inputIndex != inputIndexP2 {
		return nil, counts, false, nil
	}
	state := f.stateFor(batch.ClientID)
	itemCount := uint64(len(batch.Items))

	if state.averagesEOFReceived {
		out, filterErr := f.filterBatchItems(batch, state)
		if filterErr == nil {
			state.p2Seen += itemCount
		}
		return out, counts, true, filterErr
	}
	if err := f.appendToBuffer(state, rawBatch); err != nil {
		return nil, counts, false, fmt.Errorf("buffer P2 batch: %w", err)
	}
	state.p2Seen += itemCount
	state.bufferedSeen += itemCount
	return nil, counts, true, nil
}

func (f *FilterQ3) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	if envelope.InputIndex != inputIndexAverages {
		return nil, strategy.LocalCounts{}, fmt.Errorf("filter_q3 unexpected InputIndex=%d in ProcessMessage", envelope.InputIndex)
	}
	if envelope.Kind != inner.Q3AverageItem {
		return nil, strategy.LocalCounts{}, fmt.Errorf("filter_q3 averages input: unexpected kind=%d", envelope.Kind)
	}
	avg, err := inner.DeserializeQ3Average(envelope.Payload)
	if err != nil {
		return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize Q3Average: %w", err)
	}
	state := f.stateFor(envelope.ClientID)
	state.averages[avg.PaymentFormat] = avg.Average
	state.averagesSeen++
	return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
}

func (f *FilterQ3) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	st := f.stateFor(envelope.ClientID)

	if envelope.InputIndex == inputIndexAverages {
		action := f.avgCoord.OnUpstreamEOF(envelope.ClientID, envelope.SenderStageType, envelope.SenderReplicaID, envelope.Total, st.averagesSeen)
		if action.Kind != eof.ActionEmitEOFs {
			return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
		}
		st.averagesEOFReceived = true
		if st.p2EOFReceived {
			return f.finalizeAndEmit(envelope.ClientID, st, false, nil)
		}
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
	}

	action, _ := f.p2Ring.OnUpstreamEOF(envelope.ClientID, envelope.Total, st.p2Seen, 0)
	return f.handleRingAction(envelope.ClientID, st, action)
}

func (f *FilterQ3) OnRingToken(token *eof.Token, _ uint64) (strategy.EOFOutcome, error) {
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
	q3Count := f.countQ3Rows(clientID, st)
	seq, err := f.drainIterator(clientID, st)
	if err != nil {
		return strategy.EOFOutcome{}, err
	}

	rk := f.routingKeyFor(clientID)

	outcome := strategy.EOFOutcome{
		OutputsIterator: seq,
		ClientCompleted: true,
		EOFs: []eof.EOFEmit{{
			OutputIndex: 0,
			RoutingKey:  rk,
			QueryID:     queryID,
			Total:       q3Count,
		}},
	}
	if forwardToken {
		outcome.Action = eof.Action{Kind: eof.ActionEmitEOFsAndForwardToken, Token: token}
	} else {
		outcome.Action = eof.Action{Kind: eof.ActionEmitEOFs}
	}
	return outcome, nil
}

func (f *FilterQ3) filterBatchItems(batch *inner.BatchMessage, state *clientState) ([]strategy.OutputMessage, error) {
	var outputs []strategy.OutputMessage
	rk := f.routingKeyFor(batch.ClientID)
	for _, item := range batch.Items {
		tx, _, err := external.DeserializeTransaction(item.Payload)
		if err != nil {
			continue
		}
		avg, ok := state.averages[tx.PaymentFormat]
		if !ok || tx.AmountPaid >= avg*f.thresholdPct {
			continue
		}
		body, err := inner.SerializeQuery3Row(&inner.Query3Row{
			SourceBank:    tx.FromBank,
			SourceAccount: tx.FromAccount,
			Amount:        tx.AmountPaid,
		})
		if err != nil {
			continue
		}
		outputs = append(outputs, strategy.OutputMessage{
			OutputIndices: []int{0},
			Body:          body,
			ClientID:      batch.ClientID,
			RoutingKey:    rk,
			BatchItemKind: inner.Query3RowItem,
			BatchQueryID:  queryID,
		})
	}
	return outputs, nil
}

func (f *FilterQ3) appendToBuffer(state *clientState, rawBatch []byte) error {
	if state.bufferFile == nil {
		file, err := os.OpenFile(state.bufferPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("open buffer file %q: %w", state.bufferPath, err)
		}
		state.bufferFile = file
		state.bufferWriter = bufio.NewWriter(file)
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(rawBatch)))
	if _, err := state.bufferWriter.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := state.bufferWriter.Write(rawBatch); err != nil {
		return err
	}
	if err := state.bufferWriter.Flush(); err != nil {
		return err
	}
	state.bufferCount++
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

func (f *FilterQ3) recoverBufferState(state *clientState) uint64 {
	state.recoveredHeaders = nil
	file, err := os.OpenFile(state.bufferPath, os.O_RDWR, 0o644)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		slog.Warn("filter_q3 recoverBufferState: open failed", "path", state.bufferPath, "err", err)
		return 0
	}
	defer file.Close()

	headerSz := uint32(inner.HeaderWireSize())
	var validOffset int64
	var recoveredItems uint64
	reader := bufio.NewReader(file)

	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			if err == io.EOF {
				break
			}
			_ = file.Truncate(validOffset)
			break
		}
		payloadLen := binary.BigEndian.Uint32(lenBuf[:])
		if payloadLen < headerSz {
			_ = file.Truncate(validOffset)
			break
		}
		raw := make([]byte, payloadLen)
		if _, err := io.ReadFull(reader, raw); err != nil {
			_ = file.Truncate(validOffset)
			break
		}
		msgType, h, err := inner.PeekHeader(raw)
		if err != nil || msgType != inner.Batch {
			_ = file.Truncate(validOffset)
			break
		}
		parsed, err := inner.NewFromSerializedData(raw)
		if err != nil {
			_ = file.Truncate(validOffset)
			break
		}
		batch, ok := parsed.(*inner.BatchMessage)
		if !ok {
			_ = file.Truncate(validOffset)
			break
		}
		state.recoveredHeaders = append(state.recoveredHeaders, h)
		state.bufferCount++
		recoveredItems += uint64(len(batch.Items))
		validOffset += 4 + int64(payloadLen)
	}
	return recoveredItems
}

func (f *FilterQ3) drainIterator(clientID inner.ClientID, st *clientState) (iter.Seq[strategy.OutputMessage], error) {
	if st.bufferCount == 0 {
		return func(yield func(strategy.OutputMessage) bool) {}, nil
	}
	path := st.bufferPath
	rk := f.routingKeyFor(clientID)
	averages := st.averages
	threshold := f.thresholdPct

	return func(yield func(strategy.OutputMessage) bool) {
		file, err := os.Open(path)
		if err != nil {
			if !os.IsNotExist(err) {
				slog.Error("filter_q3 reopen buffer file failed", "client_id", clientID, "path", path, "err", err)
			}
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
				if err != io.EOF {
					slog.Error("filter_q3 read buffer length failed", "client_id", clientID, "err", err)
				}
				return
			}
			payloadLen := binary.BigEndian.Uint32(lenBuf[:])
			raw := make([]byte, payloadLen)
			if _, err := io.ReadFull(reader, raw); err != nil {
				slog.Error("filter_q3 read buffer payload failed", "client_id", clientID, "err", err)
				return
			}
			parsed, err := inner.NewFromSerializedData(raw)
			if err != nil {
				slog.Error("filter_q3 parse buffered batch failed", "client_id", clientID, "err", err)
				return
			}
			batch, ok := parsed.(*inner.BatchMessage)
			if !ok {
				continue
			}
			for _, item := range batch.Items {
				tx, _, err := external.DeserializeTransaction(item.Payload)
				if err != nil {
					continue
				}
				if !passesQ3Filter(tx.PaymentFormat, tx.AmountPaid, averages, threshold) {
					continue
				}
				body, err := inner.SerializeQuery3Row(&inner.Query3Row{
					SourceBank:    tx.FromBank,
					SourceAccount: tx.FromAccount,
					Amount:        tx.AmountPaid,
				})
				if err != nil {
					continue
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
		}
	}, nil
}

func passesQ3Filter(paymentFormat string, amountPaid float64, averages map[string]float64, threshold float64) bool {
	avg, ok := averages[paymentFormat]
	return ok && amountPaid < avg*threshold
}

func (f *FilterQ3) countQ3Rows(clientID inner.ClientID, st *clientState) uint32 {
	file, err := os.Open(st.bufferPath)
	if err != nil {
		return 0
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var count uint32
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			return count
		}
		payloadLen := binary.BigEndian.Uint32(lenBuf[:])
		raw := make([]byte, payloadLen)
		if _, err := io.ReadFull(reader, raw); err != nil {
			return count
		}
		parsed, err := inner.NewFromSerializedData(raw)
		if err != nil {
			return count
		}
		batch, ok := parsed.(*inner.BatchMessage)
		if !ok {
			continue
		}
		for _, item := range batch.Items {
			tx, _, err := external.DeserializeTransaction(item.Payload)
			if err != nil {
				continue
			}
			if passesQ3Filter(tx.PaymentFormat, tx.AmountPaid, st.averages, f.thresholdPct) {
				count++
			}
		}
	}
}

func (f *FilterQ3) MarshalClientState(clientID inner.ClientID) ([]byte, error) {
	state := f.stateFor(clientID)
	ringSnap, _ := f.p2Ring.GetClientRingState(clientID)
	bufferedSeen := state.bufferedSeen
	checkPoint := filterQ3Checkpoint{
		AveragesEOFReceived: state.averagesEOFReceived,
		P2EOFReceived:       state.p2EOFReceived,
		P2Seen:              state.p2Seen,
		AveragesSeen:        state.averagesSeen,
		BufferedSeen:        &bufferedSeen,
		Averages:            state.averages,
		BufferPath:          state.bufferPath,
		P2Ring:              ringSnap,
		AvgCoord:            f.avgCoord.GetClientJACState(clientID),
	}
	return json.Marshal(checkPoint)
}

func (f *FilterQ3) UnmarshalClientState(clientID inner.ClientID, data []byte) error {
	var checkPoint filterQ3Checkpoint
	if err := json.Unmarshal(data, &checkPoint); err != nil {
		return err
	}
	state := f.stateFor(clientID)
	state.averagesEOFReceived = checkPoint.AveragesEOFReceived
	state.p2EOFReceived = checkPoint.P2EOFReceived
	state.p2Seen = checkPoint.P2Seen
	state.averagesSeen = checkPoint.AveragesSeen
	checkpointedBufferedSeen := uint64(0)
	if checkPoint.BufferedSeen != nil {
		checkpointedBufferedSeen = *checkPoint.BufferedSeen
		state.bufferedSeen = checkpointedBufferedSeen
	}
	if checkPoint.Averages != nil {
		state.averages = checkPoint.Averages
	}
	if checkPoint.BufferPath != "" {
		state.bufferPath = checkPoint.BufferPath
	}
	f.p2Ring.RestoreClientRingState(clientID, checkPoint.P2Ring)
	f.avgCoord.RestoreClientJACState(clientID, checkPoint.AvgCoord)
	recoveredBufferedSeen := f.recoverBufferState(state)
	if checkPoint.BufferedSeen != nil {
		if recoveredBufferedSeen > checkpointedBufferedSeen {
			state.p2Seen += recoveredBufferedSeen - checkpointedBufferedSeen
		}
	} else if recoveredBufferedSeen > state.p2Seen {
		// Legacy checkpoints did not persist how much of p2Seen came from the
		// buffer, so use the buffer as a lower bound without double-counting.
		state.p2Seen = recoveredBufferedSeen
	}
	state.bufferedSeen = recoveredBufferedSeen
	return nil
}

func (f *FilterQ3) CleanupClient(clientID inner.ClientID) {
	delete(f.state, clientID)
	f.p2Ring.CleanupClient(clientID)
	f.avgCoord.CleanupClient(clientID)
}

func (f *FilterQ3) RecoveredDedupHeaders() []inner.Header {
	var headers []inner.Header
	for _, state := range f.state {
		headers = append(headers, state.recoveredHeaders...)
	}
	return headers
}

func (f *FilterQ3) CloseAllBuffers() {
	for _, state := range f.state {
		if state.bufferWriter != nil {
			_ = state.bufferWriter.Flush()
		}
		if state.bufferFile != nil {
			_ = state.bufferFile.Close()
			state.bufferFile = nil
			state.bufferWriter = nil
		}
	}
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

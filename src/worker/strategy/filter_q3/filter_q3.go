// Package filter_q3 implementa la última etapa de Q3: recibe el promedio
// global por payment_format (de average_global_q3) y el stream de
// transacciones Period 2 USD (de filter_currency_usd_p2). Emite las
// transacciones cuyo monto es estrictamente menor a 1/100 del promedio de
// Period 1 USD para el mismo payment_format.
//
// Para soportar millones de transacciones por cliente sin saturar memoria, las
// txs P2 que llegan antes de que los promedios estén listos se persisten en un
// archivo de spill por cliente (en BUFFER_DIR). Cuando llegan los promedios,
// el archivo se drena secuencialmente, se aplica el filtro, y se borra.
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

type clientState struct {
	averages      map[string]float64
	averagesReady bool
	bufferPath    string
	bufferFile    *os.File
	bufferWriter  *bufio.Writer
	bufferCount   uint64
}

type FilterQ3 struct {
	cfg           strategy.StrategyConfig
	nFinalJoiners int
	bufferDir     string
	coordinator   *eof.JoinerAccumulateCoordinator
	state         map[inner.ClientID]*clientState
}

func New() *FilterQ3 {
	return &FilterQ3{state: map[inner.ClientID]*clientState{}}
}

func (f *FilterQ3) Name() string { return "filter_q3" }

func (f *FilterQ3) Init(cfg strategy.StrategyConfig) error {
	if cfg.OutputCount != 1 {
		return fmt.Errorf("filter_q3 expects exactly 1 output, got %d", cfg.OutputCount)
	}
	expectedEOFs, err := env.RequiredInt("EXPECTED_PARTIAL_EOFS", true)
	if err != nil {
		return err
	}
	nFinal, err := env.RequiredInt("N_FINAL_JOINERS", true)
	if err != nil {
		return err
	}
	dir := env.StringWithDefault("BUFFER_DIR", "/tmp")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create buffer dir %q: %w", dir, err)
	}
	f.cfg = cfg
	f.nFinalJoiners = nFinal
	f.bufferDir = dir
	f.coordinator = eof.NewJoinerAccumulateCoordinator(expectedEOFs, 1)
	return nil
}

func (f *FilterQ3) ProcessMessage(envelope *inner.Envelope) ([]strategy.OutputMessage, strategy.LocalCounts, error) {
	switch envelope.Kind {
	case inner.Q3AverageItem:
		avg, err := inner.DeserializeQ3Average(envelope.Payload)
		if err != nil {
			return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize Q3Average: %w", err)
		}
		st := f.stateFor(envelope.ClientID)
		st.averages[avg.PaymentFormat] = avg.Average
		return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil

	case inner.TransactionMessage:
		tx, err := external.DeserializeTransaction(envelope.Payload)
		if err != nil {
			return nil, strategy.LocalCounts{}, fmt.Errorf("deserialize transaction: %w", err)
		}
		st := f.stateFor(envelope.ClientID)
		if st.averagesReady {
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
		return nil, strategy.LocalCounts{}, fmt.Errorf("filter_q3 unexpected kind=%d", envelope.Kind)
	}
}

func (f *FilterQ3) OnUpstreamEOF(envelope *inner.Envelope) (strategy.EOFOutcome, error) {
	st := f.stateFor(envelope.ClientID)
	isAveragesEOF := envelope.QueryID == queryID

	// Marca averages como listos en cuanto llega su EOF: las tx P2 que arriben
	// entre este EOF y el EOF de P2 se filtran inline en ProcessMessage.
	if isAveragesEOF {
		st.averagesReady = true
	}

	action := f.coordinator.OnUpstreamEOF(envelope.ClientID, envelope.Total)
	if action.Kind != eof.ActionEmitEOFs {
		// Solo un EOF recibido; todavía no podemos drenar (el runtime descarta
		// outputs cuando Action != ActionEmitEOFs). Esperamos al segundo EOF.
		return strategy.EOFOutcome{Action: action}, nil
	}

	if err := f.closeBufferForRead(st); err != nil {
		return strategy.EOFOutcome{}, err
	}
	seq, err := f.drainSeq(envelope.ClientID, st)
	if err != nil {
		return strategy.EOFOutcome{}, err
	}

	rk := f.routingKeyFor(envelope.ClientID)
	delete(f.state, envelope.ClientID)

	return strategy.EOFOutcome{
		Action:     eof.Action{Kind: eof.ActionEmitEOFs},
		OutputsSeq: seq,
		EOFs: []eof.EOFEmit{{
			OutputIndex: 0,
			RoutingKey:  rk,
			QueryID:     queryID,
		}},
	}, nil
}

func (f *FilterQ3) OnRingToken(_ *eof.Token) (strategy.EOFOutcome, error) {
	return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
}

// inlineFilter aplica el filtro a una tx P2 ya con averages disponibles.
func (f *FilterQ3) inlineFilter(clientID inner.ClientID, st *clientState, tx *transaction.Transaction) (strategy.OutputMessage, bool) {
	avg, ok := st.averages[tx.PaymentFormat]
	if !ok || tx.AmountPaid >= avg/100.0 {
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

// appendToBuffer escribe el payload serializado de una tx P2 al archivo de
// spill del cliente, prefijado por su longitud (4B BE).
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

func (f *FilterQ3) drainSeq(clientID inner.ClientID, st *clientState) (iter.Seq[strategy.OutputMessage], error) {
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
			if !ok || tx.AmountPaid >= avg/100.0 {
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
			bufferPath: filepath.Join(f.bufferDir, "q3_buf_"+sanitizeClientID(string(clientID))+".bin"),
		}
		f.state[clientID] = st
	}
	return st
}

// sanitizeClientID convierte un client id a un nombre de archivo seguro.
func sanitizeClientID(clientID string) string {
	out := make([]byte, 0, len(clientID))
	for i := 0; i < len(clientID); i++ {
		c := clientID[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

// Package filter_q3 implementa la última etapa de Q3: recibe el promedio
// global por payment_format (de aggregator_q3) y el stream de transacciones
// Period 2 USD (de filter_currency_usd_p2). Emite las transacciones cuyo
// monto es estrictamente menor a 1/100 del promedio de Period 1 USD para el
// mismo payment_format.
//
// Para soportar N réplicas, cada filter_q3 consume dos INPUTs separados:
//   - INPUT_0: queue compartida (competing consumers) con las txs P2 USD.
//   - INPUT_1: direct exchange con los averages, bindeado por replica_id —
//     aggregator_q3 broadcastea una copia de los averages a cada réplica.
//
// El cierre del stream P2 se coordina con un ring (BroadcastRingCoordinator).
// El cierre del stream de averages es por contador simple: cada cliente
// queda sticky a un único aggregator_q3 (sum_q3 hashea por clientID antes
// de emitir partials), por lo que sólo se espera 1 EOF de averages por
// cliente, sin importar K_AGGREGATORS_Q3.
//
// El drenaje del archivo de spill y la emisión del EOF a final_joiner sólo
// ocurren cuando ambos cierres se cumplieron: si el ring entra en
// PhaseClosing antes de que averages cierre, la réplica forwardea el token
// (preservando la fase) pero NO emite ni drena; cuando llegue el EOF de
// averages drena su spill y emite el EOF downstream.
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

const (
	inputIndexP2       = 0
	inputIndexAverages = 1
)

type clientState struct {
	averages map[string]float64
	// averagesReady se setea con el primer EOF de averages: a partir de ese
	// momento las tx P2 que llegan se intentan filtrar inline (si la map ya
	// tiene el payment_format) en vez de spillarse.
	averagesReady bool
	// averagesEOFDone se setea cuando avgCoord acumuló los K EOFs esperados:
	// la map de averages está completa y se puede drenar el spill final.
	averagesEOFDone bool
	// p2Closed se setea cuando el ring de P2 cerró localmente para este cliente
	// (token Closing visto o N=1).
	p2Closed bool
	// p2Seen cuenta las txs P2 procesadas localmente (para el chequeo de
	// coherencia que hace el RingCoordinator entre el total upstream y la suma
	// agregada por el anillo).
	p2Seen       uint64
	bufferPath   string
	bufferFile   *os.File
	bufferWriter *bufio.Writer
	bufferCount  uint64
}

type FilterQ3 struct {
	cfg           strategy.StrategyConfig
	nFinalJoiners int
	bufferDir     string
	avgCoord      *eof.JoinerAccumulateCoordinator
	p2Ring        *eof.RingCoordinator
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
	dir := env.StringWithDefault("BUFFER_DIR", "/tmp")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create buffer dir %q: %w", dir, err)
	}
	f.cfg = cfg
	f.nFinalJoiners = nFinal
	f.bufferDir = dir
	// Cada cliente queda sticky a un único aggregator_q3 (hashing por clientID
	// hecho upstream en sum_q3), así que sólo se espera 1 EOF de averages.
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
		if st.averagesReady {
			if msg, emitted := f.inlineFilter(envelope.ClientID, st, tx); emitted {
				return []strategy.OutputMessage{msg}, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
			}
			// El average para este payment_format puede no estar todavía (otro
			// aggregator de Q3 lo manda y aún no llegó). Spillar para reintentar
			// en el drain final.
			if _, ok := st.averages[tx.PaymentFormat]; !ok {
				if err := f.appendToBuffer(st, envelope.Payload); err != nil {
					return nil, strategy.LocalCounts{}, fmt.Errorf("buffer P2 tx: %w", err)
				}
				return nil, strategy.LocalCounts{Processed: 1, Matched: 1}, nil
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
		// El primer EOF habilita filtrado inline para tx futuras (la map puede
		// estar parcial si K>1; las txs cuyo formato no está aún se spillan).
		st.averagesReady = true
		action := f.avgCoord.OnUpstreamEOF(envelope.ClientID, envelope.Total)
		if action.Kind != eof.ActionEmitEOFs {
			return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
		}
		st.averagesEOFDone = true
		if st.p2Closed {
			return f.finalizeAndEmit(envelope.ClientID, st, false, nil)
		}
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil
	}

	// EOF del stream P2 (InputIndex == 0)
	action, _ := f.p2Ring.OnUpstreamEOF(envelope.ClientID, envelope.Total, st.p2Seen, 0)
	return f.handleRingAction(envelope.ClientID, st, action)
}

func (f *FilterQ3) OnRingToken(token *eof.Token) (strategy.EOFOutcome, error) {
	st := f.stateFor(token.ClientID)
	action, _ := f.p2Ring.OnRingToken(token, st.p2Seen, 0)
	return f.handleRingAction(token.ClientID, st, action)
}

// handleRingAction adapta lo que devuelve el RingCoordinator al estado actual
// de la composición de coordinators. Si el ring quiere cerrar pero todavía no
// llegaron todos los EOFs de averages, se degrada el cierre a sólo forwardear
// el token: el drain + emit downstream queda para cuando avgCoord cierre.
func (f *FilterQ3) handleRingAction(clientID inner.ClientID, st *clientState, action eof.Action) (strategy.EOFOutcome, error) {
	switch action.Kind {
	case eof.ActionNone, eof.ActionReenqueueUpstreamEOF:
		return strategy.EOFOutcome{Action: action}, nil

	case eof.ActionForwardToken:
		// PhaseCollecting o phase Closing degradada por otra réplica: el runtime
		// sólo forwardea el token; no emitimos ni drenamos.
		return strategy.EOFOutcome{Action: action}, nil

	case eof.ActionEmitEOFs:
		// Ring n=1 finalizó o la réplica initiator recibió de vuelta el token
		// Closing. El stream de P2 cerró para este cliente acá.
		st.p2Closed = true
		if st.averagesEOFDone {
			return f.finalizeAndEmit(clientID, st, false, nil)
		}
		// Esperamos al EOF de averages: el drain + EOF downstream se dispara
		// desde OnUpstreamEOF cuando avgCoord termine.
		return strategy.EOFOutcome{Action: eof.Action{Kind: eof.ActionNone}}, nil

	case eof.ActionEmitEOFsAndForwardToken:
		// Réplica intermedia viendo el token Closing.
		st.p2Closed = true
		if st.averagesEOFDone {
			return f.finalizeAndEmit(clientID, st, true, action.Token)
		}
		// Degradamos a sólo forwardear el token: preservamos PhaseClosing para
		// que las otras réplicas también marquen p2Closed. NO emitimos EOFs ni
		// drenamos. Cuando llegue el EOF de averages drenamos localmente.
		return strategy.EOFOutcome{
			Action: eof.Action{Kind: eof.ActionForwardToken, Token: action.Token},
		}, nil

	default:
		return strategy.EOFOutcome{}, fmt.Errorf("filter_q3: unexpected ring action kind=%d", action.Kind)
	}
}

// finalizeAndEmit cierra el buffer en disco, arma el OutputsSeq lazy para
// drenarlo y devuelve el EOFOutcome con el EOF que va a final_joiner.
func (f *FilterQ3) finalizeAndEmit(clientID inner.ClientID, st *clientState, forwardToken bool, token *eof.Token) (strategy.EOFOutcome, error) {
	if err := f.closeBufferForRead(st); err != nil {
		return strategy.EOFOutcome{}, err
	}
	seq, err := f.drainSeq(clientID, st)
	if err != nil {
		return strategy.EOFOutcome{}, err
	}

	rk := f.routingKeyFor(clientID)
	delete(f.state, clientID)

	outcome := strategy.EOFOutcome{
		OutputsSeq: seq,
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

// inlineFilter aplica el filtro a una tx P2 ya con averages disponibles para
// su payment_format. Si el formato no está en la map devuelve (zero, false)
// y el caller cae al spill.
func (f *FilterQ3) inlineFilter(clientID inner.ClientID, st *clientState, tx *transaction.Transaction) (strategy.OutputMessage, bool) {
	avg, ok := st.averages[tx.PaymentFormat]
	if !ok {
		return strategy.OutputMessage{}, false
	}
	if tx.AmountPaid >= avg/100.0 {
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

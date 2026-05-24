package inner

import (
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
)

type ClientID string
type GatewayID int

type MsgKind uint8

const (
	AllTransactionsBatch MsgKind = iota + 1
	AllTransactionsEOF
	AllAccountsBatch
	AllAccountsEOF
	// FinalQueryResultBatch groups N query results into a single AMQP post.
	// Binary payload: 2B (LE) count | { 1B QueryID | 1B len | len bytes Data } × count
	FinalQueryResultBatch
	// TransactionMessage carries a single transaction (Payload = SerializeTransaction(tx)).
	// Used in internal pipeline queues after the gateway unpacks the external batch.
	TransactionMessage
	// AccountMessage carries a single account (Payload = SerializeAccount(acc)).
	AccountMessage
	// InternalEOF: EOF entre stages internas del pipeline; Total = items publicados por
	// el iniciador del anillo a la output queue correspondiente.
	InternalEOF
	// RingTokenMessage transporta el token del anillo entre réplicas de una misma strategy.
	// Payload = JSON del RingToken.
	RingTokenMessage
	// InnerBatch agrupa N payloads de un mismo item_kind en una única publicación
	// AMQP. El cuerpo binario:
	//   1B item_kind | 2B (LE) count | { 4B (LE) len | len bytes payload } × count
	// El número de query al que pertenece el batch (cuando aplica) viaja en
	// Envelope.QueryID; 0 indica "sin query asociada".
	InnerBatch
	// ShardedTxMessage transporta una transacción ya shardeada por una de sus
	// cuentas (origen o destino). Item dentro de un InnerBatch emitido por
	// el Sharder del pipeline de Q4.
	ShardedTxMessage
	// SuspiciousPathMessage transporta una terna (cuenta origen, cuenta intermedia,
	// cuenta destino). Item dentro de un InnerBatch emitido por el Path-Finder.
	SuspiciousPathMessage
	// Query4PairItem transporta un par (cuenta origen, cuenta destino) que alcanzó
	// el umbral de cuentas intermedias. Item dentro de un InnerBatch emitido
	// por el Counter al exchange `results`.
	Query4PairItem
	// Query1RowItem transporta una transacción USD <50 ya proyectada
	// (cuenta origen, cuenta destino, monto). Item dentro de un InnerBatch
	// emitido por el Sharder de Q1 al exchange `results`.
	Query1RowItem
)

var (
	ErrMalformedEnvelope = errors.New("malformed inner envelope")
	ErrUnexpectedKind    = errors.New("unexpected inner message kind")
)

type Envelope struct {
	GatewayID GatewayID `json:"g,omitempty"`
	ClientID  ClientID  `json:"c"`
	Kind      MsgKind   `json:"k"`
	Total     uint32    `json:"t,omitempty"`
	QueryID   uint8     `json:"q,omitempty"`
	Payload   []byte    `json:"p,omitempty"`
}

// QueryResultItem is an item within a FinalQueryResultBatch: a CSV or "EOF" row.
type QueryResultItem struct {
	QueryID uint8
	Data    string
}

func SerializeEnvelope(kind MsgKind, gatewayID GatewayID, clientID ClientID, total uint32, payload []byte) (*middleware.Message, error) {
	return SerializeEnvelopeWithQuery(kind, gatewayID, clientID, total, 0, payload)
}

func SerializeEnvelopeWithQuery(kind MsgKind, gatewayID GatewayID, clientID ClientID, total uint32, queryID uint8, payload []byte) (*middleware.Message, error) {
	body, err := json.Marshal(Envelope{
		GatewayID: gatewayID,
		ClientID:  clientID,
		Kind:      kind,
		Total:     total,
		QueryID:   queryID,
		Payload:   payload,
	})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeEnvelope(msg *middleware.Message) (*Envelope, error) {
	if msg == nil || msg.Body == "" {
		return nil, ErrMalformedEnvelope
	}

	var envelope Envelope
	if err := json.Unmarshal([]byte(msg.Body), &envelope); err != nil {
		return nil, ErrMalformedEnvelope
	}
	if envelope.ClientID == "" {
		return nil, ErrMalformedEnvelope
	}
	if envelope.GatewayID <= 0 {
		return nil, ErrMalformedEnvelope
	}
	if envelope.Kind == 0 {
		return nil, ErrMalformedEnvelope
	}
	return &envelope, nil
}

func SerializeAllTransactionsBatch(gatewayID GatewayID, clientID ClientID, payload []byte) (*middleware.Message, error) {
	return SerializeEnvelope(AllTransactionsBatch, gatewayID, clientID, 0, payload)
}

func SerializeAllTransactionsEOF(gatewayID GatewayID, clientID ClientID, total uint32) (*middleware.Message, error) {
	return SerializeEnvelope(AllTransactionsEOF, gatewayID, clientID, total, nil)
}

func SerializeAllAccountsBatch(gatewayID GatewayID, clientID ClientID, payload []byte) (*middleware.Message, error) {
	return SerializeEnvelope(AllAccountsBatch, gatewayID, clientID, 0, payload)
}

func SerializeAllAccountsEOF(gatewayID GatewayID, clientID ClientID, total uint32) (*middleware.Message, error) {
	return SerializeEnvelope(AllAccountsEOF, gatewayID, clientID, total, nil)
}

// SerializeTransactionMessage envuelve una transaction serializada en un envelope con Kind=TransactionMessage.
func SerializeTransactionMessage(gatewayID GatewayID, clientID ClientID, payload []byte) (*middleware.Message, error) {
	return SerializeEnvelope(TransactionMessage, gatewayID, clientID, 0, payload)
}

// SerializeAccountMessage envuelve un account serializado en un envelope con Kind=AccountMessage.
func SerializeAccountMessage(gatewayID GatewayID, clientID ClientID, payload []byte) (*middleware.Message, error) {
	return SerializeEnvelope(AccountMessage, gatewayID, clientID, 0, payload)
}

// SerializeInternalEOF emite un EOF interno con total agregado (count emitido a la output).
func SerializeInternalEOF(gatewayID GatewayID, clientID ClientID, total uint32) (*middleware.Message, error) {
	return SerializeEnvelope(InternalEOF, gatewayID, clientID, total, nil)
}

// SerializeRingToken envuelve el token JSON del anillo en un envelope.
func SerializeRingToken(gatewayID GatewayID, clientID ClientID, tokenJSON []byte) (*middleware.Message, error) {
	return SerializeEnvelope(RingTokenMessage, gatewayID, clientID, 0, tokenJSON)
}

func validInnerBatchItemKind(kind MsgKind) bool {
	switch kind {
	case TransactionMessage, AccountMessage, ShardedTxMessage, SuspiciousPathMessage, Query4PairItem, Query1RowItem:
		return true
	default:
		return false
	}
}

// SerializeInnerBatch agrupa N payloads de un mismo item_kind en un envelope
// InnerBatch. items NO debe estar vacío y todos sus elementos deben venir ya
// serializados con la forma esperada por item_kind. El queryID se propaga en
// el envelope (Envelope.QueryID); 0 indica "sin query asociada" — comportamiento
// por default para todos los flujos pre-Q4. Los nodos del pipeline de Q4 usan
// queryID=4 desde el Sharder y propagan ese valor a través de Path-Finder y
// Counter.
func SerializeInnerBatch(queryID uint8, itemKind MsgKind, gatewayID GatewayID, clientID ClientID, items [][]byte) (*middleware.Message, error) {
	if len(items) == 0 {
		return nil, errors.New("inner batch must contain at least one item")
	}
	if len(items) > 0xFFFF {
		return nil, errors.New("inner batch exceeds max items (65535)")
	}
	if !validInnerBatchItemKind(itemKind) {
		return nil, errors.New("inner batch unsupported item kind")
	}

	total := 1 + 2
	for _, it := range items {
		total += 4 + len(it)
	}
	buf := make([]byte, 0, total)
	buf = append(buf, byte(itemKind))
	var countBuf [2]byte
	binary.LittleEndian.PutUint16(countBuf[:], uint16(len(items)))
	buf = append(buf, countBuf[:]...)
	for _, it := range items {
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(it)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, it...)
	}
	return SerializeEnvelopeWithQuery(InnerBatch, gatewayID, clientID, 0, queryID, buf)
}

// DeserializeInnerBatch parsea el payload binario de un envelope InnerBatch.
// Retorna el item_kind original y la lista de payloads sin decodificar. El
// QueryID está en env.QueryID y debe ser leído por el caller cuando aplique.
func DeserializeInnerBatch(env *Envelope) (MsgKind, [][]byte, error) {
	if env == nil || env.Kind != InnerBatch {
		return 0, nil, ErrUnexpectedKind
	}
	p := env.Payload
	if len(p) < 3 {
		return 0, nil, ErrMalformedEnvelope
	}
	itemKind := MsgKind(p[0])
	if !validInnerBatchItemKind(itemKind) {
		return 0, nil, ErrMalformedEnvelope
	}
	count := binary.LittleEndian.Uint16(p[1:3])
	items, err := readBatchItems(p[3:], int(count))
	if err != nil {
		return 0, nil, err
	}
	return itemKind, items, nil
}

func readBatchItems(p []byte, count int) ([][]byte, error) {
	items := make([][]byte, 0, count)
	off := 0
	for i := 0; i < count; i++ {
		if off+4 > len(p) {
			return nil, ErrMalformedEnvelope
		}
		itemLen := binary.LittleEndian.Uint32(p[off : off+4])
		off += 4
		if off+int(itemLen) > len(p) {
			return nil, ErrMalformedEnvelope
		}
		items = append(items, p[off:off+int(itemLen)])
		off += int(itemLen)
	}
	if off != len(p) {
		return nil, ErrMalformedEnvelope
	}
	return items, nil
}

func SerializeFinalQueryResultBatch(gatewayID GatewayID, clientID ClientID, items []QueryResultItem) (*middleware.Message, error) {
	if len(items) == 0 {
		return nil, errors.New("result batch must contain at least one item")
	}
	if len(items) > 0xFFFF {
		return nil, errors.New("result batch exceeds max items (65535)")
	}
	size := 2
	for _, item := range items {
		size += 1 + 1 + len(item.Data)
	}
	buf := make([]byte, 0, size)
	var countBuf [2]byte
	binary.LittleEndian.PutUint16(countBuf[:], uint16(len(items)))
	buf = append(buf, countBuf[:]...)
	for _, item := range items {
		buf = append(buf, item.QueryID)
		buf = append(buf, byte(len(item.Data)))
		buf = append(buf, item.Data...)
	}
	return SerializeEnvelope(FinalQueryResultBatch, gatewayID, clientID, 0, buf)
}

func DeserializeFinalQueryResultBatch(env *Envelope) ([]QueryResultItem, error) {
	if env == nil || env.Kind != FinalQueryResultBatch {
		return nil, ErrUnexpectedKind
	}
	p := env.Payload
	if len(p) < 2 {
		return nil, ErrMalformedEnvelope
	}
	count := binary.LittleEndian.Uint16(p[0:2])
	items := make([]QueryResultItem, 0, count)
	off := 2
	for i := 0; i < int(count); i++ {
		if off+2 > len(p) {
			return nil, ErrMalformedEnvelope
		}
		queryID := p[off]
		dataLen := int(p[off+1])
		off += 2
		if off+dataLen > len(p) {
			return nil, ErrMalformedEnvelope
		}
		items = append(items, QueryResultItem{
			QueryID: queryID,
			Data:    string(p[off : off+dataLen]),
		})
		off += dataLen
	}
	if off != len(p) {
		return nil, ErrMalformedEnvelope
	}
	return items, nil
}

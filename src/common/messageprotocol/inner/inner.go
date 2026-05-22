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
	FinalQueryResult
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
	// InnerBatch agrupa N payloads de un mismo item_kind (TransactionMessage o
	// AccountMessage) en una única publicación AMQP. El cuerpo binario:
	//   1B item_kind | 2B (LE) count | { 4B (LE) len | len bytes payload } × count
	InnerBatch
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
	Payload   []byte    `json:"p,omitempty"` // batches de transacciones, cuentas, el token del ring coordinator
	QueryID   uint8     `json:"q,omitempty"`
	Data      string    `json:"d,omitempty"` // solo lo usa FinalQueryResult, y transporta exactamente una de dos cosas: una fila CSV de resultado
}

func SerializeEnvelope(kind MsgKind, gatewayID GatewayID, clientID ClientID, total uint32, payload []byte) (*middleware.Message, error) {
	body, err := json.Marshal(Envelope{
		GatewayID: gatewayID,
		ClientID:  clientID,
		Kind:      kind,
		Total:     total,
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

// SerializeInnerBatch agrupa N payloads de un mismo item_kind en un envelope
// InnerBatch. items NO debe estar vacío y todos sus elementos deben venir ya
// serializados con la forma esperada por item_kind.
func SerializeInnerBatch(itemKind MsgKind, gatewayID GatewayID, clientID ClientID, items [][]byte) (*middleware.Message, error) {
	if len(items) == 0 {
		return nil, errors.New("inner batch must contain at least one item")
	}
	if len(items) > 0xFFFF {
		return nil, errors.New("inner batch exceeds max items (65535)")
	}
	if itemKind != TransactionMessage && itemKind != AccountMessage {
		return nil, errors.New("inner batch only supports TransactionMessage or AccountMessage items")
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
	return SerializeEnvelope(InnerBatch, gatewayID, clientID, 0, buf)
}

// DeserializeInnerBatch parsea el payload binario de un envelope InnerBatch.
// Retorna el item_kind original y la lista de payloads sin decodificar (el
// caller los pasa a DeserializeTransaction / DeserializeAccount según corresponda).
func DeserializeInnerBatch(env *Envelope) (MsgKind, [][]byte, error) {
	if env == nil || env.Kind != InnerBatch {
		return 0, nil, ErrUnexpectedKind
	}
	p := env.Payload
	if len(p) < 3 {
		return 0, nil, ErrMalformedEnvelope
	}
	itemKind := MsgKind(p[0])
	if itemKind != TransactionMessage && itemKind != AccountMessage {
		return 0, nil, ErrMalformedEnvelope
	}
	count := binary.LittleEndian.Uint16(p[1:3])
	items := make([][]byte, 0, count)
	off := 3
	for i := 0; i < int(count); i++ {
		if off+4 > len(p) {
			return 0, nil, ErrMalformedEnvelope
		}
		itemLen := binary.LittleEndian.Uint32(p[off : off+4])
		off += 4
		if off+int(itemLen) > len(p) {
			return 0, nil, ErrMalformedEnvelope
		}
		items = append(items, p[off:off+int(itemLen)])
		off += int(itemLen)
	}
	if off != len(p) {
		return 0, nil, ErrMalformedEnvelope
	}
	return itemKind, items, nil
}

func SerializeFinalQueryResult(gatewayID GatewayID, clientID ClientID, queryID uint8, status string) (*middleware.Message, error) {
	body, err := json.Marshal(Envelope{
		GatewayID: gatewayID,
		ClientID:  clientID,
		Kind:      FinalQueryResult,
		QueryID:   queryID,
		Data:      status,
	})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

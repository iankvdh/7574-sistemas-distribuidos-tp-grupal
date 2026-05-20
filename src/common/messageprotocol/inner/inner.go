package inner

import (
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
	Payload   []byte    `json:"p,omitempty"`
	QueryID   uint8     `json:"q,omitempty"`
	Status    string    `json:"s,omitempty"`
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

func SerializeFinalQueryResult(gatewayID GatewayID, clientID ClientID, queryID uint8, status string) (*middleware.Message, error) {
	body, err := json.Marshal(Envelope{
		GatewayID: gatewayID,
		ClientID:  clientID,
		Kind:      FinalQueryResult,
		QueryID:   queryID,
		Status:    status,
	})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

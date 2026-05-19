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

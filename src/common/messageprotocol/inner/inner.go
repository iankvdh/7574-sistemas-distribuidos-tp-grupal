package inner

import (
	"encoding/json"
	"errors"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
)

type ClientID string

type MsgKind uint8

const (
	AllTransactionsBatch MsgKind = iota + 1
	AllTransactionsEOF
	AllAccountsBatch
	AllAccountsEOF
	FinalQueryResult
	FinalEOF
)

var (
	ErrMalformedEnvelope = errors.New("malformed inner envelope")
	ErrUnexpectedKind    = errors.New("unexpected inner message kind")
)

type Envelope struct {
	ClientID ClientID `json:"c"`
	Kind     MsgKind  `json:"k"`
	Total    uint32   `json:"t,omitempty"`
	Payload  []byte   `json:"p,omitempty"`
	QueryID  uint8    `json:"q,omitempty"`
	Status   string   `json:"s,omitempty"`
}

func SerializeEnvelope(kind MsgKind, clientID ClientID, total uint32, payload []byte) *middleware.Message {
	body, _ := json.Marshal(Envelope{
		ClientID: clientID,
		Kind:     kind,
		Total:    total,
		Payload:  payload,
	})
	return &middleware.Message{Body: string(body)}
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
	if envelope.Kind == 0 {
		return nil, ErrMalformedEnvelope
	}
	return &envelope, nil
}

func SerializeAllTransactionsBatch(clientID ClientID, payload []byte) *middleware.Message {
	return SerializeEnvelope(AllTransactionsBatch, clientID, 0, payload)
}

func SerializeAllTransactionsEOF(clientID ClientID, total uint32) *middleware.Message {
	return SerializeEnvelope(AllTransactionsEOF, clientID, total, nil)
}

func SerializeAllAccountsBatch(clientID ClientID, payload []byte) *middleware.Message {
	return SerializeEnvelope(AllAccountsBatch, clientID, 0, payload)
}

func SerializeAllAccountsEOF(clientID ClientID, total uint32) *middleware.Message {
	return SerializeEnvelope(AllAccountsEOF, clientID, total, nil)
}

func SerializeFinalQueryResult(clientID ClientID, queryID uint8, status string) (*middleware.Message, error) {
	body, err := json.Marshal(Envelope{
		ClientID: clientID,
		Kind:     FinalQueryResult,
		QueryID:  queryID,
		Status:   status,
	})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func SerializeFinalEOF(clientID ClientID) *middleware.Message {
	return SerializeEnvelope(FinalEOF, clientID, 0, nil)
}

package messagehandler

import (
	"fmt"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/account"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

type MessageHandler struct {
	clientID          inner.ClientID
	transactionAmount uint32
	accountAmount     uint32
}

func NewMessageHandler(clientID inner.ClientID) MessageHandler {
	return MessageHandler{clientID: clientID}
}

func (handler *MessageHandler) ClientID() inner.ClientID {
	return handler.clientID
}

func (handler *MessageHandler) SerializeTransactionBatchMessage(batch []transaction.Transaction) (*middleware.Message, error) {
	payload, err := external.SerializeTransactionBatchPayload(batch)
	if err != nil {
		return nil, err
	}
	handler.transactionAmount += uint32(len(batch))
	return inner.SerializeAllTransactionsBatch(handler.clientID, payload), nil
}

func (handler *MessageHandler) SerializeTransactionEOFMessage() *middleware.Message {
	return inner.SerializeAllTransactionsEOF(handler.clientID, handler.transactionAmount)
}

func (handler *MessageHandler) SerializeAccountBatchMessage(batch []account.Account) (*middleware.Message, error) {
	payload, err := external.SerializeAccountBatchPayload(batch)
	if err != nil {
		return nil, err
	}
	handler.accountAmount += uint32(len(batch))
	return inner.SerializeAllAccountsBatch(handler.clientID, payload), nil
}

func (handler *MessageHandler) SerializeAccountEOFMessage() *middleware.Message {
	return inner.SerializeAllAccountsEOF(handler.clientID, handler.accountAmount)
}

type FinalMessage struct {
	ClientID     inner.ClientID
	QueryID      uint8
	Status       string
	EndOfResults bool
}

func DeserializeFinalMessage(message *middleware.Message) (*FinalMessage, error) {
	envelope, err := inner.DeserializeEnvelope(message)
	if err != nil {
		return nil, err
	}

	switch envelope.Kind {
	case inner.FinalQueryResult:
		return &FinalMessage{ClientID: envelope.ClientID, QueryID: envelope.QueryID, Status: envelope.Status}, nil
	case inner.FinalEOF:
		return &FinalMessage{ClientID: envelope.ClientID, EndOfResults: true}, nil
	default:
		return nil, fmt.Errorf("%w: %d", inner.ErrUnexpectedKind, envelope.Kind)
	}
}

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
	gatewayID         inner.GatewayID
	clientID          inner.ClientID
	transactionAmount uint32
	accountAmount     uint32
}

func NewMessageHandler(gatewayID inner.GatewayID, clientID inner.ClientID) MessageHandler {
	return MessageHandler{
		gatewayID: gatewayID,
		clientID:  clientID,
	}
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
	return inner.SerializeAllTransactionsBatch(handler.gatewayID, handler.clientID, payload)
}

func (handler *MessageHandler) SerializeTransactionEOFMessage() (*middleware.Message, error) {
	return inner.SerializeAllTransactionsEOF(handler.gatewayID, handler.clientID, handler.transactionAmount)
}

func (handler *MessageHandler) SerializeAccountBatchMessage(batch []account.Account) (*middleware.Message, error) {
	payload, err := external.SerializeAccountBatchPayload(batch)
	if err != nil {
		return nil, err
	}
	handler.accountAmount += uint32(len(batch))
	return inner.SerializeAllAccountsBatch(handler.gatewayID, handler.clientID, payload)
}

func (handler *MessageHandler) SerializeAccountEOFMessage() (*middleware.Message, error) {
	return inner.SerializeAllAccountsEOF(handler.gatewayID, handler.clientID, handler.accountAmount)
}

type FinalMessage struct {
	GatewayID inner.GatewayID
	ClientID  inner.ClientID
	QueryID   uint8
	Status    string
}

func DeserializeFinalMessage(message *middleware.Message) (*FinalMessage, error) {
	envelope, err := inner.DeserializeEnvelope(message)
	if err != nil {
		return nil, err
	}

	switch envelope.Kind {
	case inner.FinalQueryResult:
		return &FinalMessage{
			GatewayID: envelope.GatewayID,
			ClientID:  envelope.ClientID,
			QueryID:   envelope.QueryID,
			Status:    envelope.Status,
		}, nil
	default:
		return nil, fmt.Errorf("%w: %d", inner.ErrUnexpectedKind, envelope.Kind)
	}
}

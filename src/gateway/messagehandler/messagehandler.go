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

// SerializeTransactionBatch produces a single InnerBatch envelope carrying all
// transactions in the TCP batch. One Publish instead of N drastically cuts
// AMQP framing overhead through the pipeline.
func (handler *MessageHandler) SerializeTransactionBatch(batch []transaction.Transaction) (*middleware.Message, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	items := make([][]byte, 0, len(batch))
	for i := range batch {
		payload, err := external.SerializeTransaction(&batch[i])
		if err != nil {
			return nil, err
		}
		items = append(items, payload)
	}
	msg, err := inner.SerializeInnerBatch(inner.TransactionMessage, handler.gatewayID, handler.clientID, items)
	if err != nil {
		return nil, err
	}
	handler.transactionAmount += uint32(len(batch))
	return msg, nil
}

func (handler *MessageHandler) SerializeTransactionEOFMessage() (*middleware.Message, error) {
	return inner.SerializeAllTransactionsEOF(handler.gatewayID, handler.clientID, handler.transactionAmount)
}

// SerializeAccountBatch produces a single InnerBatch envelope carrying all
// accounts in the TCP batch.
func (handler *MessageHandler) SerializeAccountBatch(batch []account.Account) (*middleware.Message, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	items := make([][]byte, 0, len(batch))
	for i := range batch {
		payload, err := external.SerializeAccount(&batch[i])
		if err != nil {
			return nil, err
		}
		items = append(items, payload)
	}
	msg, err := inner.SerializeInnerBatch(inner.AccountMessage, handler.gatewayID, handler.clientID, items)
	if err != nil {
		return nil, err
	}
	handler.accountAmount += uint32(len(batch))
	return msg, nil
}

func (handler *MessageHandler) SerializeAccountEOFMessage() (*middleware.Message, error) {
	return inner.SerializeAllAccountsEOF(handler.gatewayID, handler.clientID, handler.accountAmount)
}

type FinalMessage struct {
	GatewayID inner.GatewayID
	ClientID  inner.ClientID
	QueryID   uint8
	Data      string
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
			Data:      envelope.Data,
		}, nil
	default:
		return nil, fmt.Errorf("%w: %d", inner.ErrUnexpectedKind, envelope.Kind)
	}
}

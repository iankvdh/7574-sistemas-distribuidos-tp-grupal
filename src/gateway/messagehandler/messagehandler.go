package messagehandler

import (
	"fmt"
	"hash/fnv"
	"strconv"

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
	outSeqID          uint64
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

func (handler *MessageHandler) nextSeqID() uint64 {
	handler.outSeqID++
	return handler.outSeqID
}

func (handler *MessageHandler) outHeader() inner.Header {
	return inner.Header{
		GatewayID:       handler.gatewayID,
		ClientID:        handler.clientID,
		SeqID:           handler.nextSeqID(),
		SenderStageType: inner.StageGateway,
		SenderReplicaID: 0,
	}
}

func TxShardForBatch(batch []transaction.Transaction, nShards int) (string, error) {
	h := fnv.New32a()
	for i := range batch {
		payload, err := external.SerializeTransaction(&batch[i])
		if err != nil {
			return "", err
		}
		_, _ = h.Write(payload)
	}
	return strconv.Itoa(int(h.Sum32()) % nShards), nil
}

func AcctShardForBatch(batch []account.Account, nShards int) (string, error) {
	h := fnv.New32a()
	for i := range batch {
		payload, err := external.SerializeAccount(&batch[i])
		if err != nil {
			return "", err
		}
		_, _ = h.Write(payload)
	}
	return strconv.Itoa(int(h.Sum32()) % nShards), nil
}

// SerializeTransactionBatch produces a single Batch message carrying all
// transactions in the TCP batch. One Publish instead of N drastically cuts
// AMQP framing overhead through the pipeline.
func (handler *MessageHandler) SerializeTransactionBatch(batch []transaction.Transaction) (*middleware.Message, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	items := make([]inner.BatchItem, 0, len(batch))
	for i := range batch {
		payload, err := external.SerializeTransaction(&batch[i])
		if err != nil {
			return nil, err
		}
		items = append(items, inner.BatchItem{QueryID: 0, Payload: payload})
	}
	msg, err := serialize(&inner.BatchMessage{
		Header:   handler.outHeader(),
		ItemKind: inner.TransactionMessage,
		Items:    items,
	})
	if err != nil {
		return nil, err
	}
	handler.transactionAmount += uint32(len(batch))
	return msg, nil
}

func (handler *MessageHandler) SerializeTransactionEOFMessage() (*middleware.Message, error) {
	return serialize(&inner.EOFMessage{
		Header: handler.outHeader(),
		Total:  handler.transactionAmount,
	})
}

// SerializeAccountBatch produces a single Batch message carrying all accounts in
// the TCP batch.
func (handler *MessageHandler) SerializeAccountBatch(batch []account.Account) (*middleware.Message, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	items := make([]inner.BatchItem, 0, len(batch))
	for i := range batch {
		payload, err := external.SerializeAccount(&batch[i])
		if err != nil {
			return nil, err
		}
		items = append(items, inner.BatchItem{QueryID: 0, Payload: payload})
	}
	msg, err := serialize(&inner.BatchMessage{
		Header:   handler.outHeader(),
		ItemKind: inner.AccountMessage,
		Items:    items,
	})
	if err != nil {
		return nil, err
	}
	handler.accountAmount += uint32(len(batch))
	return msg, nil
}

func (handler *MessageHandler) SerializeAccountEOFMessage() (*middleware.Message, error) {
	return serialize(&inner.EOFMessage{
		Header: handler.outHeader(),
		Total:  handler.accountAmount,
	})
}

// serialize wraps the bytes of an InternalMessage in a middleware.Message.
func serialize(msg inner.InternalMessage) (*middleware.Message, error) {
	raw, err := msg.Serialize()
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(raw)}, nil
}

type FinalBatch struct {
	GatewayID       inner.GatewayID
	ClientID        inner.ClientID
	SeqID           uint64
	SenderStageType uint8
	SenderReplicaID uint16
	Items           []inner.QueryResultItem
}

func DeserializeFinalBatch(message *middleware.Message) (*FinalBatch, error) {
	msg, err := inner.NewFromSerializedData([]byte(message.Body))
	if err != nil {
		return nil, err
	}

	batch, ok := msg.(*inner.BatchMessage)
	if !ok || batch.ItemKind != inner.ResultRow {
		return nil, fmt.Errorf("%w: expected ResultRow batch", inner.ErrUnexpectedKind)
	}

	items := make([]inner.QueryResultItem, 0, len(batch.Items))
	for _, item := range batch.Items {
		items = append(items, inner.QueryResultItem{QueryID: item.QueryID, Data: string(item.Payload)})
	}
	return &FinalBatch{
		GatewayID:       batch.GatewayID,
		ClientID:        batch.ClientID,
		SeqID:           batch.SeqID,
		SenderStageType: batch.SenderStageType,
		SenderReplicaID: batch.SenderReplicaID,
		Items:           items,
	}, nil
}

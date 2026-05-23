package gateway

import (
	"errors"
	"fmt"
	"net"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/gateway/clientregistry"
)

type resultBatchBuilder struct {
	state               *clientregistry.ClientState
	maxExternalBatchBytes int

	pendingDeliveries []clientregistry.ResultDelivery
	pendingItems      []external.ResultBatchItem
	currentBatchBytes int
	eofCountInBatch   int
}

func newResultBatchBuilder(state *clientregistry.ClientState, maxExternalBatchBytes int) *resultBatchBuilder {
	return &resultBatchBuilder{
		state:               state,
		maxExternalBatchBytes: maxExternalBatchBytes,
		pendingDeliveries:   make([]clientregistry.ResultDelivery, 0, 16),
		pendingItems:        make([]external.ResultBatchItem, 0, 16),
		currentBatchBytes:   external.ResultBatchHeaderBytes(),
	}
}

func (b *resultBatchBuilder) reset() {
	b.pendingDeliveries = b.pendingDeliveries[:0]
	b.pendingItems = b.pendingItems[:0]
	b.currentBatchBytes = external.ResultBatchHeaderBytes()
	b.eofCountInBatch = 0
}

func (b *resultBatchBuilder) nackPending() {
	for _, pending := range b.pendingDeliveries {
		pending.Nack()
	}
	b.reset()
}

func (b *resultBatchBuilder) append(delivery clientregistry.ResultDelivery) error {
	item := external.ResultBatchItem{
		QueryID: delivery.QueryID,
		Data:    delivery.Data,
	}
	itemBytes, err := external.ResultBatchItemSize(item)
	if err != nil {
		return err
	}
	if itemBytes+external.ResultBatchHeaderBytes() > b.maxExternalBatchBytes {
		return fmt.Errorf("result row exceeds MAX_EXTERNAL_BATCH_BYTES: row=%d max=%d", itemBytes+external.ResultBatchHeaderBytes(), b.maxExternalBatchBytes)
	}

	if len(b.pendingItems) > 0 && b.currentBatchBytes+itemBytes > b.maxExternalBatchBytes {
		if _, err := b.flush(); err != nil {
			return err
		}
	}

	b.pendingDeliveries = append(b.pendingDeliveries, delivery)
	b.pendingItems = append(b.pendingItems, item)
	b.currentBatchBytes += itemBytes
	if delivery.Data == queryResultEOF {
		b.eofCountInBatch++
	}
	return nil
}

func (b *resultBatchBuilder) flush() (int, error) {
	if len(b.pendingItems) == 0 {
		return 0, nil
	}

	eofs := b.eofCountInBatch

	err := b.state.WriteWithLock(func(conn net.Conn) error {
		return external.WriteResultBatch(conn, b.pendingItems)
	})
	if err != nil {
		b.nackPending()
		return 0, err
	}

	if !b.state.WaitForResultBatchAck() {
		b.nackPending()
		return 0, errors.New("client session closed while waiting ResultBatchAck")
	}

	for _, pending := range b.pendingDeliveries {
		pending.Ack()
	}
	b.reset()
	return eofs, nil
}

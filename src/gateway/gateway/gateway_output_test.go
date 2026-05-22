package gateway

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/gateway/clientregistry"
)

func TestHandleClientResultOutputAcksAfterResultBatchAck(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	state := clientregistry.NewClientState(clientConn)
	gateway := &Gateway{resultBatchMaxBytes: 256}
	gateway.running.Store(true)

	var ackCount atomic.Int32
	var nackCount atomic.Int32

	gateway.waitingGroup.Add(1)
	go gateway.handleClientResultOutput(state, inner.ClientID("client-1"))

	state.EnqueueResult(clientregistry.ResultDelivery{
		QueryID: 1,
		Data:  "row-1",
		Ack:     func() { ackCount.Add(1) },
		Nack:    func() { nackCount.Add(1) },
	})
	state.EnqueueResult(clientregistry.ResultDelivery{
		QueryID: 1,
		Data:  "EOF",
		Ack:     func() { ackCount.Add(1) },
		Nack:    func() { nackCount.Add(1) },
	})

	msgType, err := external.ReadMsgType(serverConn)
	if err != nil {
		t.Fatalf("ReadMsgType failed: %v", err)
	}
	if msgType != external.ResultBatch {
		t.Fatalf("unexpected message type: got=%d want=%d", msgType, external.ResultBatch)
	}
	items, err := external.ReadResultBatch(serverConn)
	if err != nil {
		t.Fatalf("ReadResultBatch failed: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("unexpected batch size: got=%d want=2", len(items))
	}
	if ackCount.Load() != 0 {
		t.Fatalf("acks should not be emitted before ResultBatchAck, got=%d", ackCount.Load())
	}

	if !state.NotifyResultBatchAck() {
		t.Fatal("NotifyResultBatchAck returned false")
	}

	waitForCondition(t, func() bool { return ackCount.Load() == 2 })
	if nackCount.Load() != 0 {
		t.Fatalf("unexpected nacks: %d", nackCount.Load())
	}

	state.Close()
	gateway.waitingGroup.Wait()
}

func TestHandleClientResultOutputNacksWhenClientClosesBeforeAck(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	state := clientregistry.NewClientState(clientConn)
	gateway := &Gateway{resultBatchMaxBytes: 256}
	gateway.running.Store(true)

	var ackCount atomic.Int32
	var nackCount atomic.Int32

	gateway.waitingGroup.Add(1)
	go gateway.handleClientResultOutput(state, inner.ClientID("client-2"))

	state.EnqueueResult(clientregistry.ResultDelivery{
		QueryID: 2,
		Data:  "row-2",
		Ack:     func() { ackCount.Add(1) },
		Nack:    func() { nackCount.Add(1) },
	})

	msgType, err := external.ReadMsgType(serverConn)
	if err != nil {
		t.Fatalf("ReadMsgType failed: %v", err)
	}
	if msgType != external.ResultBatch {
		t.Fatalf("unexpected message type: got=%d want=%d", msgType, external.ResultBatch)
	}
	if _, err := external.ReadResultBatch(serverConn); err != nil {
		t.Fatalf("ReadResultBatch failed: %v", err)
	}

	state.Close()

	waitForCondition(t, func() bool { return nackCount.Load() == 1 })
	if ackCount.Load() != 0 {
		t.Fatalf("unexpected acks: %d", ackCount.Load())
	}

	gateway.waitingGroup.Wait()
}

func TestHandleClientResultOutputNacksOversizedRow(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	state := clientregistry.NewClientState(clientConn)
	gateway := &Gateway{resultBatchMaxBytes: external.ResultBatchHeaderBytes() + 1}
	gateway.running.Store(true)

	var ackCount atomic.Int32
	var nackCount atomic.Int32

	gateway.waitingGroup.Add(1)
	go gateway.handleClientResultOutput(state, inner.ClientID("client-3"))

	state.EnqueueResult(clientregistry.ResultDelivery{
		QueryID: 3,
		Data:  "",
		Ack:     func() { ackCount.Add(1) },
		Nack:    func() { nackCount.Add(1) },
	})

	waitForCondition(t, func() bool { return nackCount.Load() == 1 })
	if ackCount.Load() != 0 {
		t.Fatalf("unexpected acks: %d", ackCount.Load())
	}

	state.Close()
	gateway.waitingGroup.Wait()
}

func waitForCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

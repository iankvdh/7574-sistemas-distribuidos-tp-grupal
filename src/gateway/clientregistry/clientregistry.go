package clientregistry

import (
	"context"
	"net"
	"sync"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

const (
	defaultResultQueueSize = 1024
)

type ResultDelivery struct {
	QueryID uint8
	Data    string
	Ack     func()
	Nack    func()
}

type senderKey struct {
	StageType uint8
	ReplicaID uint16
}

type ClientState struct {
	Conn net.Conn

	writeMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	resultQueue   chan ResultDelivery
	resultAckCh   chan struct{}
	lastRecvSeqID map[senderKey]uint64
}

func NewClientState(parentCtx context.Context, conn net.Conn) *ClientState {
	ctx, cancel := context.WithCancel(parentCtx)
	return &ClientState{
		Conn:          conn,
		ctx:           ctx,
		cancel:        cancel,
		resultQueue:   make(chan ResultDelivery, defaultResultQueueSize),
		resultAckCh:   make(chan struct{}, 1),
		lastRecvSeqID: make(map[senderKey]uint64),
	}
}

func (state *ClientState) WriteWithLock(action func(conn net.Conn) error) error {
	state.writeMu.Lock()
	defer state.writeMu.Unlock()
	return action(state.Conn)
}

func (state *ClientState) EnqueueResult(delivery ResultDelivery) bool {
	select {
	case <-state.ctx.Done():
		return false
	case state.resultQueue <- delivery:
		return true
	}
}

func (state *ClientState) DequeueResult() (ResultDelivery, bool) {
	select {
	case delivery := <-state.resultQueue:
		return delivery, true
	default:
	}

	select {
	case delivery := <-state.resultQueue:
		return delivery, true
	case <-state.ctx.Done():
		return ResultDelivery{}, false
	}
}

func (state *ClientState) HasPendingResults() bool {
	return len(state.resultQueue) > 0
}

func (state *ClientState) NotifyResultBatchAck() bool {
	select {
	case <-state.ctx.Done():
		return false
	default:
	}

	select {
	case state.resultAckCh <- struct{}{}:
		return true
	default:
		// No pending batch wait at the moment. Drop duplicated/early ACKs.
		return true
	}
}

func (state *ClientState) WaitForResultBatchAck() bool {
	select {
	case <-state.ctx.Done():
		return false
	case <-state.resultAckCh:
		return true
	}
}

func (state *ClientState) Close() {
	state.cancel()
	_ = state.Conn.Close()
}

func (state *ClientState) IsDuplicate(stageType uint8, replicaID uint16, seqID uint64) bool {
	if seqID == 0 {
		return false
	}
	return seqID <= state.lastRecvSeqID[senderKey{stageType, replicaID}]
}

func (state *ClientState) MarkReceived(stageType uint8, replicaID uint16, seqID uint64) {
	k := senderKey{stageType, replicaID}
	if seqID > state.lastRecvSeqID[k] {
		state.lastRecvSeqID[k] = seqID
	}
}

type ClientRegistry struct {
	mu      sync.Mutex
	clients map[inner.ClientID]*ClientState
}

func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{
		clients: make(map[inner.ClientID]*ClientState),
	}
}

func (registry *ClientRegistry) Add(parentCtx context.Context, clientID inner.ClientID, conn net.Conn) *ClientState {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	state := NewClientState(parentCtx, conn)
	registry.clients[clientID] = state
	return state
}

func (registry *ClientRegistry) Get(clientID inner.ClientID) (*ClientState, bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	state, ok := registry.clients[clientID]
	return state, ok
}

func (registry *ClientRegistry) Remove(clientID inner.ClientID) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	delete(registry.clients, clientID)
}

func (registry *ClientRegistry) RemoveAndClose(clientID inner.ClientID) {
	registry.mu.Lock()
	state, ok := registry.clients[clientID]
	if ok {
		delete(registry.clients, clientID)
	}
	registry.mu.Unlock()

	if ok {
		state.Close()
	}
}

func (registry *ClientRegistry) CloseAll() {
	registry.mu.Lock()
	clients := make([]*ClientState, 0, len(registry.clients))
	for id, state := range registry.clients {
		clients = append(clients, state)
		delete(registry.clients, id)
	}
	registry.mu.Unlock()

	for _, state := range clients {
		state.Close()
	}
}

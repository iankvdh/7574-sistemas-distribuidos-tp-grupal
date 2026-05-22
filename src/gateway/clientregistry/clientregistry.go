package clientregistry

import (
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

type ClientState struct {
	Conn net.Conn

	writeMu sync.Mutex

	resultQueue chan ResultDelivery
	resultAckCh chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
}

func NewClientState(conn net.Conn) *ClientState {
	return &ClientState{
		Conn:        conn,
		resultQueue: make(chan ResultDelivery, defaultResultQueueSize),
		resultAckCh: make(chan struct{}, 1),
		closed:      make(chan struct{}),
	}
}

func (state *ClientState) WriteWithLock(action func(conn net.Conn) error) error {
	state.writeMu.Lock()
	defer state.writeMu.Unlock()
	return action(state.Conn)
}

func (state *ClientState) EnqueueResult(delivery ResultDelivery) bool {
	select {
	case <-state.closed:
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
	case <-state.closed:
		return ResultDelivery{}, false
	}
}

func (state *ClientState) TryDequeueResult() (ResultDelivery, bool) {
	select {
	case delivery := <-state.resultQueue:
		return delivery, true
	default:
	}

	select {
	case <-state.closed:
		return ResultDelivery{}, false
	default:
		return ResultDelivery{}, false
	}
}

func (state *ClientState) NotifyResultBatchAck() bool {
	select {
	case <-state.closed:
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
	case <-state.closed:
		return false
	case <-state.resultAckCh:
		return true
	}
}

func (state *ClientState) Closed() <-chan struct{} {
	return state.closed
}

func (state *ClientState) Close() {
	state.closeOnce.Do(func() {
		close(state.closed)
		_ = state.Conn.Close()
	})
}

type ClientRegistry struct {
	mu      sync.Mutex
	clients map[inner.ClientID]*ClientState
}

func (registry *ClientRegistry) Add(clientID inner.ClientID, conn net.Conn) *ClientState {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if registry.clients == nil {
		registry.clients = make(map[inner.ClientID]*ClientState)
	}
	state := NewClientState(conn)
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

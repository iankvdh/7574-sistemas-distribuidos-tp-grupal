package clientregistry

import (
	"net"
	"sync"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

type ClientState struct {
	Conn    net.Conn
	writeMu sync.Mutex
}

func (state *ClientState) WriteWithLock(action func(conn net.Conn) error) error {
	state.writeMu.Lock()
	defer state.writeMu.Unlock()
	return action(state.Conn)
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
	state := &ClientState{Conn: conn}
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
		_ = state.Conn.Close()
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
		_ = state.Conn.Close()
	}
}

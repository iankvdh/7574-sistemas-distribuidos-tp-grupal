package session

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/account"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/serialanalyzer/queries"
)

const EOFStatus = "EOF"

type QueryOutput struct {
	QueryID uint8
	Status  string
}

type clientState struct {
	transactions []transaction.Transaction
	accounts     []account.Account
	txEOF        bool
	accEOF       bool
	emittedEOF   map[uint8]bool
	completed    bool
}

type sessionKey struct {
	GatewayID inner.GatewayID
	ClientID  inner.ClientID
}

type Processor struct {
	sessions map[sessionKey]*clientState
}

func NewProcessor() *Processor {
	return &Processor{
		sessions: make(map[sessionKey]*clientState),
	}
}

func (processor *Processor) AddTransactions(gatewayID inner.GatewayID, clientID inner.ClientID, batch []transaction.Transaction) []QueryOutput {
	state := processor.getOrCreateState(gatewayID, clientID)
	if state.completed || state.txEOF {
		return nil
	}
	state.transactions = append(state.transactions, batch...)

	if state.emittedEOF[queries.Query1ID] {
		return nil
	}
	return emitQueryRows(queries.Query1ID, queries.Query1Rows(batch))
}

func (processor *Processor) AddAccounts(gatewayID inner.GatewayID, clientID inner.ClientID, batch []account.Account) []QueryOutput {
	state := processor.getOrCreateState(gatewayID, clientID)
	if state.completed || state.accEOF {
		return nil
	}
	state.accounts = append(state.accounts, batch...)
	return nil
}

func (processor *Processor) MarkTransactionsEOF(gatewayID inner.GatewayID, clientID inner.ClientID, _ uint32) []QueryOutput {
	key := sessionKey{GatewayID: gatewayID, ClientID: clientID}
	state, exists := processor.sessions[key]
	if !exists || state.completed {
		return nil
	}
	state.txEOF = true
	outputs := processor.emitReadyQueries(state)
	if state.completed {
		delete(processor.sessions, key)
	}
	return outputs
}

func (processor *Processor) MarkAccountsEOF(gatewayID inner.GatewayID, clientID inner.ClientID, _ uint32) []QueryOutput {
	key := sessionKey{GatewayID: gatewayID, ClientID: clientID}
	state, exists := processor.sessions[key]
	if !exists || state.completed {
		return nil
	}
	state.accEOF = true
	outputs := processor.emitReadyQueries(state)
	if state.completed {
		delete(processor.sessions, key)
	}
	return outputs
}

func (processor *Processor) getOrCreateState(gatewayID inner.GatewayID, clientID inner.ClientID) *clientState {
	key := sessionKey{
		GatewayID: gatewayID,
		ClientID:  clientID,
	}

	state, exists := processor.sessions[key]
	if !exists {
		state = &clientState{
			emittedEOF: make(map[uint8]bool),
		}
		processor.sessions[key] = state
	}
	return state
}

func (processor *Processor) emitReadyQueries(state *clientState) []QueryOutput {
	outputs := make([]QueryOutput, 0)

	if state.txEOF {
		outputs = append(outputs, emitQueryEOF(state, queries.Query1ID)...)
		outputs = append(outputs, emitQuery(state, queries.Query3ID, queries.Query3Rows(state.transactions))...)
		outputs = append(outputs, emitQuery(state, queries.Query4ID, queries.Query4Rows(state.transactions))...)
		outputs = append(outputs, emitQuery(state, queries.Query5ID, queries.Query5Rows(state.transactions))...)
	}
	if state.txEOF && state.accEOF {
		outputs = append(outputs, emitQuery(state, queries.Query2ID, queries.Query2Rows(state.transactions, state.accounts))...)
	}

	if state.emittedEOF[queries.Query1ID] &&
		state.emittedEOF[queries.Query2ID] &&
		state.emittedEOF[queries.Query3ID] &&
		state.emittedEOF[queries.Query4ID] &&
		state.emittedEOF[queries.Query5ID] {
		state.transactions = nil
		state.accounts = nil
		state.completed = true
	}

	return outputs
}

func emitQueryRows(queryID uint8, rows []string) []QueryOutput {
	result := make([]QueryOutput, 0, len(rows))
	for _, row := range rows {
		result = append(result, QueryOutput{QueryID: queryID, Status: row})
	}
	return result
}

func emitQuery(state *clientState, queryID uint8, rows []string) []QueryOutput {
	if state.emittedEOF[queryID] {
		return nil
	}

	result := emitQueryRows(queryID, rows)
	result = append(result, QueryOutput{QueryID: queryID, Status: EOFStatus})
	state.emittedEOF[queryID] = true
	return result
}

func emitQueryEOF(state *clientState, queryID uint8) []QueryOutput {
	if state.emittedEOF[queryID] {
		return nil
	}
	result := []QueryOutput{{QueryID: queryID, Status: EOFStatus}}
	state.emittedEOF[queryID] = true
	return result
}

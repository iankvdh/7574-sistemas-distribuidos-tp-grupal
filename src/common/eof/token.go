package eof

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

type TokenPhase uint8

const (
	PhaseCollecting TokenPhase = 0
	PhaseClosing    TokenPhase = 1
)

type Token struct {
	ClientID      inner.ClientID
	InitiatorID   int
	AggMatched    uint64
	AggNotMatched uint64
	Phase         TokenPhase
}

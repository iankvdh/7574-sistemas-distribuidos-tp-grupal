package eof

import (
	"encoding/json"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

type Token struct {
	ClientID      inner.ClientID `json:"c"`
	InitiatorID   int            `json:"i"`
	AggMatched    uint64         `json:"m"`
	AggNotMatched uint64         `json:"n"`
}

func MarshalToken(token *Token) ([]byte, error) {
	return json.Marshal(token)
}

func UnmarshalToken(data []byte) (*Token, error) {
	t := &Token{}
	if err := json.Unmarshal(data, t); err != nil {
		return nil, err
	}
	return t, nil
}

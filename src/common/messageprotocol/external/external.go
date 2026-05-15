// Package external defines the binary protocol between client and gateway.
//
// Wire framing: every message starts with a single byte MsgType. Batch
// messages then carry a uint32 N (record count) followed by N records.
// Control messages (Ack, EndOfAccounts, EndOfTransactions) carry no payload.
//
// All multi-byte integers are BigEndian. Short strings use a uint8 length
// prefix (max 255 bytes).
package external

import (
	"io"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

// MsgType is the single-byte tag that prefixes every message on the wire.
// We reserve 0x00 (a zero byte is ambiguous with padding / clean EOF on TCP)
// and start valid codes at 0x01 via `iota + 1`. Subsequent constants inherit
// that expression with `iota` auto-incrementing one per line, so the wire
// codes end up:
//
//   AccountBatch       = 0x01
//   EndOfAccounts      = 0x02
//   TransactionBatch   = 0x03
//   EndOfTransactions  = 0x04
//   Ack                = 0x05
type MsgType uint8

const (
	AccountBatch MsgType = iota + 1
	EndOfAccounts
	TransactionBatch
	EndOfTransactions
	Ack
)

func writeMsgType(writer io.Writer, msgType MsgType) error {
	return safeio.WriteAll(writer, serializer.SerializeUint8(uint8(msgType)))
}

func ReadMsgType(reader io.Reader) (MsgType, error) {
	buf, err := safeio.ReadAll(reader, serializer.UINT8_SIZE)
	if err != nil {
		return 0, err
	}
	return MsgType(serializer.DeserializeUint8(buf)), nil
}

func WriteAck(writer io.Writer) error {
	return writeMsgType(writer, Ack)
}

func WriteEndOfAccounts(writer io.Writer) error {
	return writeMsgType(writer, EndOfAccounts)
}

func WriteEndOfTransactions(writer io.Writer) error {
	return writeMsgType(writer, EndOfTransactions)
}

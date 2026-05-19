// Package external defines the binary protocol between client and gateway.
//
// Wire framing: every message starts with a single byte MsgType. Batch
// messages then carry a uint32 N (record count) followed by N records.
// Control messages (IngestAck, EndOfAccounts, EndOfTransactions, ResultBatchAck)
// carry no payload.
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
//	ConnectRequest     = 0x01
//	ConnectAck         = 0x02
//	AccountBatch       = 0x03
//	EndOfAccounts      = 0x04
//	TransactionBatch   = 0x05
//	EndOfTransactions  = 0x06
//	IngestAck          = 0x07
//	ResultBatch        = 0x08
//	ResultBatchAck     = 0x09
type MsgType uint8

const (
	ConnectRequest MsgType = iota + 1
	ConnectAck
	AccountBatch
	EndOfAccounts
	TransactionBatch
	EndOfTransactions
	IngestAck
	ResultBatch
	ResultBatchAck
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

func WriteIngestAck(writer io.Writer) error {
	return writeMsgType(writer, IngestAck)
}

func WriteConnectRequest(writer io.Writer) error {
	return writeMsgType(writer, ConnectRequest)
}

func WriteEndOfAccounts(writer io.Writer) error {
	return writeMsgType(writer, EndOfAccounts)
}

func WriteEndOfTransactions(writer io.Writer) error {
	return writeMsgType(writer, EndOfTransactions)
}

func WriteResultBatchAck(writer io.Writer) error {
	return writeMsgType(writer, ResultBatchAck)
}

package external

import (
	"io"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

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

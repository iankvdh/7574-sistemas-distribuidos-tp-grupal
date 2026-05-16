package external

import (
	"io"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

func WriteConnectAck(writer io.Writer, clientID string) error {
	clientIDSerialized, err := serializer.SerializeShortString(clientID)
	if err != nil {
		return err
	}

	msg := serializer.SerializeUint8(uint8(ConnectAck))
	msg = append(msg, clientIDSerialized...)
	return safeio.WriteAll(writer, msg)
}

func ReadConnectAck(reader io.Reader) (string, error) {
	return readShortString(reader)
}

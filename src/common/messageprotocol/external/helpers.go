package external

import (
	"io"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

// readShortString reads a uint8-prefixed string from reader.
// Shared by the Transaction and Account decoders.
func readShortString(reader io.Reader) (string, error) {
	lenBuf, err := safeio.ReadAll(reader, serializer.UINT8_SIZE)
	if err != nil {
		return "", err
	}
	length := uint32(serializer.DeserializeUint8(lenBuf))
	if length == 0 {
		return "", nil
	}
	data, err := safeio.ReadAll(reader, length)
	if err != nil {
		return "", err
	}
	return serializer.DeserializeShortString(data), nil
}

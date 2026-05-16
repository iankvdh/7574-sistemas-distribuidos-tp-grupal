package external

import (
	"io"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

func WriteQueryResult(writer io.Writer, queryID uint8, status string) error {
	statusSerialized, err := serializer.SerializeShortString(status)
	if err != nil {
		return err
	}

	msg := serializer.SerializeUint8(uint8(QueryResult))
	msg = append(msg, serializer.SerializeUint8(queryID)...)
	msg = append(msg, statusSerialized...)
	return safeio.WriteAll(writer, msg)
}

func ReadQueryResult(reader io.Reader) (uint8, string, error) {
	queryIDBuf, err := safeio.ReadAll(reader, serializer.UINT8_SIZE)
	if err != nil {
		return 0, "", err
	}
	status, err := readShortString(reader)
	if err != nil {
		return 0, "", err
	}
	return serializer.DeserializeUint8(queryIDBuf), status, nil
}

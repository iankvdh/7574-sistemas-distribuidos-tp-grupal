package external

import (
	"bytes"
	"io"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

const (
	resultBatchHeaderBytes = 1 + 4 // MsgType + uint32 record count
)

type ResultBatchItem struct {
	QueryID uint8
	Status  string
}

func ResultBatchItemSize(item ResultBatchItem) (int, error) {
	if len(item.Status) > 255 {
		return 0, serializer.ErrStringTooLong
	}
	return 1 + 1 + len(item.Status), nil
}

func SerializeResultBatchPayload(items []ResultBatchItem) ([]byte, error) {
	msg := serializer.SerializeUint32(uint32(len(items)))
	for _, item := range items {
		statusSerialized, err := serializer.SerializeShortString(item.Status)
		if err != nil {
			return nil, err
		}
		msg = append(msg, serializer.SerializeUint8(item.QueryID)...)
		msg = append(msg, statusSerialized...)
	}
	return msg, nil
}

func DeserializeResultBatchPayload(payload []byte) ([]ResultBatchItem, error) {
	return ReadResultBatch(bytes.NewReader(payload))
}

func WriteResultBatch(writer io.Writer, items []ResultBatchItem) error {
	payload, err := SerializeResultBatchPayload(items)
	if err != nil {
		return err
	}

	msg := serializer.SerializeUint8(uint8(ResultBatch))
	msg = append(msg, payload...)
	return safeio.WriteAll(writer, msg)
}

func ReadResultBatch(reader io.Reader) ([]ResultBatchItem, error) {
	nBuf, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	n := serializer.DeserializeUint32(nBuf)
	items := make([]ResultBatchItem, 0, n)
	for i := uint32(0); i < n; i++ {
		queryIDBuf, err := safeio.ReadAll(reader, serializer.UINT8_SIZE)
		if err != nil {
			return nil, err
		}
		status, err := readShortString(reader)
		if err != nil {
			return nil, err
		}
		items = append(items, ResultBatchItem{
			QueryID: serializer.DeserializeUint8(queryIDBuf),
			Status:  status,
		})
	}
	return items, nil
}

func ResultBatchPayloadBytes(items []ResultBatchItem) (int, error) {
	size := resultBatchHeaderBytes
	for _, item := range items {
		itemSize, err := ResultBatchItemSize(item)
		if err != nil {
			return 0, err
		}
		size += itemSize
	}
	return size, nil
}

func ResultBatchHeaderBytes() int {
	return resultBatchHeaderBytes
}

func ResultBatchItemBytes(queryID uint8, status string) (int, error) {
	_ = queryID
	itemSize, err := ResultBatchItemSize(ResultBatchItem{
		QueryID: queryID,
		Status:  status,
	})
	if err != nil {
		return 0, err
	}
	return itemSize, nil
}

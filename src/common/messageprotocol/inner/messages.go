package inner

import (
	"errors"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

type BatchItem struct {
	QueryID uint8
	Payload []byte
}

type BatchMessage struct {
	Header
	ItemKind MsgKind
	Items    []BatchItem
}

func (b *BatchMessage) Type() MessageType { return Batch }

func (b *BatchMessage) Serialize() ([]byte, error) {
	if len(b.Items) > 0xFFFF {
		return nil, errors.New("batch exceeds max items (65535)")
	}
	if !validBatchItemKind(b.ItemKind) {
		return nil, errors.New("batch unsupported item kind")
	}
	buf, err := writeHeader(Batch, b.Header)
	if err != nil {
		return nil, err
	}
	buf = append(buf, byte(b.ItemKind))
	buf = append(buf, serializer.SerializeUint16(uint16(len(b.Items)))...)
	for _, item := range b.Items {
		buf = append(buf, item.QueryID)
		buf = append(buf, serializer.SerializeUint32(uint32(len(item.Payload)))...)
		buf = append(buf, item.Payload...)
	}
	return buf, nil
}

func parseBatch(header Header, body []byte) (*BatchMessage, error) {
	if len(body) < 3 {
		return nil, ErrMalformedEnvelope
	}
	itemKind := MsgKind(body[0])
	if !validBatchItemKind(itemKind) {
		return nil, ErrMalformedEnvelope
	}
	count := serializer.DeserializeUint16(body[1:3])
	items := make([]BatchItem, 0, count)
	offset := 3
	for i := 0; i < int(count); i++ {
		if offset+1+int(serializer.UINT32_SIZE) > len(body) {
			return nil, ErrMalformedEnvelope
		}
		queryID := body[offset]
		offset++
		itemLen := serializer.DeserializeUint32(body[offset : offset+int(serializer.UINT32_SIZE)])
		offset += int(serializer.UINT32_SIZE)
		if offset+int(itemLen) > len(body) {
			return nil, ErrMalformedEnvelope
		}
		items = append(items, BatchItem{QueryID: queryID, Payload: body[offset : offset+int(itemLen)]})
		offset += int(itemLen)
	}
	if offset != len(body) {
		return nil, ErrMalformedEnvelope
	}
	return &BatchMessage{Header: header, ItemKind: itemKind, Items: items}, nil
}

func validBatchItemKind(kind MsgKind) bool {
	switch kind {
	case TransactionMessage, AccountMessage, ShardedTxMessage, SuspiciousPathMessage,
		Query4PairItem, Query1RowItem, Q2PartialMaxItem, Q2BankNameItem, Q2ResultItem,
		Q3PartialAvgItem, Q3AverageItem, Query3RowItem, Q5ConversionsItem,
		Q5PartialCountItem, Query5RowItem, ResultRow:
		return true
	default:
		return false
	}
}

type EOFMessage struct {
	Header
	QueryID uint8
	Total   uint32
}

func (e *EOFMessage) Type() MessageType { return EOF }

func (e *EOFMessage) Serialize() ([]byte, error) {
	buf, err := writeHeader(EOF, e.Header)
	if err != nil {
		return nil, err
	}
	buf = append(buf, e.QueryID)
	buf = append(buf, serializer.SerializeUint32(e.Total)...)
	return buf, nil
}

func parseEOF(header Header, body []byte) (*EOFMessage, error) {
	if len(body) != 1+int(serializer.UINT32_SIZE) {
		return nil, ErrMalformedEnvelope
	}
	return &EOFMessage{
		Header:  header,
		QueryID: body[0],
		Total:   serializer.DeserializeUint32(body[1:5]),
	}, nil
}

type RingTokenMessage struct {
	Header
	InitiatorID   int32
	AggMatched    uint64
	AggNotMatched uint64
	Phase         uint8
}

func (t *RingTokenMessage) Type() MessageType { return RingToken }

func (t *RingTokenMessage) Serialize() ([]byte, error) {
	buf, err := writeHeader(RingToken, t.Header)
	if err != nil {
		return nil, err
	}
	buf = append(buf, serializer.SerializeUint32(uint32(t.InitiatorID))...)
	buf = append(buf, serializer.SerializeUint64(t.AggMatched)...)
	buf = append(buf, serializer.SerializeUint64(t.AggNotMatched)...)
	buf = append(buf, t.Phase)
	return buf, nil
}

func parseRingToken(header Header, body []byte) (*RingTokenMessage, error) {
	const size = int(serializer.UINT32_SIZE) + int(serializer.UINT64_SIZE) + int(serializer.UINT64_SIZE) + 1
	if len(body) != size {
		return nil, ErrMalformedEnvelope
	}
	return &RingTokenMessage{
		Header:        header,
		InitiatorID:   int32(serializer.DeserializeUint32(body[0:4])),
		AggMatched:    serializer.DeserializeUint64(body[4:12]),
		AggNotMatched: serializer.DeserializeUint64(body[12:20]),
		Phase:         body[20],
	}, nil
}

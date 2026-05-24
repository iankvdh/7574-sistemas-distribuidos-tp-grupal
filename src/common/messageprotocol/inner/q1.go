package inner

import (
	"bytes"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

type Query1Row struct {
	SourceBank    uint32
	SourceAccount string
	DestBank      uint32
	DestAccount   string
	Amount        float64
}

func SerializeQuery1Row(r *Query1Row) ([]byte, error) {
	src, err := serializer.SerializeShortString(r.SourceAccount)
	if err != nil {
		return nil, err
	}
	dst, err := serializer.SerializeShortString(r.DestAccount)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 4+len(src)+4+len(dst)+8)
	buf = append(buf, serializer.SerializeUint32(r.SourceBank)...)
	buf = append(buf, src...)
	buf = append(buf, serializer.SerializeUint32(r.DestBank)...)
	buf = append(buf, dst...)
	buf = append(buf, serializer.SerializeFloat64(r.Amount)...)
	return buf, nil
}

func DeserializeQuery1Row(payload []byte) (*Query1Row, error) {
	r := bytes.NewReader(payload)
	srcBank, err := readQ4Uint32(r)
	if err != nil {
		return nil, err
	}
	srcAcc, err := readQ4ShortString(r)
	if err != nil {
		return nil, err
	}
	dstBank, err := readQ4Uint32(r)
	if err != nil {
		return nil, err
	}
	dstAcc, err := readQ4ShortString(r)
	if err != nil {
		return nil, err
	}
	amountBuf, err := safeio.ReadAll(r, serializer.UINT64_SIZE)
	if err != nil {
		return nil, err
	}
	return &Query1Row{
		SourceBank:    srcBank,
		SourceAccount: srcAcc,
		DestBank:      dstBank,
		DestAccount:   dstAcc,
		Amount:        serializer.DeserializeFloat64(amountBuf),
	}, nil
}

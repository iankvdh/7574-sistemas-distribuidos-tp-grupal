package inner

import (
	"bytes"
	"io"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

// ShardedTx representa una transacción ya shardeada por una de sus dos cuentas
// (origen o destino). El Sharder emite dos ShardedTx por cada Transaction que
// procesa, con ShardedBySource=true y false respectivamente.
type ShardedTx struct {
	FromBank        uint32
	FromAccount     string
	ToBank          uint32
	ToAccount       string
	ShardedBySource bool
}

func SerializeShardedTx(tx *ShardedTx) ([]byte, error) {
	fromAccount, err := serializer.SerializeShortString(tx.FromAccount)
	if err != nil {
		return nil, err
	}
	toAccount, err := serializer.SerializeShortString(tx.ToAccount)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 8+len(fromAccount)+len(toAccount)+1)
	buf = append(buf, serializer.SerializeUint32(tx.FromBank)...)
	buf = append(buf, fromAccount...)
	buf = append(buf, serializer.SerializeUint32(tx.ToBank)...)
	buf = append(buf, toAccount...)
	flag := uint8(0)
	if tx.ShardedBySource {
		flag = 1
	}
	buf = append(buf, flag)
	return buf, nil
}

func DeserializeShardedTx(payload []byte) (*ShardedTx, error) {
	r := bytes.NewReader(payload)
	fromBank, err := readQ4Uint32(r)
	if err != nil {
		return nil, err
	}
	fromAccount, err := readQ4ShortString(r)
	if err != nil {
		return nil, err
	}
	toBank, err := readQ4Uint32(r)
	if err != nil {
		return nil, err
	}
	toAccount, err := readQ4ShortString(r)
	if err != nil {
		return nil, err
	}
	flagBuf, err := safeio.ReadAll(r, serializer.UINT8_SIZE)
	if err != nil {
		return nil, err
	}
	return &ShardedTx{
		FromBank:        fromBank,
		FromAccount:     fromAccount,
		ToBank:          toBank,
		ToAccount:       toAccount,
		ShardedBySource: serializer.DeserializeUint8(flagBuf) == 1,
	}, nil
}

// SuspiciousPath representa una terna (cuenta origen, cuenta intermedia,
// cuenta destino) emitida por el Path-Finder al Counter.
type SuspiciousPath struct {
	SourceBank          uint32
	SourceAccount       string
	IntermediateBank    uint32
	IntermediateAccount string
	DestBank            uint32
	DestAccount         string
}

func SerializeSuspiciousPath(p *SuspiciousPath) ([]byte, error) {
	src, err := serializer.SerializeShortString(p.SourceAccount)
	if err != nil {
		return nil, err
	}
	mid, err := serializer.SerializeShortString(p.IntermediateAccount)
	if err != nil {
		return nil, err
	}
	dst, err := serializer.SerializeShortString(p.DestAccount)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 12+len(src)+len(mid)+len(dst))
	buf = append(buf, serializer.SerializeUint32(p.SourceBank)...)
	buf = append(buf, src...)
	buf = append(buf, serializer.SerializeUint32(p.IntermediateBank)...)
	buf = append(buf, mid...)
	buf = append(buf, serializer.SerializeUint32(p.DestBank)...)
	buf = append(buf, dst...)
	return buf, nil
}

func DeserializeSuspiciousPath(payload []byte) (*SuspiciousPath, error) {
	r := bytes.NewReader(payload)
	srcBank, err := readQ4Uint32(r)
	if err != nil {
		return nil, err
	}
	srcAcc, err := readQ4ShortString(r)
	if err != nil {
		return nil, err
	}
	midBank, err := readQ4Uint32(r)
	if err != nil {
		return nil, err
	}
	midAcc, err := readQ4ShortString(r)
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
	return &SuspiciousPath{
		SourceBank:          srcBank,
		SourceAccount:       srcAcc,
		IntermediateBank:    midBank,
		IntermediateAccount: midAcc,
		DestBank:            dstBank,
		DestAccount:         dstAcc,
	}, nil
}

// Query4Pair representa un par (cuenta origen, cuenta destino) que el Counter
// emite cuando un par supera el umbral de intermediarios distintos.
type Query4Pair struct {
	SourceBank    uint32
	SourceAccount string
	DestBank      uint32
	DestAccount   string
}

func SerializeQuery4Pair(p *Query4Pair) ([]byte, error) {
	srcAcc, err := serializer.SerializeShortString(p.SourceAccount)
	if err != nil {
		return nil, err
	}
	dstAcc, err := serializer.SerializeShortString(p.DestAccount)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 8+len(srcAcc)+len(dstAcc))
	buf = append(buf, serializer.SerializeUint32(p.SourceBank)...)
	buf = append(buf, srcAcc...)
	buf = append(buf, serializer.SerializeUint32(p.DestBank)...)
	buf = append(buf, dstAcc...)
	return buf, nil
}

func DeserializeQuery4Pair(payload []byte) (*Query4Pair, error) {
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
	return &Query4Pair{
		SourceBank:    srcBank,
		SourceAccount: srcAcc,
		DestBank:      dstBank,
		DestAccount:   dstAcc,
	}, nil
}

func readQ4Uint32(r io.Reader) (uint32, error) {
	buf, err := safeio.ReadAll(r, serializer.UINT32_SIZE)
	if err != nil {
		return 0, err
	}
	return serializer.DeserializeUint32(buf), nil
}

func readQ4ShortString(r io.Reader) (string, error) {
	lenBuf, err := safeio.ReadAll(r, serializer.UINT8_SIZE)
	if err != nil {
		return "", err
	}
	length := uint32(serializer.DeserializeUint8(lenBuf))
	if length == 0 {
		return "", nil
	}
	data, err := safeio.ReadAll(r, length)
	if err != nil {
		return "", err
	}
	return serializer.DeserializeShortString(data), nil
}

package inner

import (
	"bytes"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

// Q3PartialAvg es un parcial emitido por una réplica de average_local_q3 al
// cierre del ring: suma y count de los montos USD de Period 1 observados
// localmente para un payment_format dado.
type Q3PartialAvg struct {
	PaymentFormat string
	Sum           float64
	Count         uint64
}

func SerializeQ3PartialAvg(p *Q3PartialAvg) ([]byte, error) {
	fmtBuf, err := serializer.SerializeShortString(p.PaymentFormat)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, len(fmtBuf)+8+8)
	buf = append(buf, fmtBuf...)
	buf = append(buf, serializer.SerializeFloat64(p.Sum)...)
	buf = append(buf, serializer.SerializeUint64(p.Count)...)
	return buf, nil
}

func DeserializeQ3PartialAvg(payload []byte) (*Q3PartialAvg, error) {
	r := bytes.NewReader(payload)
	format, err := readQ4ShortString(r)
	if err != nil {
		return nil, err
	}
	sumBuf, err := safeio.ReadAll(r, serializer.UINT64_SIZE)
	if err != nil {
		return nil, err
	}
	countBuf, err := safeio.ReadAll(r, serializer.UINT64_SIZE)
	if err != nil {
		return nil, err
	}
	return &Q3PartialAvg{
		PaymentFormat: format,
		Sum:           serializer.DeserializeFloat64(sumBuf),
		Count:         serializer.DeserializeUint64(countBuf),
	}, nil
}

// Q3Average es el promedio global por payment_format emitido por
// average_global_q3 hacia las réplicas de filter_q3 (broadcast).
type Q3Average struct {
	PaymentFormat string
	Average       float64
}

func SerializeQ3Average(a *Q3Average) ([]byte, error) {
	fmtBuf, err := serializer.SerializeShortString(a.PaymentFormat)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, len(fmtBuf)+8)
	buf = append(buf, fmtBuf...)
	buf = append(buf, serializer.SerializeFloat64(a.Average)...)
	return buf, nil
}

func DeserializeQ3Average(payload []byte) (*Q3Average, error) {
	r := bytes.NewReader(payload)
	format, err := readQ4ShortString(r)
	if err != nil {
		return nil, err
	}
	avgBuf, err := safeio.ReadAll(r, serializer.UINT64_SIZE)
	if err != nil {
		return nil, err
	}
	return &Q3Average{
		PaymentFormat: format,
		Average:       serializer.DeserializeFloat64(avgBuf),
	}, nil
}

// Query3Row es la fila final de Q3 emitida por filter_q3 hacia el final_joiner:
// transacciones de Period 2 USD cuyo monto es < (1/100) del promedio de Period 1
// USD para el mismo payment_format.
type Query3Row struct {
	SourceBank    uint32
	SourceAccount string
	Amount        float64
}

func SerializeQuery3Row(r *Query3Row) ([]byte, error) {
	acc, err := serializer.SerializeShortString(r.SourceAccount)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 4+len(acc)+8)
	buf = append(buf, serializer.SerializeUint32(r.SourceBank)...)
	buf = append(buf, acc...)
	buf = append(buf, serializer.SerializeFloat64(r.Amount)...)
	return buf, nil
}

func DeserializeQuery3Row(payload []byte) (*Query3Row, error) {
	r := bytes.NewReader(payload)
	bank, err := readQ4Uint32(r)
	if err != nil {
		return nil, err
	}
	acc, err := readQ4ShortString(r)
	if err != nil {
		return nil, err
	}
	amountBuf, err := safeio.ReadAll(r, serializer.UINT64_SIZE)
	if err != nil {
		return nil, err
	}
	return &Query3Row{
		SourceBank:    bank,
		SourceAccount: acc,
		Amount:        serializer.DeserializeFloat64(amountBuf),
	}, nil
}

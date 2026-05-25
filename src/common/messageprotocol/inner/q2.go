package inner

import (
	"bytes"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

// Q2PartialMax es un parcial emitido por una réplica de max_q2 al cerrar el
// ring: el monto máximo local observado para un banco origen y la cuenta
// emisora asociada a ese monto.
type Q2PartialMax struct {
	BankID      uint32
	FromAccount string
	MaxAmount   float64
}

func SerializeQ2PartialMax(p *Q2PartialMax) ([]byte, error) {
	acc, err := serializer.SerializeShortString(p.FromAccount)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 4+len(acc)+8)
	buf = append(buf, serializer.SerializeUint32(p.BankID)...)
	buf = append(buf, acc...)
	buf = append(buf, serializer.SerializeFloat64(p.MaxAmount)...)
	return buf, nil
}

func DeserializeQ2PartialMax(payload []byte) (*Q2PartialMax, error) {
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
	return &Q2PartialMax{
		BankID:      bank,
		FromAccount: acc,
		MaxAmount:   serializer.DeserializeFloat64(amountBuf),
	}, nil
}

// Q2BankName es una entrada del catálogo de bancos shardeada por cliente,
// emitida por bank_aggregator hacia aggregator_q2 para el join final de Q2.
type Q2BankName struct {
	BankID   uint32
	BankName string
}

func SerializeQ2BankName(b *Q2BankName) ([]byte, error) {
	name, err := serializer.SerializeShortString(b.BankName)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 4+len(name))
	buf = append(buf, serializer.SerializeUint32(b.BankID)...)
	buf = append(buf, name...)
	return buf, nil
}

func DeserializeQ2BankName(payload []byte) (*Q2BankName, error) {
	r := bytes.NewReader(payload)
	bank, err := readQ4Uint32(r)
	if err != nil {
		return nil, err
	}
	name, err := readQ4ShortString(r)
	if err != nil {
		return nil, err
	}
	return &Q2BankName{BankID: bank, BankName: name}, nil
}

// Q2Result es la fila final de Q2 que aggregator_q2 publica al exchange
// `results`: máximo global por banco con su ID, nombre y cuenta emisora.
type Q2Result struct {
	BankID      uint32
	BankName    string
	FromAccount string
	MaxAmount   float64
}

func SerializeQ2Result(r *Q2Result) ([]byte, error) {
	name, err := serializer.SerializeShortString(r.BankName)
	if err != nil {
		return nil, err
	}
	acc, err := serializer.SerializeShortString(r.FromAccount)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 4+len(name)+len(acc)+8)
	buf = append(buf, serializer.SerializeUint32(r.BankID)...)
	buf = append(buf, name...)
	buf = append(buf, acc...)
	buf = append(buf, serializer.SerializeFloat64(r.MaxAmount)...)
	return buf, nil
}

func DeserializeQ2Result(payload []byte) (*Q2Result, error) {
	r := bytes.NewReader(payload)
	bankID, err := readQ4Uint32(r)
	if err != nil {
		return nil, err
	}
	name, err := readQ4ShortString(r)
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
	return &Q2Result{
		BankID:      bankID,
		BankName:    name,
		FromAccount: acc,
		MaxAmount:   serializer.DeserializeFloat64(amountBuf),
	}, nil
}

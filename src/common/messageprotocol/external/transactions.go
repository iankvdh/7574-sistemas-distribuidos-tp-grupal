package external

import (
	"bytes"
	"io"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

func SerializeTransaction(tx *transaction.Transaction, mask transaction.Projection) ([]byte, error) {
	var fromAccount, toAccount, currency, format []byte
	var err error
	if mask&transaction.FieldFromAccount != 0 {
		if fromAccount, err = serializer.SerializeShortString(tx.FromAccount); err != nil {
			return nil, err
		}
	}
	if mask&transaction.FieldToAccount != 0 {
		if toAccount, err = serializer.SerializeShortString(tx.ToAccount); err != nil {
			return nil, err
		}
	}
	if mask&transaction.FieldPaymentCurrency != 0 {
		if currency, err = serializer.SerializeShortString(tx.PaymentCurrency); err != nil {
			return nil, err
		}
	}
	if mask&transaction.FieldPaymentFormat != 0 {
		if format, err = serializer.SerializeShortString(tx.PaymentFormat); err != nil {
			return nil, err
		}
	}

	buf := make([]byte, 0, 1+24+len(fromAccount)+len(toAccount)+len(currency)+len(format))
	buf = append(buf, byte(mask))
	if mask&transaction.FieldDate != 0 {
		buf = append(buf, serializer.SerializeUint32(tx.Date)...)
	}
	if mask&transaction.FieldFromBank != 0 {
		buf = append(buf, serializer.SerializeUint32(tx.FromBank)...)
	}
	buf = append(buf, fromAccount...)
	if mask&transaction.FieldToBank != 0 {
		buf = append(buf, serializer.SerializeUint32(tx.ToBank)...)
	}
	buf = append(buf, toAccount...)
	if mask&transaction.FieldAmountPaid != 0 {
		buf = append(buf, serializer.SerializeFloat64(tx.AmountPaid)...)
	}
	buf = append(buf, currency...)
	buf = append(buf, format...)
	return buf, nil
}

func DeserializeTransaction(payload []byte) (*transaction.Transaction, transaction.Projection, error) {
	return deserializeTransaction(bytes.NewReader(payload))
}

func deserializeTransaction(reader io.Reader) (*transaction.Transaction, transaction.Projection, error) {
	maskBuf, err := safeio.ReadAll(reader, serializer.UINT8_SIZE)
	if err != nil {
		return nil, 0, err
	}
	mask := transaction.Projection(serializer.DeserializeUint8(maskBuf))

	tx := &transaction.Transaction{}
	if mask&transaction.FieldDate != 0 {
		dateBuf, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
		if err != nil {
			return nil, 0, err
		}
		tx.Date = serializer.DeserializeUint32(dateBuf)
	}
	if mask&transaction.FieldFromBank != 0 {
		fromBankBuf, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
		if err != nil {
			return nil, 0, err
		}
		tx.FromBank = serializer.DeserializeUint32(fromBankBuf)
	}
	if mask&transaction.FieldFromAccount != 0 {
		if tx.FromAccount, err = readShortString(reader); err != nil {
			return nil, 0, err
		}
	}
	if mask&transaction.FieldToBank != 0 {
		toBankBuf, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
		if err != nil {
			return nil, 0, err
		}
		tx.ToBank = serializer.DeserializeUint32(toBankBuf)
	}
	if mask&transaction.FieldToAccount != 0 {
		if tx.ToAccount, err = readShortString(reader); err != nil {
			return nil, 0, err
		}
	}
	if mask&transaction.FieldAmountPaid != 0 {
		amountBuf, err := safeio.ReadAll(reader, serializer.UINT64_SIZE)
		if err != nil {
			return nil, 0, err
		}
		tx.AmountPaid = serializer.DeserializeFloat64(amountBuf)
	}
	if mask&transaction.FieldPaymentCurrency != 0 {
		if tx.PaymentCurrency, err = readShortString(reader); err != nil {
			return nil, 0, err
		}
	}
	if mask&transaction.FieldPaymentFormat != 0 {
		if tx.PaymentFormat, err = readShortString(reader); err != nil {
			return nil, 0, err
		}
	}

	return tx, mask, nil
}

func WriteTransactionBatch(writer io.Writer, txs []transaction.Transaction) error {
	payload, err := SerializeTransactionBatchPayload(txs)
	if err != nil {
		return err
	}
	msg := serializer.SerializeUint8(uint8(TransactionBatch))
	msg = append(msg, payload...)
	return safeio.WriteAll(writer, msg)
}

func SerializeTransactionBatchPayload(txs []transaction.Transaction) ([]byte, error) {
	msg := serializer.SerializeUint32(uint32(len(txs)))
	for i := range txs {
		serialized, err := SerializeTransaction(&txs[i], transaction.AllFields)
		if err != nil {
			return nil, err
		}
		msg = append(msg, serialized...)
	}
	return msg, nil
}

func DeserializeTransactionBatchPayload(payload []byte) ([]transaction.Transaction, error) {
	return ReadTransactionBatch(bytes.NewReader(payload))
}

func ReadTransactionBatch(reader io.Reader) ([]transaction.Transaction, error) {
	nBuf, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	n := serializer.DeserializeUint32(nBuf)
	txs := make([]transaction.Transaction, n)
	for i := uint32(0); i < n; i++ {
		tx, _, err := deserializeTransaction(reader)
		if err != nil {
			return nil, err
		}
		txs[i] = *tx
	}
	return txs, nil
}

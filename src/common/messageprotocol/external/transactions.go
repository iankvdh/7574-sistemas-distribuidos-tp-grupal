package external

import (
	"bytes"
	"io"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

func serializeTransaction(tx *transaction.Transaction) ([]byte, error) {
	fromAccount, err := serializer.SerializeShortString(tx.FromAccount)
	if err != nil {
		return nil, err
	}
	toAccount, err := serializer.SerializeShortString(tx.ToAccount)
	if err != nil {
		return nil, err
	}
	currency, err := serializer.SerializeShortString(tx.PaymentCurrency)
	if err != nil {
		return nil, err
	}
	format, err := serializer.SerializeShortString(tx.PaymentFormat)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 0, 24+len(fromAccount)+len(toAccount)+len(currency)+len(format))
	buf = append(buf, serializer.SerializeUint32(tx.Date)...)
	buf = append(buf, serializer.SerializeUint32(tx.FromBank)...)
	buf = append(buf, fromAccount...)
	buf = append(buf, serializer.SerializeUint32(tx.ToBank)...)
	buf = append(buf, toAccount...)
	buf = append(buf, serializer.SerializeUint64(tx.AmountPaidCents)...)
	buf = append(buf, currency...)
	buf = append(buf, format...)
	return buf, nil
}

func deserializeTransaction(reader io.Reader) (*transaction.Transaction, error) {
	dateBuf, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	fromBankBuf, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	fromAccount, err := readShortString(reader)
	if err != nil {
		return nil, err
	}
	toBankBuf, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	toAccount, err := readShortString(reader)
	if err != nil {
		return nil, err
	}
	amountBuf, err := safeio.ReadAll(reader, serializer.UINT64_SIZE)
	if err != nil {
		return nil, err
	}
	currency, err := readShortString(reader)
	if err != nil {
		return nil, err
	}
	format, err := readShortString(reader)
	if err != nil {
		return nil, err
	}

	return &transaction.Transaction{
		Date:            serializer.DeserializeUint32(dateBuf),
		FromBank:        serializer.DeserializeUint32(fromBankBuf),
		FromAccount:     fromAccount,
		ToBank:          serializer.DeserializeUint32(toBankBuf),
		ToAccount:       toAccount,
		AmountPaidCents: serializer.DeserializeUint64(amountBuf),
		PaymentCurrency: currency,
		PaymentFormat:   format,
	}, nil
}

// WriteTransactionBatch writes [MsgType=TransactionBatch][N uint32][tx1]...[txN]
// in a single Write to keep one TCP segment per batch.
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
		serialized, err := serializeTransaction(&txs[i])
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

// ReadTransactionBatch reads [N uint32][tx1]...[txN] from reader. The MsgType
// byte must have been consumed by ReadMsgType before calling this.
func ReadTransactionBatch(reader io.Reader) ([]transaction.Transaction, error) {
	nBuf, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	n := serializer.DeserializeUint32(nBuf)
	txs := make([]transaction.Transaction, n)
	for i := uint32(0); i < n; i++ {
		tx, err := deserializeTransaction(reader)
		if err != nil {
			return nil, err
		}
		txs[i] = *tx
	}
	return txs, nil
}

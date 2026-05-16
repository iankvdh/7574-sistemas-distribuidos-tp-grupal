package external

import (
	"bytes"
	"io"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/account"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

func serializeAccount(acc *account.Account) ([]byte, error) {
	bankName, err := serializer.SerializeShortString(acc.BankName)
	if err != nil {
		return nil, err
	}
	accountNumber, err := serializer.SerializeShortString(acc.AccountNumber)
	if err != nil {
		return nil, err
	}
	entityID, err := serializer.SerializeShortString(acc.EntityID)
	if err != nil {
		return nil, err
	}
	entityName, err := serializer.SerializeShortString(acc.EntityName)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 0, 4+len(bankName)+len(accountNumber)+len(entityID)+len(entityName))
	buf = append(buf, bankName...)
	buf = append(buf, serializer.SerializeUint32(acc.BankID)...)
	buf = append(buf, accountNumber...)
	buf = append(buf, entityID...)
	buf = append(buf, entityName...)
	return buf, nil
}

func deserializeAccount(reader io.Reader) (*account.Account, error) {
	bankName, err := readShortString(reader)
	if err != nil {
		return nil, err
	}
	bankIDBuf, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	accountNumber, err := readShortString(reader)
	if err != nil {
		return nil, err
	}
	entityID, err := readShortString(reader)
	if err != nil {
		return nil, err
	}
	entityName, err := readShortString(reader)
	if err != nil {
		return nil, err
	}

	return &account.Account{
		BankName:      bankName,
		BankID:        serializer.DeserializeUint32(bankIDBuf),
		AccountNumber: accountNumber,
		EntityID:      entityID,
		EntityName:    entityName,
	}, nil
}

// WriteAccountBatch writes [MsgType=AccountBatch][N uint32][acc1]...[accN]
// in a single Write to keep one TCP segment per batch.
func WriteAccountBatch(writer io.Writer, accs []account.Account) error {
	payload, err := SerializeAccountBatchPayload(accs)
	if err != nil {
		return err
	}
	msg := serializer.SerializeUint8(uint8(AccountBatch))
	msg = append(msg, payload...)
	return safeio.WriteAll(writer, msg)
}

func SerializeAccountBatchPayload(accs []account.Account) ([]byte, error) {
	msg := serializer.SerializeUint32(uint32(len(accs)))
	for i := range accs {
		serialized, err := serializeAccount(&accs[i])
		if err != nil {
			return nil, err
		}
		msg = append(msg, serialized...)
	}
	return msg, nil
}

func DeserializeAccountBatchPayload(payload []byte) ([]account.Account, error) {
	return ReadAccountBatch(bytes.NewReader(payload))
}

// ReadAccountBatch reads [N uint32][acc1]...[accN]. The MsgType byte must have
// been consumed by ReadMsgType before calling this.
func ReadAccountBatch(reader io.Reader) ([]account.Account, error) {
	nBuf, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	n := serializer.DeserializeUint32(nBuf)
	accs := make([]account.Account, n)
	for i := uint32(0); i < n; i++ {
		acc, err := deserializeAccount(reader)
		if err != nil {
			return nil, err
		}
		accs[i] = *acc
	}
	return accs, nil
}

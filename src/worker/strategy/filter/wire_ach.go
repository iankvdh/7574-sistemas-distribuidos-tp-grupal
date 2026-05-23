package filter

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

func IsWireOrACH(tx transaction.Transaction) bool {
	return tx.PaymentFormat == "Wire" || tx.PaymentFormat == "ACH"
}

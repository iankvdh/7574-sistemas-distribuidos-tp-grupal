package filter

import (
	"os"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

const defaultUSDCurrencyName = "US Dollar"

func IsUSDPredicate() Predicate {
	name := os.Getenv("CURRENCY_USD")
	if name == "" {
		name = defaultUSDCurrencyName
	}
	return func(tx transaction.Transaction) bool {
		return tx.PaymentCurrency == name
	}
}

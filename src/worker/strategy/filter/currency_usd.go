package filter

import (
	"os"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const defaultUSDCurrencyName = "US Dollar"

// All three currency_usd variants share the same predicate; they differ only in
// their input/output topology and so are registered as distinct strategy names.
func init() {
	makeIsUSD := func() func(transaction.Transaction) bool {
		name := os.Getenv("CURRENCY_USD")
		if name == "" {
			name = defaultUSDCurrencyName
		}
		return func(tx transaction.Transaction) bool {
			return tx.PaymentCurrency == name
		}
	}
	strategy.Register("filter_currency_usd_p1", func() (strategy.Strategy, error) {
		return New("filter_currency_usd_p1", makeIsUSD()), nil
	})
	strategy.Register("filter_currency_usd_p2", func() (strategy.Strategy, error) {
		return New("filter_currency_usd_p2", makeIsUSD()), nil
	})
	strategy.Register("filter_currency_usd_other_periods", func() (strategy.Strategy, error) {
		return New("filter_currency_usd_other_periods", makeIsUSD()), nil
	})
}

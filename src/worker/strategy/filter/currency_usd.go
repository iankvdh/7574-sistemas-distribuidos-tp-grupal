package filter

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

// All three currency_usd variants share the same predicate; they differ only in
// their input/output topology and so are registered as distinct strategy names.
func init() {
	isUSD := func(tx transaction.Transaction) bool {
		return tx.PaymentCurrency == "US Dollar"
	}
	strategy.Register("filter_currency_usd_p1", func() (strategy.Strategy, error) {
		return New("filter_currency_usd_p1", isUSD), nil
	})
	strategy.Register("filter_currency_usd_p2", func() (strategy.Strategy, error) {
		return New("filter_currency_usd_p2", isUSD), nil
	})
	strategy.Register("filter_currency_usd_other_periods", func() (strategy.Strategy, error) {
		return New("filter_currency_usd_other_periods", isUSD), nil
	})
}

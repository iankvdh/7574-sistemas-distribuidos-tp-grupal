package filter

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

func init() {
	strategy.Register("filter_amount_lt_50", func() (strategy.Strategy, error) {
		return New("filter_amount_lt_50", func(tx transaction.Transaction) bool {
			return tx.AmountPaid < 50.0
		}), nil
	})
}

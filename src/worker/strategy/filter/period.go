package filter

import (
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/dates"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

func init() {
	strategy.Register("filter_period1", func() (strategy.Strategy, error) {
		return New("filter_period1", func(tx transaction.Transaction) bool {
			return dates.InPeriod1(tx.Date)
		}), nil
	})
	strategy.Register("filter_period2", func() (strategy.Strategy, error) {
		return New("filter_period2", func(tx transaction.Transaction) bool {
			return dates.InPeriod2(tx.Date)
		}), nil
	})
}

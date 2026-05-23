package filter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

const defaultAmountThreshold = 50.0

func init() {
	strategy.Register("filter_amount_lt_50", func() (strategy.Strategy, error) {
		threshold, err := parseAmountThreshold()
		if err != nil {
			return nil, err
		}
		return New("filter_amount_lt_50", func(tx transaction.Transaction) bool {
			return tx.AmountPaid < threshold
		}), nil
	})
}

func parseAmountThreshold() (float64, error) {
	raw := os.Getenv("AMOUNT_THRESHOLD_USD")
	if raw == "" {
		return defaultAmountThreshold, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("invalid AMOUNT_THRESHOLD_USD=%q — expected a positive number", raw)
	}
	return v, nil
}

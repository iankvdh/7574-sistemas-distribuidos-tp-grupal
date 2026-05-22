package filter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/dates"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
)

func init() {
	strategy.Register("filter_period1", func() (strategy.Strategy, error) {
		start, err := parsePeriodDate("PERIOD1_START", dates.Period1Start)
		if err != nil {
			return nil, err
		}
		end, err := parsePeriodDate("PERIOD1_END", dates.Period1End)
		if err != nil {
			return nil, err
		}
		return New("filter_period1", func(tx transaction.Transaction) bool {
			return dates.InRange(tx.Date, start, end)
		}), nil
	})

	strategy.Register("filter_period2", func() (strategy.Strategy, error) {
		start, err := parsePeriodDate("PERIOD2_START", dates.Period2Start)
		if err != nil {
			return nil, err
		}
		end, err := parsePeriodDate("PERIOD2_END", dates.Period2End)
		if err != nil {
			return nil, err
		}
		return New("filter_period2", func(tx transaction.Transaction) bool {
			return dates.InRange(tx.Date, start, end)
		}), nil
	})
}

// parsePeriodDate reads an env var as a YYYYMMDD date (uint32).
// Returns defaultVal if the variable is unset.
func parsePeriodDate(envName string, defaultVal uint32) (uint32, error) {
	raw := os.Getenv(envName)
	if raw == "" {
		return defaultVal, nil
	}
	v, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %q — expected YYYYMMDD integer", envName, raw)
	}
	return uint32(v), nil
}

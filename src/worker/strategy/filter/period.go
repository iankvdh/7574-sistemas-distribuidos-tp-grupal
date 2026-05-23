package filter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/dates"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

func Period1Defaults() (uint32, uint32) { return dates.Period1Start, dates.Period1End }
func Period2Defaults() (uint32, uint32) { return dates.Period2Start, dates.Period2End }

func ParsePeriodRange(startEnv, endEnv string, defStart, defEnd uint32) (uint32, uint32, error) {
	start, err := parsePeriodDate(startEnv, defStart)
	if err != nil {
		return 0, 0, err
	}
	end, err := parsePeriodDate(endEnv, defEnd)
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

func InDateRange(start, end uint32) Predicate {
	return func(tx transaction.Transaction) bool {
		return dates.InRange(tx.Date, start, end)
	}
}

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

package builder

import (
	"fmt"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy"
	aggregator_q2 "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/aggregator_q2"
	bank_aggregator "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/bank_aggregator"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/counter"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/drain"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/filter"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/finaljoiner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/joiner"
	max_q2 "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/max_q2"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/noop"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/pathfinder"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/sharder"
	sharder_q1 "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/sharder_q1"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/suspicious_filter"
)

func Build(name string) (strategy.Strategy, error) {
	switch name {
	case "drain":
		return drain.New(), nil
	case "noop":
		return noop.New(), nil
	case "joiner_usd":
		return joiner.NewEOFJoiner("joiner_usd"), nil
	case "filter_wire_ach":
		return filter.New("filter_wire_ach", filter.IsWireOrACH).
			WithMatchProjection(filter.WithoutPaymentFormat).
			WithNoMatchProjection(filter.WithoutPaymentFormat), nil
	case "filter_amount_lt_50":
		threshold, err := filter.ParseAmountThreshold()
		if err != nil {
			return nil, err
		}
		return filter.New("filter_amount_lt_50", filter.AmountLessThan(threshold)), nil
	case "filter_period1":
		defStart, defEnd := filter.Period1Defaults()
		start, end, err := filter.ParsePeriodRange("PERIOD1_START", "PERIOD1_END", defStart, defEnd)
		if err != nil {
			return nil, err
		}
		return filter.New("filter_period1", filter.InDateRange(start, end)).
			WithMatchProjection(filter.WithoutDate).
			WithNoMatchProjection(filter.WithoutPaymentFormat), nil
	case "filter_period2":
		defStart, defEnd := filter.Period2Defaults()
		start, end, err := filter.ParsePeriodRange("PERIOD2_START", "PERIOD2_END", defStart, defEnd)
		if err != nil {
			return nil, err
		}
		return filter.New("filter_period2", filter.InDateRange(start, end)).
			WithMatchProjection(filter.WithoutDate).
			WithNoMatchProjection(filter.WithoutDate), nil
	case "filter_currency_usd_p1", "filter_currency_usd_p2", "filter_currency_usd_other_periods":
		return filter.New(name, filter.IsUSDPredicate()).
			WithMatchProjection(filter.WithoutPaymentCurrency), nil
	case "sharder_q4":
		return sharder.New(), nil
	case "sharder_q1":
		return sharder_q1.New(), nil
	case "final_joiner":
		return finaljoiner.New(), nil
	case "suspicious_account_filter":
		return suspicious_filter.New(), nil
	case "path_finder_q4":
		return pathfinder.New(), nil
	case "counter_q4":
		return counter.New(), nil
	case "drain_q4":
		return drain.NewQ4(), nil
	case "max_q2":
		return max_q2.New(), nil
	case "bank_aggregator":
		return bank_aggregator.New(), nil
	case "aggregator_q2":
		return aggregator_q2.New(), nil
	default:
		return nil, fmt.Errorf("unknown STRATEGY: %s", name)
	}
}

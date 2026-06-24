package inner

import "fmt"

const (
	StageGateway                 uint8 = 1
	StageFilterCurrencyUsdP1     uint8 = 2
	StageFilterCurrencyUsdP2     uint8 = 3
	StageFilterCurrencyUsdOther  uint8 = 4
	StageFilterWireACH           uint8 = 6
	StageFilterPeriod1           uint8 = 7
	StageFilterPeriod2           uint8 = 8
	StageFilterQ3                uint8 = 9
	StageFilterAmountLt50        uint8 = 10
	StageSharderQ4               uint8 = 11
	StageJoinerUSD               uint8 = 12
	StageMaxQ2                   uint8 = 13
	StageBankAggregator          uint8 = 14
	StageSumQ3                   uint8 = 15
	StageAggregatorQ2            uint8 = 16
	StageAggregatorQ3            uint8 = 17
	StageAggregatorQ5            uint8 = 18
	StageMicroTransactionCounter uint8 = 19
	StageSuspiciousFilter        uint8 = 20
	StagePathFinder              uint8 = 21
	StageCounterQ4               uint8 = 22
	StageFinalJoiner             uint8 = 23
	StageFetcher                 uint8 = 24
)

var stageTypeByName = map[string]uint8{
	"Gateway":                 StageGateway,
	"FilterCurrencyUsdP1":     StageFilterCurrencyUsdP1,
	"FilterCurrencyUsdP2":     StageFilterCurrencyUsdP2,
	"FilterCurrencyUsdOther":  StageFilterCurrencyUsdOther,
	"FilterWireACH":           StageFilterWireACH,
	"FilterPeriod1":           StageFilterPeriod1,
	"FilterPeriod2":           StageFilterPeriod2,
	"FilterQ3":                StageFilterQ3,
	"FilterAmountLt50":        StageFilterAmountLt50,
	"SharderQ4":               StageSharderQ4,
	"JoinerUSD":               StageJoinerUSD,
	"MaxQ2":                   StageMaxQ2,
	"BankAggregator":          StageBankAggregator,
	"SumQ3":                   StageSumQ3,
	"AggregatorQ2":            StageAggregatorQ2,
	"AggregatorQ3":            StageAggregatorQ3,
	"AggregatorQ5":            StageAggregatorQ5,
	"MicroTransactionCounter": StageMicroTransactionCounter,
	"SuspiciousFilter":        StageSuspiciousFilter,
	"PathFinder":              StagePathFinder,
	"CounterQ4":               StageCounterQ4,
	"FinalJoiner":             StageFinalJoiner,
	"Fetcher":                 StageFetcher,
}

func StageTypeFromName(name string) (uint8, error) {
	v, ok := stageTypeByName[name]
	if !ok {
		return 0, fmt.Errorf("unknown STAGE_TYPE %q", name)
	}
	return v, nil
}

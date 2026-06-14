package inner

const (
	StageGateway                 uint8 = 1
	StageFilterCurrencyUsdP1     uint8 = 2
	StageFilterCurrencyUsdP2     uint8 = 3
	StageFilterCurrencyUsdOther  uint8 = 4
	StageFilterAmountLt50        uint8 = 5
	StageFilterWireACH           uint8 = 6
	StageFilterPeriod1           uint8 = 7
	StageFilterPeriod2           uint8 = 8
	StageFilterQ3                uint8 = 9
	StageSharderQ1               uint8 = 10
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

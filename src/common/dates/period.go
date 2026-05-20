package dates

// Period boundaries used by the filtering pipeline. Dates are encoded as YYYYMMDD uint32.
const (
	Period1Start uint32 = 20220901
	Period1End   uint32 = 20220905
	Period2Start uint32 = 20220906
	Period2End   uint32 = 20220915
)

// InPeriod1 reports whether the given date lies in [2022-09-01, 2022-09-05].
func InPeriod1(date uint32) bool {
	return date >= Period1Start && date <= Period1End
}

// InPeriod2 reports whether the given date lies in [2022-09-06, 2022-09-15].
func InPeriod2(date uint32) bool {
	return date >= Period2Start && date <= Period2End
}

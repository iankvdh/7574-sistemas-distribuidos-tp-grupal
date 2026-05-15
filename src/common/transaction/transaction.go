package transaction

type Transaction struct {
	Date            uint32 // 4 bytes - YYYYMMDD (e.g. 20220801)
	FromBank        uint32 // 4 bytes
	FromAccount     string // 1 + N bytes - uint8 length prefix
	ToBank          uint32 // 4 bytes
	ToAccount       string // 1 + N bytes - uint8 length prefix
	AmountPaidCents uint64 // 8 bytes - TODO: when parsing, convert from string with decimal point to uint64 in cents (e.g. "123.45" -> 12345)
	PaymentCurrency string // 1 + N bytes - uint8 length prefix
	PaymentFormat   string // 1 + N bytes - uint8 length prefix
}

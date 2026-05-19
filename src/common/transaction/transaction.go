package transaction

type Transaction struct {
	Date            uint32  // 4 bytes - YYYYMMDD (e.g. 20220801)
	FromBank        uint32  // 4 bytes
	FromAccount     string  // 1 + N bytes - uint8 length prefix
	ToBank          uint32  // 4 bytes
	ToAccount       string  // 1 + N bytes - uint8 length prefix
	AmountPaid      float64 // 8 bytes - IEEE-754 float64
	PaymentCurrency string  // 1 + N bytes - uint8 length prefix
	PaymentFormat   string  // 1 + N bytes - uint8 length prefix
}

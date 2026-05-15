package transaction

// Timestamp,From Bank,Account,To Bank,Account,Amount Received,
// Receiving Currency,Amount Paid,Payment Currency,Payment Format,Is Laundering

type Transaction struct {
	Date            uint32 //  ->  2024-06-01 -> 20240601
	FromBank        uint32
	FromAccount     string
	ToBank          uint32
	ToAccount       string
	AmountPaidCents uint64 //1.00  --> 100 (guardamos en centavos para evitar problemas de precisión con float)
	PaymentCurrency string
	PaymentFormat   string
}

// func (fruitItem FruitItem) Sum(other FruitItem) FruitItem {
// 	return FruitItem{Fruit: fruitItem.Fruit, Amount: fruitItem.Amount + other.Amount}
// }

// func (fruitItem FruitItem) Less(other FruitItem) bool {
// 	return fruitItem.Amount < other.Amount
// }

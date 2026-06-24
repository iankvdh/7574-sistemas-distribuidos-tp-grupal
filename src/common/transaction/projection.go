package transaction

type Projection uint8

const (
	FieldDate            Projection = 1 << 7
	FieldFromBank        Projection = 1 << 6
	FieldFromAccount     Projection = 1 << 5
	FieldToBank          Projection = 1 << 4
	FieldToAccount       Projection = 1 << 3
	FieldAmountPaid      Projection = 1 << 2
	FieldPaymentCurrency Projection = 1 << 1
	FieldPaymentFormat   Projection = 1 << 0

	AllFields Projection = 0xFF
)

func (p Projection) Apply(tx *Transaction) {
	if p == AllFields {
		return
	}
	if p&FieldDate == 0 {
		tx.Date = 0
	}
	if p&FieldFromBank == 0 {
		tx.FromBank = 0
	}
	if p&FieldFromAccount == 0 {
		tx.FromAccount = ""
	}
	if p&FieldToBank == 0 {
		tx.ToBank = 0
	}
	if p&FieldToAccount == 0 {
		tx.ToAccount = ""
	}
	if p&FieldAmountPaid == 0 {
		tx.AmountPaid = 0
	}
	if p&FieldPaymentCurrency == 0 {
		tx.PaymentCurrency = ""
	}
	if p&FieldPaymentFormat == 0 {
		tx.PaymentFormat = ""
	}
}

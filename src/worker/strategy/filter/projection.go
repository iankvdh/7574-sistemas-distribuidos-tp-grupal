package filter

import "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"

// Projection is a bitmask that controls which Transaction fields are forwarded.
// A bit set to 1 means the field is kept; 0 means it is zeroed before re-serialization.
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

var (
	WithoutDate                   = AllFields &^ FieldDate
	WithoutPaymentFormat          = AllFields &^ FieldPaymentFormat
	WithoutPaymentCurrencyAndDate = AllFields &^ FieldPaymentCurrency &^ FieldDate
)

// Apply zeroes out the fields that are not set in the projection mask.
func (p Projection) Apply(tx *transaction.Transaction) {
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

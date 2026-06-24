package filter

import "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"

type Projection = transaction.Projection

var (
	WithoutDate                   = transaction.AllFields &^ transaction.FieldDate
	WithoutPaymentFormat          = transaction.AllFields &^ transaction.FieldPaymentFormat
	WithoutPaymentCurrencyAndDate = transaction.AllFields &^ transaction.FieldPaymentCurrency &^ transaction.FieldDate
)

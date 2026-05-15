package account

type Account struct {
	BankName      string // 1 + N     uint8 length prefix
	BankID        uint32 // 4 bytes 
	AccountNumber string // 1 + N     uint8 length prefix
	EntityID      string // 1 + N     uint8 length prefix
	EntityName    string // 1 + N     uint8 length prefix
}

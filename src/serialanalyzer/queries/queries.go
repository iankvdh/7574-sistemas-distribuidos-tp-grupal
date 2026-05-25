package queries

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/account"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/transaction"
)

const (
	Query1ID uint8 = 1
	Query2ID uint8 = 2
	Query3ID uint8 = 3
	Query4ID uint8 = 4
	Query5ID uint8 = 5
)

const (
	usdCurrency = "US Dollar"
)

const (
	septStart uint32 = 20220901
	sept5End  uint32 = 20220905
	sept6     uint32 = 20220906
	sept15End uint32 = 20220915
)

var toUSD = map[string]float64{
	"Australian Dollar": 0.72,
	"Bitcoin":           78.33,
	"Brazil Real":       0.20,
	"Canadian Dollar":   0.73,
	"Euro":              1.17,
	"Mexican Peso":      0.06,
	"Ruble":             0.01,
	"Rupee":             0.01,
	"Saudi Riyal":       0.27,
	"Shekel":            0.33,
	"Swiss Franc":       1.27,
	"UK Pound":          1.35,
	"US Dollar":         1.0,
	"Yen":               0.006,
	"Yuan":              0.15,
}

func Query1Rows(transactions []transaction.Transaction) []string {
	rows := make([]string, 0)
	for _, tx := range transactions {
		if tx.PaymentCurrency != usdCurrency || tx.AmountPaid >= 50.0 {
			continue
		}

		row := []string{
			strconv.FormatUint(uint64(tx.FromBank), 10),
			tx.FromAccount,
			strconv.FormatUint(uint64(tx.ToBank), 10),
			tx.ToAccount,
			formatAmount(tx.AmountPaid),
		}
		rows = append(rows, strings.Join(row, ","))
	}
	return rows
}

func Query2Rows(transactions []transaction.Transaction, accounts []account.Account) []string {
	type bankMax struct {
		Account string
		Amount  float64
	}

	bankNameByID := make(map[uint32]string)
	for _, acc := range accounts {
		if acc.BankName == "" {
			continue
		}
		if _, exists := bankNameByID[acc.BankID]; !exists {
			bankNameByID[acc.BankID] = acc.BankName
		}
	}

	maxByBank := make(map[uint32]bankMax)
	for _, tx := range transactions {
		if tx.PaymentCurrency != usdCurrency {
			continue
		}

		current, exists := maxByBank[tx.FromBank]
		if !exists || tx.AmountPaid > current.Amount {
			maxByBank[tx.FromBank] = bankMax{
				Account: tx.FromAccount,
				Amount:  tx.AmountPaid,
			}
		}
	}

	banks := make([]uint32, 0, len(maxByBank))
	for bankID := range maxByBank {
		banks = append(banks, bankID)
	}
	sort.Slice(banks, func(i, j int) bool { return banks[i] < banks[j] })

	rows := make([]string, 0, len(banks))
	for _, bankID := range banks {
		entry := maxByBank[bankID]
		bankName, ok := bankNameByID[bankID]
		if !ok {
			bankName = fmt.Sprintf("UNKNOWN_%d", bankID)
		}
		row := []string{
			strconv.FormatUint(uint64(bankID), 10),
			entry.Account,
			bankName,
			formatAmount(entry.Amount),
		}
		rows = append(rows, strings.Join(row, ","))
	}

	return rows
}

func Query3Rows(transactions []transaction.Transaction) []string {
	type agg struct {
		Sum   float64
		Count uint64
	}

	avgByPaymentFormat := make(map[string]agg)
	for _, tx := range transactions {
		if tx.PaymentCurrency != usdCurrency || tx.Date < septStart || tx.Date > sept5End {
			continue
		}
		current := avgByPaymentFormat[tx.PaymentFormat]
		current.Sum += tx.AmountPaid
		current.Count++
		avgByPaymentFormat[tx.PaymentFormat] = current
	}

	rows := make([]string, 0)
	for _, tx := range transactions {
		if tx.PaymentCurrency != usdCurrency || tx.Date < sept6 || tx.Date > sept15End {
			continue
		}

		avg, exists := avgByPaymentFormat[tx.PaymentFormat]
		if !exists || avg.Count == 0 {
			continue
		}

		avgAmount := avg.Sum / float64(avg.Count)
		threshold := avgAmount * 0.01
		if tx.AmountPaid >= threshold {
			continue
		}

		row := []string{
			strconv.FormatUint(uint64(tx.FromBank), 10),
			tx.FromAccount,
			tx.PaymentFormat,
			formatAmount(tx.AmountPaid),
		}
		rows = append(rows, strings.Join(row, ","))
	}

	return rows
}

type accountKey struct {
	Bank    uint32
	Account string
}

func Query4Rows(transactions []transaction.Transaction) []string {
	senderToDestinations := make(map[accountKey]map[accountKey]struct{})

	for _, tx := range transactions {
		if tx.PaymentCurrency != usdCurrency || tx.Date < septStart || tx.Date > sept5End {
			continue
		}
		source := accountKey{Bank: tx.FromBank, Account: tx.FromAccount}
		destination := accountKey{Bank: tx.ToBank, Account: tx.ToAccount}

		if _, ok := senderToDestinations[source]; !ok {
			senderToDestinations[source] = make(map[accountKey]struct{})
		}
		senderToDestinations[source][destination] = struct{}{}
	}

	candidateSources := make(map[accountKey]struct{})
	for source, destinations := range senderToDestinations {
		if len(destinations) >= 5 {
			candidateSources[source] = struct{}{}
		}
	}

	type pathKey struct {
		source accountKey
		dest   accountKey
	}
	intermediaries := make(map[pathKey]map[accountKey]struct{})
	for source := range candidateSources {
		for intermediate := range senderToDestinations[source] {
			nextHops, hasOutgoing := senderToDestinations[intermediate]
			if !hasOutgoing {
				continue
			}
			for dest := range nextHops {
				if source == dest {
					continue
				}
				key := pathKey{source, dest}
				if intermediaries[key] == nil {
					intermediaries[key] = make(map[accountKey]struct{})
				}
				intermediaries[key][intermediate] = struct{}{}
			}
		}
	}

	participatingAccounts := make(map[accountKey]struct{})
	for key, mids := range intermediaries {
		if len(mids) >= 5 {
			participatingAccounts[key.source] = struct{}{}
			participatingAccounts[key.dest] = struct{}{}
		}
	}

	accounts := make([]accountKey, 0, len(participatingAccounts))
	for key := range participatingAccounts {
		accounts = append(accounts, key)
	}

	sort.Slice(accounts, func(i, j int) bool {
		if accounts[i].Bank == accounts[j].Bank {
			return accounts[i].Account < accounts[j].Account
		}
		return accounts[i].Bank < accounts[j].Bank
	})

	rows := make([]string, 0, len(accounts))
	for _, acc := range accounts {
		row := []string{
			strconv.FormatUint(uint64(acc.Bank), 10),
			acc.Account,
		}
		rows = append(rows, strings.Join(row, ","))
	}

	return rows
}

func Query5Rows(transactions []transaction.Transaction) []string {
	count := 0
	for _, tx := range transactions {
		if tx.Date < septStart || tx.Date > sept5End {
			continue
		}
		if tx.PaymentFormat != "Wire" && tx.PaymentFormat != "ACH" {
			continue
		}

		rate, ok := toUSD[tx.PaymentCurrency]
		if !ok {
			continue
		}

		amountUSD := tx.AmountPaid * rate
		if amountUSD < 1.0 {
			count++
		}
	}

	return []string{strconv.Itoa(count)}
}

func formatAmount(amount float64) string {
	return fmt.Sprintf("%.2f", amount)
}

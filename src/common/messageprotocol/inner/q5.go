package inner

import (
	"bytes"
	"errors"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

const MAX_CURRENCIES_PER_DATE = 65535

type Q5Conversions struct {
	Rates map[uint32]map[string]float64
}

type Q5PartialCount struct {
	Count uint64
}

type Query5Row struct {
	Count uint64
}

func SerializeQ5Conversions(c *Q5Conversions) ([]byte, error) {
	buf := make([]byte, 0, 4)
	buf = append(buf, serializer.SerializeUint32(uint32(len(c.Rates)))...)
	for date, currencies := range c.Rates {
		if len(currencies) > MAX_CURRENCIES_PER_DATE {
			return nil, errors.New("Q5Conversions: too many currencies for a single date")
		}
		buf = append(buf, serializer.SerializeUint32(date)...)
		buf = append(buf, serializer.SerializeUint16(uint16(len(currencies)))...)
		for currency, rate := range currencies {
			currencyBuf, err := serializer.SerializeShortString(currency)
			if err != nil {
				return nil, err
			}
			buf = append(buf, currencyBuf...)
			buf = append(buf, serializer.SerializeFloat64(rate)...)
		}
	}
	return buf, nil
}

func DeserializeQ5Conversions(payload []byte) (*Q5Conversions, error) {
	r := bytes.NewReader(payload)
	nDatesBuf, err := safeio.ReadAll(r, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	nDates := serializer.DeserializeUint32(nDatesBuf)
	rates := make(map[uint32]map[string]float64, nDates)
	for i := uint32(0); i < nDates; i++ {
		dateBuf, err := safeio.ReadAll(r, serializer.UINT32_SIZE)
		if err != nil {
			return nil, err
		}
		date := serializer.DeserializeUint32(dateBuf)
		nCurrenciesBuf, err := safeio.ReadAll(r, serializer.UINT16_SIZE)
		if err != nil {
			return nil, err
		}
		nCurrencies := serializer.DeserializeUint16(nCurrenciesBuf)
		currencies := make(map[string]float64, nCurrencies)
		for j := uint16(0); j < nCurrencies; j++ {
			currency, err := readQ4ShortString(r)
			if err != nil {
				return nil, err
			}
			rateBuf, err := safeio.ReadAll(r, serializer.UINT64_SIZE)
			if err != nil {
				return nil, err
			}
			currencies[currency] = serializer.DeserializeFloat64(rateBuf)
		}
		rates[date] = currencies
	}
	return &Q5Conversions{Rates: rates}, nil
}

func SerializeQ5PartialCount(p *Q5PartialCount) ([]byte, error) {
	return serializer.SerializeUint64(p.Count), nil
}

func DeserializeQ5PartialCount(payload []byte) (*Q5PartialCount, error) {
	r := bytes.NewReader(payload)
	buf, err := safeio.ReadAll(r, serializer.UINT64_SIZE)
	if err != nil {
		return nil, err
	}
	return &Q5PartialCount{Count: serializer.DeserializeUint64(buf)}, nil
}

func SerializeQuery5Row(r *Query5Row) ([]byte, error) {
	return serializer.SerializeUint64(r.Count), nil
}

func DeserializeQuery5Row(payload []byte) (*Query5Row, error) {
	r := bytes.NewReader(payload)
	buf, err := safeio.ReadAll(r, serializer.UINT64_SIZE)
	if err != nil {
		return nil, err
	}
	return &Query5Row{Count: serializer.DeserializeUint64(buf)}, nil
}

package inner

import (
	"bytes"
	"encoding/json"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

type Q5Conversions struct {
	Rates map[uint32]map[string]float64 `json:"r"`
}

type Q5PartialCount struct {
	Count uint64
}

type Query5Row struct {
	Count uint64
}

func SerializeQ5Conversions(c *Q5Conversions) ([]byte, error) {
	return json.Marshal(c)
}

func DeserializeQ5Conversions(payload []byte) (*Q5Conversions, error) {
	var c Q5Conversions
	if err := json.Unmarshal(payload, &c); err != nil {
		return nil, err
	}
	if c.Rates == nil {
		c.Rates = map[uint32]map[string]float64{}
	}
	return &c, nil
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

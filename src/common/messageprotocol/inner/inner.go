package inner

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/serializer"
)

type ClientID string
type GatewayID int

// MessageType discriminates the on-the-wire message type of the internal
// protocol. It is the first byte of every serialized message and is read by
// NewFromSerializedData to build the corresponding concrete type.
type MessageType uint8

const (
	// Batch groups N items of the same item_kind (see MsgKind). It unifies the
	// internal pipeline batches and the final batch towards the gateway.
	Batch MessageType = iota + 1
	// EOF signals the end of a stream between stages (transactions, accounts or
	// internal EOF; the concrete stream is distinguished by the input it arrives on).
	EOF
	// RingToken carries the ring token between replicas of a strategy.
	RingToken
)

// MsgKind identifies the type of item that travels inside a Batch.
type MsgKind uint8

const (
	// TransactionMessage: payload = external.SerializeTransaction(tx).
	TransactionMessage MsgKind = iota + 1
	// AccountMessage: payload = external.SerializeAccount(acc).
	AccountMessage
	// ShardedTxMessage: transaction sharded by one of its accounts (Q4).
	ShardedTxMessage
	// SuspiciousPathMessage: triple (origin, intermediate, destination) (Q4).
	SuspiciousPathMessage
	// Query4PairItem: pair (origin, destination) suspected of scatter (Q4).
	Query4PairItem
	// Query1RowItem: projected USD <50 transaction (Q1).
	Query1RowItem
	// Q2PartialMaxItem: partial (BankID, FromAccount, MaxAmount) (Q2).
	Q2PartialMaxItem
	// Q2BankNameItem: (BankID, BankName) from the accounts catalog (Q2).
	Q2BankNameItem
	// Q2ResultItem: global maximum per bank with name and account (Q2).
	Q2ResultItem
	// Q3PartialAvgItem: partial (PaymentFormat, Sum, Count) (Q3).
	Q3PartialAvgItem
	// Q3AverageItem: global average (PaymentFormat, Average) (Q3).
	Q3AverageItem
	// Query3RowItem: final row (SourceBank, SourceAccount, Amount) (Q3).
	Query3RowItem
	// Q5ConversionsItem: map of exchange rates date→currency→rate (Q5).
	Q5ConversionsItem
	// Q5PartialCountItem: partial count of microtransactions (Q5).
	Q5PartialCountItem
	// Query5RowItem: final total count per client (Q5).
	Query5RowItem
	// ResultRow: final row towards the gateway (CSV or the "EOF" sentinel). It is
	// the item_kind of the Batch that travels through the final_queues.
	ResultRow
)

var (
	ErrMalformedEnvelope = errors.New("malformed inner message")
	ErrUnexpectedKind    = errors.New("unexpected inner message kind")
)

// Header is the common header of every internal protocol message. On-the-wire:
//
//	[ MessageType u8 ][ GatewayID u16 BE ][ ClientID 16 bytes (UUID) ]
type Header struct {
	GatewayID GatewayID
	ClientID  ClientID
}

// headerSize is the fixed size of the serialized header: type (1) + gatewayID (2)
// + clientID (16 bytes UUID).
const headerSize = 1 + int(serializer.UINT16_SIZE) + 16

// Envelope is the IN-MEMORY representation of an item that the runtime delivers
// to the strategies. It is NOT the wire format: the runtime builds it when
// exploding a Batch (one Envelope per item) or when receiving an EOF.
type Envelope struct {
	GatewayID  GatewayID
	ClientID   ClientID
	Kind       MsgKind
	Total      uint32
	QueryID    uint8
	Payload    []byte
	InputIndex int
}

// QueryResultItem is a result row: a CSV or the "EOF" sentinel.
type QueryResultItem struct {
	QueryID uint8
	Data    string
}

// InternalMessage is every serializable message of the internal protocol.
// Serialization is binary and big-endian. Deserialization is done with the
// NewFromSerializedData factory (Go interfaces do not allow constructors).
type InternalMessage interface {
	Serialize() ([]byte, error)
	Type() MessageType
}

// NewFromSerializedData reads the MessageType (first byte) and the common
// header, and delegates to the parser of the corresponding concrete type.
func NewFromSerializedData(data []byte) (InternalMessage, error) {
	msgType, header, body, err := parseHeader(data)
	if err != nil {
		return nil, err
	}
	switch msgType {
	case Batch:
		return parseBatch(header, body)
	case EOF:
		return parseEOF(header, body)
	case RingToken:
		return parseRingToken(header, body)
	default:
		return nil, ErrUnexpectedKind
	}
}

// writeHeader serializes the header (including the MessageType byte) into a new
// buffer ready for the message body to be concatenated onto it.
func writeHeader(msgType MessageType, h Header) ([]byte, error) {
	clientID, err := serializeClientID(h.ClientID)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, headerSize+8)
	buf = append(buf, byte(msgType))
	buf = append(buf, serializer.SerializeUint16(uint16(h.GatewayID))...)
	buf = append(buf, clientID...)
	return buf, nil
}

// parseHeader extracts the MessageType, the header and returns the rest of the
// buffer (the type-specific body).
func parseHeader(data []byte) (MessageType, Header, []byte, error) {
	if len(data) < headerSize {
		return 0, Header{}, nil, ErrMalformedEnvelope
	}
	msgType := MessageType(data[0])
	gatewayID := serializer.DeserializeUint16(data[1:3])
	clientID, err := deserializeClientID(data[3:headerSize])
	if err != nil {
		return 0, Header{}, nil, ErrMalformedEnvelope
	}
	header := Header{GatewayID: GatewayID(gatewayID), ClientID: clientID}
	return msgType, header, data[headerSize:], nil
}

// serializeClientID encodes the ClientID (UUID in string format) as 16 bytes.
func serializeClientID(c ClientID) ([]byte, error) {
	parsed, err := uuid.Parse(string(c))
	if err != nil {
		return nil, fmt.Errorf("client id is not a valid uuid: %w", err)
	}
	bytes, err := parsed.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// deserializeClientID reconstructs the ClientID (string format) from 16 bytes.
func deserializeClientID(data []byte) (ClientID, error) {
	parsed, err := uuid.FromBytes(data)
	if err != nil {
		return "", err
	}
	return ClientID(parsed.String()), nil
}

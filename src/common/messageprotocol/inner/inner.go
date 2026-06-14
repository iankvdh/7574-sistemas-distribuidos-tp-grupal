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
	// ClientAborted signals that the gateway aborted a client session. No payload;
	// SeqID=0 is allowed for this type. Workers use it to trigger cleanup.
	ClientAborted
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
//	[ MessageType u8 ][ GatewayID u16 BE ][ ClientID 16B UUID ]
//	[ SeqID u64 BE ][ SenderStageType u8 ][ SenderReplicaID u16 BE ]
//
// Total: 30 bytes.
type Header struct {
	GatewayID       GatewayID
	ClientID        ClientID
	SeqID           uint64 // autoincremental per (client, publisher); 0 only allowed for ClientAborted
	SenderStageType uint8  // StageType constant of the publisher (see stage_types.go)
	SenderReplicaID uint16 // REPLICA_ID of the publisher
}

// headerSize is the fixed wire size of the header (including the MessageType byte):
// type (1) + gatewayID (2) + clientID (16) + seqID (8) + stageType (1) + replicaID (2) = 30.
const headerSize = 1 + int(serializer.UINT16_SIZE) + 16 +
	int(serializer.UINT64_SIZE) + 1 + int(serializer.UINT16_SIZE)

// SeqKey is the dedup key used by consumers to track the last seen SeqID per
// upstream sender. It distinguishes senders of the same stage type by ReplicaID,
// and senders of different stage types by StageType (avoids collisions when two
// stage types with the same REPLICA_ID publish to the same queue).
type SeqKey struct {
	ClientID  ClientID
	StageType uint8
	ReplicaID uint16
}

// Envelope is the IN-MEMORY representation of an item that the runtime delivers
// to the strategies. It is NOT the wire format: the runtime builds it when
// exploding a Batch (one Envelope per item) or when receiving an EOF.
type Envelope struct {
	GatewayID       GatewayID
	ClientID        ClientID
	Kind            MsgKind
	Total           uint32
	QueryID         uint8
	Payload         []byte
	InputIndex      int
	SeqID           uint64
	SenderStageType uint8
	SenderReplicaID uint16
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

// HeaderWireSize returns the fixed byte size of the serialized header
// (including the MessageType byte). All InternalMessages start with this many bytes.
func HeaderWireSize() int { return headerSize }

// PeekHeader reads the MessageType and Header from raw bytes without parsing
// the message body. Useful for lightweight recovery passes that only need sender
// identity and SeqID.
func PeekHeader(data []byte) (MessageType, Header, error) {
	msgType, header, _, err := parseHeader(data)
	return msgType, header, err
}

// HeaderOf extracts the Header from any InternalMessage via exhaustive type
// switch. If a new MessageType is added without updating this function, it
// panics — making the omission visible in tests.
func HeaderOf(msg InternalMessage) Header {
	switch m := msg.(type) {
	case *BatchMessage:
		return m.Header
	case *EOFMessage:
		return m.Header
	case *RingTokenMessage:
		return m.Header
	case *ClientAbortedMessage:
		return m.Header
	default:
		panic(fmt.Sprintf("HeaderOf: unknown InternalMessage type %T", msg))
	}
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
	case ClientAborted:
		return parseClientAborted(header, body)
	default:
		return nil, ErrUnexpectedKind
	}
}

// validateHeader rejects messages with missing SeqID or SenderStageType.
// SeqID=0 is allowed only for ClientAborted (which has no sequence semantics).
func validateHeader(h Header, typ MessageType) error {
	if h.SeqID == 0 && typ != ClientAborted {
		return fmt.Errorf("%w: SeqID=0 in message type=%d", ErrMalformedEnvelope, typ)
	}
	if h.SenderStageType == 0 {
		return fmt.Errorf("%w: SenderStageType=0 in message type=%d", ErrMalformedEnvelope, typ)
	}
	return nil
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
	buf = append(buf, serializer.SerializeUint64(h.SeqID)...)
	buf = append(buf, h.SenderStageType)
	buf = append(buf, serializer.SerializeUint16(h.SenderReplicaID)...)
	return buf, nil
}

// parseHeader extracts the MessageType, the header and returns the rest of the
// buffer (the type-specific body). It calls validateHeader before returning.
func parseHeader(data []byte) (MessageType, Header, []byte, error) {
	if len(data) < headerSize {
		return 0, Header{}, nil, ErrMalformedEnvelope
	}
	msgType := MessageType(data[0])
	gatewayID := serializer.DeserializeUint16(data[1:3])
	clientID, err := deserializeClientID(data[3:19])
	if err != nil {
		return 0, Header{}, nil, ErrMalformedEnvelope
	}
	seqID := serializer.DeserializeUint64(data[19:27])
	senderStageType := data[27]
	senderReplicaID := serializer.DeserializeUint16(data[28:30])
	header := Header{
		GatewayID:       GatewayID(gatewayID),
		ClientID:        clientID,
		SeqID:           seqID,
		SenderStageType: senderStageType,
		SenderReplicaID: senderReplicaID,
	}
	if err := validateHeader(header, msgType); err != nil {
		return 0, Header{}, nil, err
	}
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

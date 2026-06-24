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
	// ClientAborted signals that a client session ended at the gateway
	// (disconnect/abort or gateway revival). Workers consume it to drop all state
	// for that client. It carries only the Header (ClientID); SeqID=0, so it is
	// outside the per-sequence dedup stream (idempotency guarded by tombstones).
	ClientAborted
)

// MsgKind identifies the type of item that travels inside a Batch.
type MsgKind uint8

const (
	// TransactionMessage: payload = external.SerializeTransaction(tx, mask).
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
//	[ MinterStageType u8 ][ MinterReplicaID u16 BE ]
//	[ BranchPathLen u8 ][ BranchPath BranchPathLen×u8 ]
//
// Fixed part: 34 bytes; total = 34 + BranchPathLen.
type Header struct {
	GatewayID       GatewayID
	ClientID        ClientID
	SeqID           uint64 // origin id: minted once by the minter, carried unchanged by online stages; must be > 0
	SenderStageType uint8  // StageType constant of the immediate publisher (see stage_types.go)
	SenderReplicaID uint16 // REPLICA_ID of the immediate publisher
	// MinterStageType/MinterReplicaID identify the minter of SeqID: the root of the
	// idSpaceId (dedup key). For gateway-origin data: (StageGateway, shard). For
	// reducer-minted data: (reducerStage, replicaID). Carried unchanged by online
	// stages; reset on re-mint.
	MinterStageType uint8
	MinterReplicaID uint16
	// BranchPath records the outputIndex taken at each semantic fan-out (e.g. filter
	// matched vs nomatched). Appended at fan-outs, preserved by passthrough/joiner,
	// reset on re-mint. Together with the minter root it forms the idSpaceId.
	BranchPath []uint8
}

// minHeaderSize is the fixed wire size of the header (including the MessageType
// byte and the BranchPathLen byte, with an empty BranchPath):
// type(1) + gatewayID(2) + clientID(16) + seqID(8) + senderStage(1) + senderReplica(2)
//   - minterStage(1) + minterReplica(2) + branchPathLen(1) = 34.
//
// The real header is variable-length: it grows by len(BranchPath) bytes.
const minHeaderSize = 1 + int(serializer.UINT16_SIZE) + 16 +
	int(serializer.UINT64_SIZE) + 1 + int(serializer.UINT16_SIZE) +
	1 + int(serializer.UINT16_SIZE) + 1

// CoordinationDedupKey is the dedup key used by consumers to track the last seen
// SeqID per upstream sender for coordination messages (EOF and RingToken). It
// distinguishes senders of the same stage type by ReplicaID, and senders of
// different stage types by StageType (avoids collisions when two stage types with
// the same REPLICA_ID publish to the same queue).

type CoordinationDedupKey struct {
	ClientID  ClientID
	StageType uint8
	ReplicaID uint16
}

// DedupKey identifies a dense origin-id space at a consumer. Data batches are
// deduplicated per DedupKey: within a key the SeqID (origin id) is dense and
// unique. IDSpace encodes the minter root (MinterStageType, MinterReplicaID) and
// the BranchPath, so batches that share a SeqID but come from different lineages
// (e.g. matched vs nomatched that re-converge in a joiner) do not collide.
type DedupKey struct {
	ClientID ClientID
	IDSpace  string
}

// IDSpaceOf encodes the minter root + branchPath of a header into a compact,
// comparable string usable as a map key.
// IDSpaceOf returns the root of the data deduplication key (what goes with the
// SeqID in (ClientID, idSpace, SeqID)). It combines:
//   - the carried MINTER (who minted the SeqID): distinguishes sources whose SeqIDs
//     overlap in a fan-in (e.g. final_joiner forwarding rows from several
//     aggregators, each minting from 1).
//   - the SENDER (the immediate publishing replica): distinguishes multi-replica
//     re-convergences — when per-item sharding splits an origin batch across
//     several replicas of a stage that re-emit the same carried (minter, SeqID),
//     each publishing replica produces a distinct key, so they are not discarded as
//     false duplicates (cause of the multi-replica deadlock). A redelivery/re-emission
//     keeps the same sender → still deduplicates.
//   - the branchPath: distinguishes the outputs of a single node (matched/nomatched).
func IDSpaceOf(h Header) string {
	b := make([]byte, 0, 6+len(h.BranchPath))
	b = append(b, h.MinterStageType, byte(h.MinterReplicaID>>8), byte(h.MinterReplicaID))
	b = append(b, h.SenderStageType, byte(h.SenderReplicaID>>8), byte(h.SenderReplicaID))
	b = append(b, h.BranchPath...)
	return string(b)
}

// DedupKeyOf builds the dedup key for a data message header.
func DedupKeyOf(h Header) DedupKey {
	return DedupKey{ClientID: h.ClientID, IDSpace: IDSpaceOf(h)}
}

// WithBranch returns a copy of branchPath with outputIndex appended. Used at
// fan-out nodes (more than one output target) to keep sibling branches
// distinguishable when they re-converge downstream.
func WithBranch(branchPath []uint8, outputIndex int) []uint8 {
	out := make([]uint8, len(branchPath)+1)
	copy(out, branchPath)
	out[len(branchPath)] = uint8(outputIndex)
	return out
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
	// LocalCount: distinct items processed for this client (set by the runtime on
	// the EOF). Used by the JAC nodes for the count-based completeness check:
	// do not finalize until all data has been drained.
	LocalCount uint64
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

// HeaderWireSize returns the MINIMUM byte size of the serialized header (fixed
// part, with an empty BranchPath). The real header is variable-length: it grows
// by len(BranchPath) bytes. Callers use it only as a lower-bound sanity check.
func HeaderWireSize() int { return minHeaderSize }

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
// Every message must carry SeqID > 0 (the dedup sequence) and SenderStageType != 0.
func validateHeader(h Header, typ MessageType) error {
	if typ == ClientAborted {
		// ClientAborted travels with SeqID=0 (outside the per-sequence dedup
		// stream): it is identified only by ClientID, so SeqID>0 is not required.
		if h.SenderStageType == 0 {
			return fmt.Errorf("%w: SenderStageType=0 in message type=%d", ErrMalformedEnvelope, typ)
		}
		return nil
	}
	if h.SeqID == 0 {
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
	if len(h.BranchPath) > 0xFF {
		return nil, fmt.Errorf("%w: branchPath too long (%d > 255)", ErrMalformedEnvelope, len(h.BranchPath))
	}
	buf := make([]byte, 0, minHeaderSize+len(h.BranchPath)+8)
	buf = append(buf, byte(msgType))
	buf = append(buf, serializer.SerializeUint16(uint16(h.GatewayID))...)
	buf = append(buf, clientID...)
	buf = append(buf, serializer.SerializeUint64(h.SeqID)...)
	buf = append(buf, h.SenderStageType)
	buf = append(buf, serializer.SerializeUint16(h.SenderReplicaID)...)
	buf = append(buf, h.MinterStageType)
	buf = append(buf, serializer.SerializeUint16(h.MinterReplicaID)...)
	buf = append(buf, byte(len(h.BranchPath)))
	buf = append(buf, h.BranchPath...)
	return buf, nil
}

// parseHeader extracts the MessageType, the header and returns the rest of the
// buffer (the type-specific body). It calls validateHeader before returning.
func parseHeader(data []byte) (MessageType, Header, []byte, error) {
	if len(data) < minHeaderSize {
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
	minterStageType := data[30]
	minterReplicaID := serializer.DeserializeUint16(data[31:33])
	branchPathLen := int(data[33])
	totalHeaderSize := minHeaderSize + branchPathLen
	if len(data) < totalHeaderSize {
		return 0, Header{}, nil, ErrMalformedEnvelope
	}
	var branchPath []uint8
	if branchPathLen > 0 {
		branchPath = make([]uint8, branchPathLen)
		copy(branchPath, data[minHeaderSize:totalHeaderSize])
	}
	header := Header{
		GatewayID:       GatewayID(gatewayID),
		ClientID:        clientID,
		SeqID:           seqID,
		SenderStageType: senderStageType,
		SenderReplicaID: senderReplicaID,
		MinterStageType: minterStageType,
		MinterReplicaID: minterReplicaID,
		BranchPath:      branchPath,
	}
	if err := validateHeader(header, msgType); err != nil {
		return 0, Header{}, nil, err
	}
	return msgType, header, data[totalHeaderSize:], nil
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

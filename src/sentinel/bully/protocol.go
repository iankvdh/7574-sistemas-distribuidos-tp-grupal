package bully

import "fmt"

type MsgType byte

const (
	MsgElection     MsgType = 0x01
	MsgOK           MsgType = 0x02
	MsgCoord        MsgType = 0x03
	MsgSentinelPing MsgType = 0x04
	MsgSentinelPong MsgType = 0x05
)

func (t MsgType) String() string {
	switch t {
	case MsgElection:
		return "Election"
	case MsgOK:
		return "OK"
	case MsgCoord:
		return "Coord"
	case MsgSentinelPing:
		return "Ping"
	case MsgSentinelPong:
		return "Pong"
	default:
		return fmt.Sprintf("Unknown(0x%02x)", byte(t))
	}
}

const SentinelMessageSize = 2

type SentinelMessage struct {
	Type     MsgType
	SenderID byte
}

// Every SentinelMessage on the wire is exactly two bytes: [1B type | 1B senderID].
func (m SentinelMessage) Encode() []byte {
	return []byte{byte(m.Type), m.SenderID}
}

func DecodeSentinelMessage(b []byte) (SentinelMessage, error) {
	if len(b) != SentinelMessageSize {
		return SentinelMessage{}, fmt.Errorf("bully: invalid message, expected %d bytes, got %d", SentinelMessageSize, len(b))
	}
	return SentinelMessage{Type: MsgType(b[0]), SenderID: b[1]}, nil
}

type Peer struct {
	ID      byte
	TCPAddr string
	UDPAddr string
}

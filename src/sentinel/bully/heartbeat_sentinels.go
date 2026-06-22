package bully

import (
	"log/slog"
	"net"
	"sync/atomic"
)

type HeartbeatSender interface {
	SendHeartbeat(peerUDPAddr string, self byte) error
}

type HeartbeatTransport struct {
	conn   *net.UDPConn
	closed atomic.Bool
}

func NewHeartbeatTransport(listenAddr string) (*HeartbeatTransport, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	return &HeartbeatTransport{conn: conn}, nil
}

// Serve reads incoming heartbeats and calls onPeerHB with the sender ID.
func (t *HeartbeatTransport) Serve(self byte, onPeerHB func(byte)) {
	buf := make([]byte, SentinelMessageSize)
	for {
		n, _, err := t.conn.ReadFromUDP(buf)
		if err != nil {
			if t.closed.Load() {
				return
			}
			continue
		}
		msg, derr := DecodeSentinelMessage(buf[:n])
		if derr != nil {
			continue
		}
		if msg.Type == MsgPeerHeartbeat && msg.SenderID != self {
			if onPeerHB != nil {
				onPeerHB(msg.SenderID)
			}
		} else {
			slog.Debug("bully: unexpected UDP message type", "type", msg.Type)
		}
	}
}

func (t *HeartbeatTransport) SendHeartbeat(peerUDPAddr string, self byte) error {
	addr, err := net.ResolveUDPAddr("udp", peerUDPAddr)
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Write(SentinelMessage{MsgPeerHeartbeat, self}.Encode())
	return err
}

func (t *HeartbeatTransport) Close() error {
	t.closed.Store(true)
	return t.conn.Close()
}

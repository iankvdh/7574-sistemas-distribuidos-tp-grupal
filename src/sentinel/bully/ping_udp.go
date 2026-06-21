package bully

import (
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"time"
)

type HeartbeatSender interface {
	SendHeartbeat(peerUDPAddr string, self byte) error
}

type PingTransport struct {
	conn        *net.UDPConn
	pongTimeout time.Duration
	closed      atomic.Bool
}

func NewPingTransport(listenAddr string, pongTimeout time.Duration) (*PingTransport, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	return &PingTransport{conn: conn, pongTimeout: pongTimeout}, nil
}

func (t *PingTransport) ServePing(isLeader func() bool, self byte, onPeerHB func(byte)) {
	buf := make([]byte, SentinelMessageSize)
	for {
		n, src, err := t.conn.ReadFromUDP(buf)
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
		switch msg.Type {
		case MsgSentinelPing:
			if isLeader() {
				if _, err := t.conn.WriteToUDP(SentinelMessage{MsgSentinelPong, self}.Encode(), src); err != nil {
					slog.Debug("bully: failed to write pong", "err", err)
				}
			}
		case MsgSentinelHeartbeat:
			if onPeerHB != nil {
				onPeerHB(msg.SenderID)
			}
		}
	}
}

func (t *PingTransport) PingOnce(leaderAddr string, self byte) bool {
	udpAddr, err := net.ResolveUDPAddr("udp", leaderAddr)
	if err != nil {
		return false
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return false
	}
	defer conn.Close()
	if _, err := conn.Write(SentinelMessage{MsgSentinelPing, self}.Encode()); err != nil {
		return false
	}
	_ = conn.SetReadDeadline(time.Now().Add(t.pongTimeout))
	buf := make([]byte, SentinelMessageSize)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return false // timeout or error -> ping lost
	}
	msg, derr := DecodeSentinelMessage(buf)
	return derr == nil && msg.Type == MsgSentinelPong
}

func (t *PingTransport) SendHeartbeat(peerUDPAddr string, self byte) error {
	addr, err := net.ResolveUDPAddr("udp", peerUDPAddr)
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Write(SentinelMessage{MsgSentinelHeartbeat, self}.Encode())
	return err
}

func (t *PingTransport) Close() error {
	t.closed.Store(true)
	return t.conn.Close()
}

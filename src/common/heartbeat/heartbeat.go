package heartbeat

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"time"
)

const MsgHeartbeat byte = 0x01

// Wire format (binary, not JSON): [ 1B type=MsgHeartbeat | container name bytes ].
func Encode(containerName string) []byte {
	buf := make([]byte, 1+len(containerName))
	buf[0] = MsgHeartbeat
	copy(buf[1:], containerName)
	return buf
}

func Decode(b []byte) (string, error) {
	if len(b) < 1 || b[0] != MsgHeartbeat {
		return "", fmt.Errorf("heartbeat: not a heartbeat frame")
	}
	name := string(b[1:])
	if name == "" {
		return "", fmt.Errorf("heartbeat: empty container name")
	}
	return name, nil
}

type Emitter struct {
	conn     *net.UDPConn
	dests    []string
	msg      []byte
	interval time.Duration
	jitter   time.Duration
}

func New(containerName string, sentinelAddrs []string, interval, jitter time.Duration) (*Emitter, error) {
	if containerName == "" {
		return nil, fmt.Errorf("heartbeat: container name is required")
	}
	if len(sentinelAddrs) == 0 {
		return nil, fmt.Errorf("heartbeat: at least one sentinel address is required")
	}

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return nil, fmt.Errorf("heartbeat: open udp socket: %w", err)
	}

	if jitter < 0 {
		jitter = 0
	}
	dests := make([]string, len(sentinelAddrs))
	copy(dests, sentinelAddrs)
	return &Emitter{
		conn:     conn,
		dests:    dests,
		msg:      Encode(containerName),
		interval: interval,
		jitter:   jitter,
	}, nil
}

func (e *Emitter) Run(ctx context.Context) {
	defer e.conn.Close()
	for {
		delay := e.interval
		if e.jitter > 0 {
			delay += time.Duration(rand.Int63n(int64(e.jitter) + 1))
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			e.beat()
		}
	}
}

func (e *Emitter) beat() {
	for _, dest := range e.dests {
		addr, err := net.ResolveUDPAddr("udp", dest)
		if err != nil {
			slog.Debug("heartbeat: skip unresolved sentinel", "dest", dest, "err", err)
			continue
		}
		_, _ = e.conn.WriteToUDP(e.msg, addr)
	}
}

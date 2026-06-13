package bully

import (
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external/safeio"
)

type ControlTransport struct {
	listener     net.Listener
	dialTimeout  time.Duration
	writeTimeout time.Duration
	readTimeout  time.Duration
	closed       atomic.Bool
}

func NewControlTransport(listenAddr string, dialTimeout, ioTimeout time.Duration) (*ControlTransport, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	return &ControlTransport{
		listener:     listener,
		dialTimeout:  dialTimeout,
		writeTimeout: ioTimeout,
		readTimeout:  ioTimeout,
	}, nil
}

func (t *ControlTransport) Send(peerAddr string, msg SentinelMessage) error {
	conn, err := net.DialTimeout("tcp", peerAddr, t.dialTimeout)
	if err != nil {
		return err // peer down or unreachable
	}
	defer conn.Close()
	_ = conn.SetWriteDeadline(time.Now().Add(t.writeTimeout))
	return safeio.WriteAll(conn, msg.Encode())
}

func (t *ControlTransport) Serve(handler func(SentinelMessage)) {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			if t.closed.Load() {
				return
			}
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			_ = c.SetReadDeadline(time.Now().Add(t.readTimeout))
			buf, err := safeio.ReadAll(c, SentinelMessageSize)
			if err != nil {
				return
			}
			msg, derr := DecodeSentinelMessage(buf)
			if derr != nil {
				slog.Debug("bully: malformed control message", "err", derr)
				return
			}
			handler(msg)
		}(conn)
	}
}

func (t *ControlTransport) Close() error {
	t.closed.Store(true)
	return t.listener.Close()
}

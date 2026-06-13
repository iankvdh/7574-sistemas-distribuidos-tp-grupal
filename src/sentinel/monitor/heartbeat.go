package monitor

import (
	"context"
	"net"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/heartbeat"
)

const readBufferSize = 512

type heartbeatListener struct {
	conn *net.UDPConn
}

func newHeartbeatListener(listenAddr string) (*heartbeatListener, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	return &heartbeatListener{conn: conn}, nil
}

func (l *heartbeatListener) listen(ctx context.Context, onBeat func(containerName string)) {
	buf := make([]byte, readBufferSize)
	for {
		if ctx.Err() != nil {
			return
		}
		_ = l.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, _, err := l.conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		name, err := heartbeat.Decode(buf[:n])
		if err != nil {
			continue
		}
		onBeat(name)
	}
}

func (l *heartbeatListener) close() error {
	return l.conn.Close()
}

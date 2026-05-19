package client

import (
	"fmt"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
)

const (
	queryEOFStatus     = "EOF"
	requiredQueryEOFs  = 5
	minExpectedQueryID = 1
	maxExpectedQueryID = 5
)

func (client *Client) receiveResults() error {
	if err := client.initResultsCollector(); err != nil {
		return err
	}
	defer client.conn.SetReadDeadline(time.Time{})

	for {
		if client.hasAllQueryEOFs() {
			return nil
		}

		if err := client.conn.SetReadDeadline(time.Now().Add(client.config.QueryWaitTimeout)); err != nil {
			return err
		}

		msgType, err := external.ReadMsgType(client.conn)
		if err != nil {
			return err
		}

		switch msgType {
		case external.QueryResult:
			if err := client.consumeQueryResultFromConn(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected result message type: %d", msgType)
		}
	}
}

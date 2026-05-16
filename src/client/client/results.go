package client

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
)

func (client *Client) receiveResults() error {
	if err := client.conn.SetReadDeadline(time.Now().Add(client.config.QueryWaitTimeout)); err != nil {
		return err
	}
	defer client.conn.SetReadDeadline(time.Time{})

	for {
		msgType, err := external.ReadMsgType(client.conn)
		if err != nil {
			return err
		}

		switch msgType {
		case external.QueryResult:
			queryID, status, err := external.ReadQueryResult(client.conn)
			if err != nil {
				return err
			}
			slog.Info("Received query result", "client_id", client.clientID, "query", queryID, "status", status)
		case external.EndOfResults:
			slog.Info("Received end of results", "client_id", client.clientID)
			return nil
		default:
			return fmt.Errorf("unexpected result message type: %d", msgType)
		}
	}
}

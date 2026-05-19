package client

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
)

func TestReceiveResultsCompletesAfterFiveQueryEOFs(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	c := &Client{
		conn:     clientConn,
		clientID: "test-client",
		config: ClientConfig{
			ResultsDir: t.TempDir(),
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- c.receiveResults()
	}()

	for queryID := uint8(1); queryID <= 5; queryID++ {
		if err := external.WriteQueryResult(serverConn, queryID, "row"); err != nil {
			t.Fatalf("WriteQueryResult row failed for query %d: %v", queryID, err)
		}
		if err := external.WriteQueryResult(serverConn, queryID, queryEOFStatus); err != nil {
			t.Fatalf("WriteQueryResult EOF failed for query %d: %v", queryID, err)
		}
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("receiveResults returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receiveResults did not finish after receiving five query EOF markers")
	}

	for queryID := uint8(1); queryID <= 5; queryID++ {
		filePath := filepath.Join(c.config.ResultsDir, "test-client_q"+strconv.Itoa(int(queryID))+".csv")
		content, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("while reading query file %d: %v", queryID, err)
		}
		rows := strings.Split(strings.TrimSpace(string(content)), "\n")
		if len(rows) != 2 {
			t.Fatalf("unexpected row count in query file %d: got %d rows (%v)", queryID, len(rows), rows)
		}
		if rows[1] != "row" {
			t.Fatalf("unexpected persisted row in query file %d: got %q", queryID, rows[1])
		}
	}
}

func TestExpectMsgTypeConsumesInterleavedQueryResult(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	c := &Client{
		conn:     clientConn,
		clientID: "test-client",
		config: ClientConfig{
			ResultsDir: t.TempDir(),
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- c.expectMsgType(external.Ack)
	}()

	if err := external.WriteQueryResult(serverConn, 1, "row-before-ack"); err != nil {
		t.Fatalf("WriteQueryResult failed: %v", err)
	}
	if err := external.WriteAck(serverConn); err != nil {
		t.Fatalf("WriteAck failed: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expectMsgType returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expectMsgType did not return after receiving Ack")
	}

	content, err := os.ReadFile(filepath.Join(c.config.ResultsDir, "test-client_q1.csv"))
	if err != nil {
		t.Fatalf("while reading persisted q1 file: %v", err)
	}
	rows := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(rows) != 2 {
		t.Fatalf("unexpected row count in q1 file: got %d rows (%v)", len(rows), rows)
	}
	if rows[1] != "row-before-ack" {
		t.Fatalf("unexpected persisted row: got %q", rows[1])
	}
}

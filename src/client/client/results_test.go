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

func TestSendIngestMessageHandlesInterleavedResultBatch(t *testing.T) {
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
	c.initIOChannels()
	c.running.Store(true)
	if err := c.initResultsCollector(); err != nil {
		t.Fatalf("initResultsCollector failed: %v", err)
	}
	defer c.closeResultsCollector()

	c.startIOLoops()
	defer c.stopIO()

	serverDone := make(chan error, 1)
	go func() {
		msgType, err := external.ReadMsgType(serverConn)
		if err != nil {
			serverDone <- err
			return
		}
		if msgType != external.EndOfTransactions {
			serverDone <- errUnexpectedMsgType(msgType, external.EndOfTransactions)
			return
		}

		if err := external.WriteResultBatch(serverConn, []external.ResultBatchItem{
			{QueryID: 1, Status: "20,A,31,B,10.00"},
			{QueryID: 1, Status: queryEOFStatus},
		}); err != nil {
			serverDone <- err
			return
		}

		msgType, err = external.ReadMsgType(serverConn)
		if err != nil {
			serverDone <- err
			return
		}
		if msgType != external.ResultBatchAck {
			serverDone <- errUnexpectedMsgType(msgType, external.ResultBatchAck)
			return
		}

		if err := external.WriteIngestAck(serverConn); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	if err := c.sendIngestMessage(func(conn net.Conn) error {
		return external.WriteEndOfTransactions(conn)
	}); err != nil {
		t.Fatalf("sendIngestMessage returned error: %v", err)
	}

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("server side flow failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server side flow timed out")
	}

	content, err := os.ReadFile(filepath.Join(c.config.ResultsDir, "test-client_q1.csv"))
	if err != nil {
		t.Fatalf("while reading persisted q1 file: %v", err)
	}
	rows := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(rows) != 2 {
		t.Fatalf("unexpected row count in q1 file: got %d rows (%v)", len(rows), rows)
	}
	if rows[1] != "20,A,31,B,10.00" {
		t.Fatalf("unexpected row in q1 file: %q", rows[1])
	}
}

func TestWaitForAllQueryEOFs(t *testing.T) {
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
	c.initIOChannels()
	c.running.Store(true)
	if err := c.initResultsCollector(); err != nil {
		t.Fatalf("initResultsCollector failed: %v", err)
	}
	defer c.closeResultsCollector()

	c.startIOLoops()
	defer c.stopIO()

	for queryID := uint8(1); queryID <= 5; queryID++ {
		if err := external.WriteResultBatch(serverConn, []external.ResultBatchItem{
			{QueryID: queryID, Status: "row"},
			{QueryID: queryID, Status: queryEOFStatus},
		}); err != nil {
			t.Fatalf("WriteResultBatch failed for query %d: %v", queryID, err)
		}

		msgType, err := external.ReadMsgType(serverConn)
		if err != nil {
			t.Fatalf("while reading ResultBatchAck for query %d: %v", queryID, err)
		}
		if msgType != external.ResultBatchAck {
			t.Fatalf("unexpected msg type after result batch: got=%d want=%d", msgType, external.ResultBatchAck)
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- c.waitForAllQueryEOFs()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitForAllQueryEOFs returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitForAllQueryEOFs timed out")
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

func errUnexpectedMsgType(got, expected external.MsgType) error {
	return &unexpectedMsgTypeError{got: got, expected: expected}
}

type unexpectedMsgTypeError struct {
	got      external.MsgType
	expected external.MsgType
}

func (err *unexpectedMsgTypeError) Error() string {
	return "unexpected message type: got " + strconv.Itoa(int(err.got)) + " expected " + strconv.Itoa(int(err.expected))
}

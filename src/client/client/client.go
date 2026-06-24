package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
)

type ClientConfig struct {
	GatewayPrefix         string
	GatewayAmount         int
	GatewayPort           string
	InputTransactions     string
	InputAccounts         string
	ResultsDir            string
	MaxExternalBatchBytes int
	ExpectedQueryEOFs     int
	ConnectMaxAttempts    int
	BackoffBase           time.Duration
	BackoffMax            time.Duration
	ConnectTimeout        time.Duration
	ReconnectMaxElapsed   time.Duration
}

type sessionOutcome int

const (
	sessionCompleted sessionOutcome = iota
	sessionStopped
	sessionFailed
	sessionDropped
)

type Client struct {
	config       ClientConfig
	gatewayAddrs []string

	ctx     context.Context
	cancel  context.CancelFunc
	running atomic.Bool

	conn            net.Conn
	clientID        string
	results         *resultsCollector
	sessionCtx      context.Context
	sessionCancel   context.CancelFunc
	allEOFsReceived atomic.Bool
	ingestAckCh     chan struct{}
	allQueryEOFCh   chan struct{}

	writeMu sync.Mutex
	ioErrMu sync.Mutex
	ioErr   error
}

func NewClient(config ClientConfig) (*Client, error) {
	gatewayAddrs := buildGatewayAddresses(config)
	if len(gatewayAddrs) == 0 {
		return nil, errors.New("gateway list is empty")
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		config:       config,
		gatewayAddrs: gatewayAddrs,
		ctx:          ctx,
		cancel:       cancel,
	}
	client.running.Store(true)
	return client, nil
}

func buildGatewayAddresses(config ClientConfig) []string {
	addrs := make([]string, 0, config.GatewayAmount)
	for i := 1; i <= config.GatewayAmount; i++ {
		host := config.GatewayPrefix + strconv.Itoa(i)
		addrs = append(addrs, host+":"+config.GatewayPort)
	}
	return addrs
}

func (client *Client) Run() error {
	go client.handleSignals()
	defer client.cancel()

	var reconnectDeadline time.Time
	for attempt := 1; ; attempt++ {
		if !client.running.Load() {
			return nil
		}

		switch client.runSession() {
		case sessionCompleted:
			slog.Info("Client completed successfully", "client_id", client.clientID)
			return nil
		case sessionStopped:
			slog.Info("Client shutting down gracefully", "client_id", client.clientID)
			return nil
		case sessionFailed:
			if !client.running.Load() {
				return nil
			}
			if reconnectDeadline.IsZero() {
				reconnectDeadline = time.Now().Add(client.config.ReconnectMaxElapsed)
			}
			if time.Now().After(reconnectDeadline) {
				return fmt.Errorf("reconnect budget exhausted after %s", client.config.ReconnectMaxElapsed)
			}
			slog.Warn("Could not connect to any gateway; retrying",
				"attempt", attempt)
			client.interruptibleSleep(client.backoffForAttempt(attempt))
		case sessionDropped:
			if !client.running.Load() {
				return nil
			}
			reconnectDeadline = time.Now().Add(client.config.ReconnectMaxElapsed)
			slog.Warn("Gateway session lost; retrying from scratch",
				"old_client_id", client.clientID, "attempt", attempt)
			client.cleanupPartialResults()
			client.interruptibleSleep(client.backoffForAttempt(attempt))
		}
	}
}

func (client *Client) runSession() sessionOutcome {
	conn, clientID, err := client.connectToGateway()
	if err != nil {
		if !client.running.Load() {
			return sessionStopped
		}
		slog.Warn("Could not establish a gateway connection for this attempt", "err", err)
		return sessionFailed
	}

	sessionCtx, sessionCancel := context.WithCancel(client.ctx)
	client.conn = conn
	client.clientID = clientID
	client.sessionCtx = sessionCtx
	client.sessionCancel = sessionCancel
	client.ingestAckCh = make(chan struct{}, 1)
	client.allQueryEOFCh = make(chan struct{})
	client.allEOFsReceived.Store(false)
	client.clearIOError()

	if err := client.initResultsCollector(); err != nil {
		slog.Error("While initializing results collector", "client_id", clientID, "err", err)
		sessionCancel()
		_ = conn.Close()
		return sessionFailed
	}

	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		client.readerLoop()
	}()

	client.runIngest()

	sessionCancel()
	_ = conn.Close()
	readerWG.Wait()
	if err := client.closeResultsCollector(); err != nil {
		slog.Warn("While closing result files", "client_id", clientID, "err", err)
	}

	switch {
	case client.allEOFsReceived.Load():
		return sessionCompleted
	case !client.running.Load():
		return sessionStopped
	default:
		return sessionDropped
	}
}

func (client *Client) runIngest() {
	if err := client.sendTransactions(); err != nil {
		slog.Debug("Transactions ingest ended", "client_id", client.clientID, "err", err)
		return
	}
	slog.Info("All transactions sent", "client_id", client.clientID)

	if err := client.sendAccounts(); err != nil {
		slog.Debug("Accounts ingest ended", "client_id", client.clientID, "err", err)
		return
	}
	slog.Info("All accounts sent", "client_id", client.clientID)

	if err := client.waitForAllQueryEOFs(); err != nil {
		slog.Debug("Waiting for query EOFs ended", "client_id", client.clientID, "err", err)
	}
}

func (client *Client) connectToGateway() (net.Conn, string, error) {
	var lastErr error

	for attempt := 1; attempt <= client.config.ConnectMaxAttempts; attempt++ {
		if client.ctx.Err() != nil {
			return nil, "", client.ctx.Err()
		}
		addr := client.pickRandomGatewayAddr()
		dialer := net.Dialer{Timeout: client.config.ConnectTimeout}

		conn, err := dialer.Dial("tcp", addr)
		if err != nil {
			lastErr = err
			slog.Warn("Gateway connection failed", "gateway", addr, "attempt", attempt, "err", err)
			client.sleepBackoff(attempt)
			continue
		}

		clientID, err := client.performHandshake(conn)
		if err != nil {
			lastErr = err
			_ = conn.Close()
			slog.Warn("Gateway handshake failed", "gateway", addr, "attempt", attempt, "err", err)
			client.sleepBackoff(attempt)
			continue
		}

		slog.Info("Connected to gateway", "gateway", addr, "client_id", clientID)
		return conn, clientID, nil
	}

	if lastErr == nil {
		lastErr = errors.New("connection attempts exhausted")
	}
	return nil, "", fmt.Errorf("could not connect to any gateway after %d attempts: %w", client.config.ConnectMaxAttempts, lastErr)
}

func (client *Client) performHandshake(conn net.Conn) (string, error) {
	if err := conn.SetDeadline(time.Now().Add(client.config.ConnectTimeout)); err != nil {
		return "", err
	}
	defer conn.SetDeadline(time.Time{})

	if err := external.WriteConnectRequest(conn); err != nil {
		return "", err
	}

	msgType, err := external.ReadMsgType(conn)
	if err != nil {
		return "", err
	}
	if msgType != external.ConnectAck {
		return "", errors.New("expected CONNECT_ACK")
	}

	clientID, err := external.ReadConnectAck(conn)
	if err != nil {
		return "", err
	}
	return clientID, nil
}

func (client *Client) pickRandomGatewayAddr() string {
	idx := rand.Intn(len(client.gatewayAddrs))
	return client.gatewayAddrs[idx]
}

func (client *Client) backoffForAttempt(attempt int) time.Duration {
	delay := computeBackoffCap(client.config.BackoffBase, client.config.BackoffMax, attempt)
	return time.Duration(rand.Int63n(int64(delay) + 1))
}

func (client *Client) sleepBackoff(attempt int) {
	if attempt >= client.config.ConnectMaxAttempts {
		return
	}
	client.interruptibleSleep(client.backoffForAttempt(attempt))
}

func (client *Client) interruptibleSleep(d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-client.ctx.Done():
	}
}

func computeBackoffCap(base, max time.Duration, attempt int) time.Duration {
	if attempt <= 1 {
		if base > max {
			return max
		}
		return base
	}

	delay := base
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= max {
			return max
		}
	}
	if delay > max {
		return max
	}
	return delay
}

func (client *Client) readerLoop() {
	for {
		msgType, err := external.ReadMsgType(client.conn)
		if err != nil {
			client.setIOError(err)
			return
		}

		switch msgType {
		case external.IngestAck:
			client.pushIngestAck()
		case external.ResultBatch:
			items, err := external.ReadResultBatch(client.conn)
			if err != nil {
				client.setIOError(err)
				return
			}

			if err := client.consumeResultBatch(items); err != nil {
				client.setIOError(err)
				return
			}

			if err := client.sendWrite(func(conn net.Conn) error {
				return external.WriteResultBatchAck(conn)
			}); err != nil {
				client.setIOError(err)
				return
			}

			if client.hasAllQueryEOFs() {
				slog.Info("Received all query EOF markers", "client_id", client.clientID)
				client.signalAllQueryEOFs()
				return
			}
		default:
			client.setIOError(fmt.Errorf("unexpected message type from gateway: %d", msgType))
			return
		}
	}
}

func (client *Client) sendWrite(writeAction func(conn net.Conn) error) error {
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	return writeAction(client.conn)
}

func (client *Client) sendIngestMessage(writeAction func(conn net.Conn) error) error {
	if err := client.sendWrite(writeAction); err != nil {
		return err
	}
	return client.waitForIngestAck()
}

func (client *Client) waitForIngestAck() error {
	select {
	case <-client.ingestAckCh:
		return nil
	case <-client.sessionCtx.Done():
		return client.ioErrorOrStopped()
	}
}

func (client *Client) waitForAllQueryEOFs() error {
	select {
	case <-client.allQueryEOFCh:
		return nil
	case <-client.sessionCtx.Done():
		return client.ioErrorOrStopped()
	}
}

func (client *Client) pushIngestAck() {
	select {
	case client.ingestAckCh <- struct{}{}:
	default:
		client.setIOError(errors.New("received unexpected extra ingest ack"))
	}
}

func (client *Client) signalAllQueryEOFs() {
	select {
	case <-client.allQueryEOFCh:
	default:
		client.allEOFsReceived.Store(true)
		close(client.allQueryEOFCh)
		client.sessionCancel()
	}
}

func (client *Client) setIOError(err error) {
	if err == nil {
		return
	}

	client.ioErrMu.Lock()
	if client.ioErr == nil {
		client.ioErr = err
	}
	client.ioErrMu.Unlock()

	client.sessionCancel()
}

func (client *Client) clearIOError() {
	client.ioErrMu.Lock()
	client.ioErr = nil
	client.ioErrMu.Unlock()
}

func (client *Client) ioErrorOrStopped() error {
	client.ioErrMu.Lock()
	defer client.ioErrMu.Unlock()
	if client.ioErr != nil {
		return client.ioErr
	}
	return errors.New("client stopped")
}

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case sig := <-signals:
		slog.Info("Signal received, shutting down client", "signal", sig.String())
		client.running.Store(false)
		client.cancel()
	case <-client.ctx.Done():
	}
}

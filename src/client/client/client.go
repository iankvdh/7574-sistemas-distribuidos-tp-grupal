package client

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/external"
)

type ClientConfig struct {
	GatewayPrefix      string
	GatewayAmount      int
	GatewayPort        string
	InputTransactions  string
	InputAccounts      string
	ResultsDir         string
	BatchMaxBytes      int
	ConnectMaxAttempts int
	BackoffBase        time.Duration
	BackoffMax         time.Duration
	ConnectTimeout     time.Duration
}

type Client struct {
	conn         net.Conn
	clientID     string
	running      atomic.Bool
	config       ClientConfig
	gatewayAddrs []string
	rand         *rand.Rand
	results      *resultsCollector
}

func NewClient(config ClientConfig) (*Client, error) {
	gatewayAddrs := buildGatewayAddresses(config)
	if len(gatewayAddrs) == 0 {
		return nil, errors.New("gateway list is empty")
	}

	client := &Client{
		config:       config,
		gatewayAddrs: gatewayAddrs,
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	client.running.Store(true)

	conn, clientID, err := client.connectToGateway()
	if err != nil {
		return nil, err
	}
	client.conn = conn
	client.clientID = clientID

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

func (client *Client) connectToGateway() (net.Conn, string, error) {
	var lastErr error

	for attempt := 1; attempt <= client.config.ConnectMaxAttempts; attempt++ {
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
	idx := client.rand.Intn(len(client.gatewayAddrs))
	return client.gatewayAddrs[idx]
}

func (client *Client) sleepBackoff(attempt int) {
	if attempt >= client.config.ConnectMaxAttempts {
		return
	}

	delay := computeBackoffCap(client.config.BackoffBase, client.config.BackoffMax, attempt)

	jitter := time.Duration(client.rand.Int63n(int64(delay) + 1))
	if jitter > 0 {
		time.Sleep(jitter)
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

func (client *Client) Run() error {
	defer client.conn.Close()
	go client.handleSignals()

	if err := client.initResultsCollector(); err != nil {
		return err
	}
	defer func() {
		if err := client.closeResultsCollector(); err != nil {
			slog.Warn("While closing result files", "client_id", client.clientID, "err", err)
		}
	}()

	if err := client.sendTransactions(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	if err := client.sendAccounts(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	if err := client.receiveResults(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	return nil
}

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	<-signals
	slog.Info("SIGTERM signal received")
	client.running.Store(false)
	_ = client.conn.Close()
}

func (client *Client) expectMsgType(expectedMsgType external.MsgType) error {
	for {
		msgType, err := external.ReadMsgType(client.conn)
		if err != nil {
			slog.Debug("Error while reading message type", "err", err)
			return err
		}
		if msgType == expectedMsgType {
			return nil
		}

		if msgType == external.QueryResult {
			if err := client.consumeQueryResultFromConn(); err != nil {
				return err
			}
			continue
		}

		return fmt.Errorf("unexpected message type: got %d expected %d", msgType, expectedMsgType)
	}
}

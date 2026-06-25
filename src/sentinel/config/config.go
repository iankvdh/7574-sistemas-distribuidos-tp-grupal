package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/sentinel/bully"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/sentinel/monitor"
)

type Config struct {
	SelfID byte
	Peers  []bully.Peer

	// Listen addresses (:port)
	BullyTCPListenPort   string
	SentinelHBListenPort string
	WorkerUDPListenPort  string

	ExpectedContainers []string

	// Bully config.
	LeaderCheckInterval time.Duration
	OKTimeout           time.Duration
	CoordTimeout        time.Duration
	BaseJitter          time.Duration
	ControlDialTimeout  time.Duration
	ControlIOTimeout    time.Duration

	// Worker monitor config.
	StartupGrace      time.Duration
	HeartbeatTimeout  time.Duration
	DetectionInterval time.Duration
	RestartCooldown   time.Duration
	RestartTimeout    time.Duration
	RestartStopGrace  int

	// Sentinel peer monitor config.
	SentinelPeers             []monitor.SentinelPeerSpec
	SentinelHBInterval        time.Duration
	SentinelPeerTimeout       time.Duration
	SentinelPeerGrace         time.Duration
	SentinelPeerCooldown      time.Duration
	SentinelDetectionInterval time.Duration
}

func Load() (Config, error) {
	selfIDInt, err := env.RequiredInt("SENTINEL_ID", false)
	if err != nil {
		return Config{}, err
	}
	if selfIDInt < 0 || selfIDInt > 254 {
		return Config{}, fmt.Errorf("SENTINEL_ID=%d out of range (0..254)", selfIDInt)
	}

	peersRaw, err := env.RequiredString("PEERS")
	if err != nil {
		return Config{}, err
	}
	peers, err := parsePeers(peersRaw)
	if err != nil {
		return Config{}, err
	}

	expectedRaw, err := env.RequiredString("EXPECTED_CONTAINERS")
	if err != nil {
		return Config{}, err
	}
	expected := env.SplitNonEmpty(expectedRaw)
	if len(expected) == 0 {
		return Config{}, fmt.Errorf("EXPECTED_CONTAINERS must list at least one container")
	}

	tcpPort, err := env.IntWithDefault("SENTINEL_BULLY_TCP_PORT", 8090, true)
	if err != nil {
		return Config{}, err
	}
	udpPort, err := env.IntWithDefault("SENTINEL_HB_UDP_PORT", 8092, true)
	if err != nil {
		return Config{}, err
	}
	workerUDPPort, err := env.IntWithDefault("SENTINEL_UDP_PORT", 8091, true)
	if err != nil {
		return Config{}, err
	}

	ints := map[string]int{}
	for _, spec := range []struct {
		key string
		def int
	}{
		{"STARTUP_GRACE_SECONDS", 30},
		{"HEARTBEAT_TIMEOUT_SECONDS", 12},
		{"DETECTION_INTERVAL_SECONDS", 3},
		{"RESTART_COOLDOWN_SECONDS", 60},
		{"RESTART_TIMEOUT_SECONDS", 30},
		{"RESTART_STOP_GRACE_SECONDS", 10},
		{"LEADER_CHECK_INTERVAL_SECONDS", 1},
		{"OK_TIMEOUT_SECONDS", 3},
		{"COORD_TIMEOUT_SECONDS", 5},
		{"BULLY_BOOTSTRAP_JITTER_MS", 500},
		{"CONTROL_DIAL_TIMEOUT_MS", 500},
		{"CONTROL_IO_TIMEOUT_MS", 2000},
		{"SENTINEL_HB_INTERVAL_SECONDS", 1},
		{"SENTINEL_PEER_TIMEOUT_SECONDS", 3},
		{"SENTINEL_PEER_GRACE_SECONDS", 5},
		{"SENTINEL_PEER_COOLDOWN_SECONDS", 12},
		{"SENTINEL_DETECTION_INTERVAL_SECONDS", 1},
	} {
		v, err := env.IntWithDefault(spec.key, spec.def, true)
		if err != nil {
			return Config{}, err
		}
		ints[spec.key] = v
	}

	sentinelPeers := make([]monitor.SentinelPeerSpec, 0, len(peers))
	for _, p := range peers {
		sentinelPeers = append(sentinelPeers, monitor.SentinelPeerSpec{
			ID:        p.ID,
			Container: p.Hostname,
		})
	}

	return Config{
		SelfID:               byte(selfIDInt),
		Peers:                peers,
		BullyTCPListenPort:   fmt.Sprintf(":%d", tcpPort),
		SentinelHBListenPort: fmt.Sprintf(":%d", udpPort),
		WorkerUDPListenPort:  fmt.Sprintf(":%d", workerUDPPort),

		ExpectedContainers: expected,

		LeaderCheckInterval: secs(ints["LEADER_CHECK_INTERVAL_SECONDS"]),
		OKTimeout:           secs(ints["OK_TIMEOUT_SECONDS"]),
		CoordTimeout:        secs(ints["COORD_TIMEOUT_SECONDS"]),
		BaseJitter:          msecs(ints["BULLY_BOOTSTRAP_JITTER_MS"]),
		ControlDialTimeout:  msecs(ints["CONTROL_DIAL_TIMEOUT_MS"]),
		ControlIOTimeout:    msecs(ints["CONTROL_IO_TIMEOUT_MS"]),

		StartupGrace:      secs(ints["STARTUP_GRACE_SECONDS"]),
		HeartbeatTimeout:  secs(ints["HEARTBEAT_TIMEOUT_SECONDS"]),
		DetectionInterval: secs(ints["DETECTION_INTERVAL_SECONDS"]),
		RestartCooldown:   secs(ints["RESTART_COOLDOWN_SECONDS"]),
		RestartTimeout:    secs(ints["RESTART_TIMEOUT_SECONDS"]),
		RestartStopGrace:  ints["RESTART_STOP_GRACE_SECONDS"],

		SentinelPeers:             sentinelPeers,
		SentinelHBInterval:        secs(ints["SENTINEL_HB_INTERVAL_SECONDS"]),
		SentinelPeerTimeout:       secs(ints["SENTINEL_PEER_TIMEOUT_SECONDS"]),
		SentinelPeerGrace:         secs(ints["SENTINEL_PEER_GRACE_SECONDS"]),
		SentinelPeerCooldown:      secs(ints["SENTINEL_PEER_COOLDOWN_SECONDS"]),
		SentinelDetectionInterval: secs(ints["SENTINEL_DETECTION_INTERVAL_SECONDS"]),
	}, nil
}

func parsePeers(raw string) ([]bully.Peer, error) {
	var peers []bully.Peer
	for _, entry := range env.SplitNonEmpty(raw) {
		parts := strings.Split(entry, ":")
		if len(parts) != 4 {
			return nil, fmt.Errorf("PEERS entry %q must be id:host:tcpPort:udpPort", entry)
		}
		id, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil || id < 0 || id > 254 {
			return nil, fmt.Errorf("PEERS entry %q has invalid id", entry)
		}
		host := strings.TrimSpace(parts[1])
		tcpPort := strings.TrimSpace(parts[2])
		udpPort := strings.TrimSpace(parts[3])
		if host == "" || tcpPort == "" || udpPort == "" {
			return nil, fmt.Errorf("PEERS entry %q has empty field", entry)
		}
		peers = append(peers, bully.Peer{
			ID:       byte(id),
			Hostname: host,
			TCPAddr:  net_JoinHostPort(host, tcpPort),
			UDPAddr:  net_JoinHostPort(host, udpPort),
		})
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("PEERS must list at least one peer")
	}
	return peers, nil
}

func net_JoinHostPort(host, port string) string {
	return host + ":" + port
}

func secs(n int) time.Duration  { return time.Duration(n) * time.Second }
func msecs(n int) time.Duration { return time.Duration(n) * time.Millisecond }

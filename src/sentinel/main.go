package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/logging"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/sentinel/bully"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/sentinel/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/sentinel/monitor"
)

func run() int {
	if err := logging.Init(); err != nil {
		slog.Error("While initializing logger", "err", err)
		return 1
	}

	cfg, err := config.Load()
	if err != nil {
		slog.Error("While loading sentinel config", "err", err)
		return 1
	}
	slog.SetDefault(slog.Default().With("sentinel_id", cfg.SelfID))

	ping, err := bully.NewPingTransport(cfg.BullyUDPListenPort, cfg.PongTimeout)
	if err != nil {
		slog.Error("While binding bully UDP (ping) transport", "addr", cfg.BullyUDPListenPort, "err", err)
		return 1
	}
	control, err := bully.NewControlTransport(cfg.BullyTCPListenPort, cfg.ControlDialTimeout, cfg.ControlIOTimeout)
	if err != nil {
		slog.Error("While binding bully TCP (control) transport", "addr", cfg.BullyTCPListenPort, "err", err)
		_ = ping.Close()
		return 1
	}

	// --- bully coordinator -----------------------------------------------------
	coordinator := bully.NewBullyCoordinator(bully.Config{
		SelfID:             cfg.SelfID,
		Peers:              cfg.Peers,
		LeaderPingInterval: cfg.LeaderPingInterval,
		LeaderPingFailures: cfg.LeaderPingFailures,
		OKTimeout:          cfg.OKTimeout,
		CoordTimeout:       cfg.CoordTimeout,
		BaseJitter:         cfg.BaseJitter,
	}, control, ping)

	// --- monitor ----------------------------------------------------------
	restarter := monitor.NewDockerRestarter(cfg.RestartTimeout, cfg.RestartStopGrace)
	mon, err := monitor.NewNodeMonitor(monitor.Config{
		ListenAddr:         cfg.WorkerUDPListenPort,
		ExpectedContainers: cfg.ExpectedContainers,
		StartupGrace:       cfg.StartupGrace,
		HeartbeatTimeout:   cfg.HeartbeatTimeout,
		DetectionInterval:  cfg.DetectionInterval,
		RestartCooldown:    cfg.RestartCooldown,
	}, coordinator, restarter, nil)
	if err != nil {
		slog.Error("While binding worker heartbeat listener", "addr", cfg.WorkerUDPListenPort, "err", err)
		_ = ping.Close()
		_ = control.Close()
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Serve loops for both transports.
	wg.Add(1)
	go func() { defer wg.Done(); control.Serve(coordinator.Handle) }()
	wg.Add(1)
	go func() { defer wg.Done(); ping.ServePing(coordinator.IsLeader, cfg.SelfID) }()

	// Bully state machine (bootstrap + leader monitoring).
	wg.Add(1)
	go func() { defer wg.Done(); coordinator.Run(ctx) }()

	// Worker monitor (heartbeat receiver + detection/restart loop).
	wg.Add(1)
	go func() { defer wg.Done(); mon.Run(ctx) }()

	slog.Info("Sentinel started",
		"peers", len(cfg.Peers),
		"expected_containers", len(cfg.ExpectedContainers),
		"bully_tcp", cfg.BullyTCPListenPort,
		"bully_udp", cfg.BullyUDPListenPort,
		"worker_udp", cfg.WorkerUDPListenPort,
	)

	// Graceful quit on SIGINT/SIGTERM.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	sig := <-signals
	slog.Info("Signal received, shutting down sentinel", "signal", sig.String())

	cancel()
	_ = ping.Close()
	_ = control.Close()
	wg.Wait()
	return 0
}

func main() {
	os.Exit(run())
}

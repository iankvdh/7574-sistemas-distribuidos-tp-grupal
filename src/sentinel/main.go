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

	hb, err := bully.NewHeartbeatTransport(cfg.SentinelHBListenPort)
	if err != nil {
		slog.Error("While binding bully UDP (hb) transport", "addr", cfg.SentinelHBListenPort, "err", err)
		return 1
	}
	control, err := bully.NewControlTransport(cfg.BullyTCPListenPort, cfg.ControlDialTimeout, cfg.ControlIOTimeout)
	if err != nil {
		slog.Error("While binding bully TCP (control) transport", "addr", cfg.BullyTCPListenPort, "err", err)
		_ = hb.Close()
		return 1
	}

	coordinator := bully.NewBullyCoordinator(bully.Config{
		SelfID:             cfg.SelfID,
		Peers:              cfg.Peers,
		LeaderCheckInterval: cfg.LeaderCheckInterval,
		LeaderTimeout:      cfg.SentinelPeerTimeout,
		OKTimeout:          cfg.OKTimeout,
		CoordTimeout:       cfg.CoordTimeout,
		BaseJitter:         cfg.BaseJitter,
	}, control)

	restarter := monitor.NewDockerRestarter(cfg.RestartTimeout, cfg.RestartStopGrace)
	mon, err := monitor.NewNodeMonitor(monitor.Config{
		ListenAddr:           cfg.WorkerUDPListenPort,
		ExpectedContainers:   cfg.ExpectedContainers,
		StartupGrace:         cfg.StartupGrace,
		HeartbeatTimeout:     cfg.HeartbeatTimeout,
		DetectionInterval:    cfg.DetectionInterval,
		RestartCooldown:      cfg.RestartCooldown,
		SentinelPeers:             cfg.SentinelPeers,
		SentinelPeerTimeout:       cfg.SentinelPeerTimeout,
		SentinelPeerGrace:         cfg.SentinelPeerGrace,
		SentinelPeerCooldown:      cfg.SentinelPeerCooldown,
		SentinelDetectionInterval: cfg.SentinelDetectionInterval,
	}, coordinator, restarter, nil, coordinator)
	if err != nil {
		slog.Error("While binding worker heartbeat listener", "addr", cfg.WorkerUDPListenPort, "err", err)
		_ = hb.Close()
		_ = control.Close()
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	wg.Add(1)
	go func() { defer wg.Done(); control.Serve(coordinator.Handle) }()
	wg.Add(1)
	go func() {
		defer wg.Done()
		hb.Serve(cfg.SelfID, coordinator.RecordPeerHeartbeat)
	}()

	wg.Add(1)
	go func() { defer wg.Done(); coordinator.Run(ctx) }()

	wg.Add(1)
	go func() { defer wg.Done(); coordinator.EmitPeerHeartbeats(ctx, hb, cfg.SentinelHBInterval) }()

	wg.Add(1)
	go func() { defer wg.Done(); mon.Run(ctx) }()

	slog.Info("Sentinel started",
		"peers", len(cfg.Peers),
		"expected_containers", len(cfg.ExpectedContainers),
		"bully_tcp", cfg.BullyTCPListenPort,
		"bully_udp", cfg.SentinelHBListenPort,
		"worker_udp", cfg.WorkerUDPListenPort,
	)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	sig := <-signals
	slog.Info("Signal received, shutting down sentinel", "signal", sig.String())

	cancel()
	_ = hb.Close()
	_ = control.Close()
	wg.Wait()
	return 0
}

func main() {
	os.Exit(run())
}

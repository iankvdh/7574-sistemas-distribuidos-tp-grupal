package monitor

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type Restarter interface {
	Restart(name string) error
}

type Leadership interface {
	IsLeader() bool
}

type PeerTracker interface {
	PeersLastSeen() map[byte]time.Time
}

type SentinelPeerSpec struct {
	ID        byte
	Container string
}

type Config struct {
	ListenAddr         string
	ExpectedContainers []string
	StartupGrace       time.Duration
	HeartbeatTimeout   time.Duration
	DetectionInterval  time.Duration
	RestartCooldown    time.Duration

	SentinelPeers             []SentinelPeerSpec
	SentinelPeerTimeout       time.Duration
	SentinelPeerGrace         time.Duration
	SentinelPeerCooldown      time.Duration
	SentinelDetectionInterval time.Duration
}

type NodeMonitor struct {
	cfg              Config
	bullyCoordinator Leadership
	restarter        Restarter
	clock            Clock
	mu               sync.Mutex
	lastSeen         map[string]time.Time
	knownContainers  map[string]bool
	lastRestarted    map[string]time.Time
	startupDeadline  time.Time
	hbListener       *heartbeatListener

	peerTracker            PeerTracker
	sentinelPeers          map[byte]string
	sentinelPeerGraceUntil time.Time
	deadPeers              map[byte]bool
}

func NewNodeMonitor(cfg Config, bullyCoordinator Leadership, restarter Restarter, clock Clock, peerTracker PeerTracker) (*NodeMonitor, error) {
	if clock == nil {
		clock = realClock{}
	}
	hbListener, err := newHeartbeatListener(cfg.ListenAddr)
	if err != nil {
		return nil, err
	}
	nm := &NodeMonitor{
		cfg:              cfg,
		bullyCoordinator: bullyCoordinator,
		restarter:        restarter,
		clock:            clock,
		lastSeen:         map[string]time.Time{},
		knownContainers:  map[string]bool{},
		lastRestarted:    map[string]time.Time{},
		startupDeadline:  clock.Now().Add(cfg.StartupGrace),
		hbListener:       hbListener,
		peerTracker:      peerTracker,
	}
	for _, name := range cfg.ExpectedContainers {
		if name != "" {
			nm.knownContainers[name] = true
		}
	}
	if peerTracker != nil && len(cfg.SentinelPeers) > 0 {
		nm.sentinelPeers = make(map[byte]string, len(cfg.SentinelPeers))
		nm.deadPeers = make(map[byte]bool, len(cfg.SentinelPeers))
		for _, sp := range cfg.SentinelPeers {
			nm.sentinelPeers[sp.ID] = sp.Container
		}
		nm.sentinelPeerGraceUntil = clock.Now().Add(cfg.SentinelPeerGrace)
	}
	return nm, nil
}

func (nm *NodeMonitor) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		nm.hbListener.listen(ctx, nm.recordHeartbeat)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		nm.detectionLoop(ctx)
	}()

	if nm.peerTracker != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nm.sentinelPeerLoop(ctx)
		}()
	}

	<-ctx.Done()
	_ = nm.hbListener.close()
	wg.Wait()
}

func (nm *NodeMonitor) recordHeartbeat(containerName string) {
	nm.mu.Lock()
	nm.lastSeen[containerName] = nm.clock.Now()
	nm.knownContainers[containerName] = true
	nm.mu.Unlock()
}

func (nm *NodeMonitor) detectionLoop(ctx context.Context) {
	ticker := time.NewTicker(nm.cfg.DetectionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nm.checkContainers()
		}
	}
}

func (nm *NodeMonitor) checkContainers() {
	if !nm.bullyCoordinator.IsLeader() {
		return
	}
	now := nm.clock.Now()
	var toRestart []string

	nm.mu.Lock()
	for name := range nm.knownContainers {
		lastHB, seen := nm.lastSeen[name]
		switch {
		case !seen:
			if !now.After(nm.startupDeadline) {
				continue
			}
		case now.Sub(lastHB) <= nm.cfg.HeartbeatTimeout:
			continue
		}
		if last, ok := nm.lastRestarted[name]; ok && now.Sub(last) < nm.cfg.RestartCooldown {
			slog.Debug("sentinel: cooldown active, skipping restart", "container", name)
			continue
		}
		nm.lastRestarted[name] = now
		toRestart = append(toRestart, name)
	}
	nm.mu.Unlock()

	for _, name := range toRestart {
		if err := nm.restarter.Restart(name); err != nil {
			slog.Error("sentinel: restart failed", "container", name, "err", err)
			continue
		}
		slog.Warn("sentinel: container restarted", "container", name)
	}
}

func (nm *NodeMonitor) sentinelPeerLoop(ctx context.Context) {
	interval := nm.cfg.SentinelDetectionInterval
	if interval <= 0 {
		interval = nm.cfg.DetectionInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !nm.bullyCoordinator.IsLeader() {
				continue
			}
			nm.checkSentinelPeers()
		}
	}
}

func (nm *NodeMonitor) checkSentinelPeers() {
	now := nm.clock.Now()
	if !now.After(nm.sentinelPeerGraceUntil) {
		return
	}
	peersLastSeen := nm.peerTracker.PeersLastSeen()

	var toRestart []string
	var cameBack []string
	nm.mu.Lock()
	for peerID, container := range nm.sentinelPeers {
		lastSeen, seen := peersLastSeen[peerID]
		if seen && now.Sub(lastSeen) <= nm.cfg.SentinelPeerTimeout {
			if nm.deadPeers[peerID] {
				nm.deadPeers[peerID] = false
				cameBack = append(cameBack, container)
			}
			continue
		}
		if last, ok := nm.lastRestarted[container]; ok && now.Sub(last) < nm.cfg.SentinelPeerCooldown {
			slog.Debug("sentinel: peer cooldown active, skipping restart", "peer_container", container)
			continue
		}
		nm.deadPeers[peerID] = true
		nm.lastRestarted[container] = now
		toRestart = append(toRestart, container)
	}
	nm.mu.Unlock()

	for _, name := range cameBack {
		slog.Info("sentinel: peer is back", "container", name)
	}
	for _, name := range toRestart {
		if err := nm.restarter.Restart(name); err != nil {
			slog.Error("sentinel: peer restart failed", "container", name, "err", err)
			continue
		}
		slog.Warn("sentinel: peer restarted", "container", name)
	}
}

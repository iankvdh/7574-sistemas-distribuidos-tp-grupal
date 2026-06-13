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

type Config struct {
	ListenAddr         string
	ExpectedContainers []string
	StartupGrace       time.Duration
	HeartbeatTimeout   time.Duration
	DetectionInterval  time.Duration
	RestartCooldown    time.Duration
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
}

func NewNodeMonitor(cfg Config, bullyCoordinator Leadership, restarter Restarter, clock Clock) (*NodeMonitor, error) {
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
	}
	for _, name := range cfg.ExpectedContainers {
		if name != "" {
			nm.knownContainers[name] = true
		}
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

// checkContainers is the detection pass. Only the leader restarts anything, but
// it relies entirely on the heartbeat map (never on Docker) to decide.
//
// It selects the restart candidates under the lock (recording the cooldown
// stamp atomically), then performs the potentially-slow docker restarts after
// releasing the lock, so the UDP listener keeps recording heartbeats meanwhile.
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
			slog.Warn("sentinel: cooldown active, skipping restart", "container", name)
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

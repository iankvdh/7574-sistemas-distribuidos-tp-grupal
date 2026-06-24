package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/logging"
)

func run() int {
	if err := logging.Init(); err != nil {
		slog.Error("While initializing logger", "err", err)
		return 1
	}

	targets := filterExcluded(
		env.SplitNonEmpty(env.StringWithDefault("TARGETS", "")),
		env.SplitNonEmpty(env.StringWithDefault("EXCLUDE", "")),
	)
	if len(targets) == 0 {
		slog.Error("chaos_monkey: no targets after applying EXCLUDE; nothing to do")
		return 1
	}

	interval, err := env.IntWithDefault("KILL_INTERVAL_SECONDS", 20, true)
	if err != nil {
		slog.Error("While loading chaos_monkey config", "err", err)
		return 1
	}
	killTimeoutSec, err := env.IntWithDefault("KILL_TIMEOUT_SECONDS", 10, true)
	if err != nil {
		slog.Error("While loading chaos_monkey config", "err", err)
		return 1
	}
	killTimeout := time.Duration(killTimeoutSec) * time.Second

	killCount, err := env.IntWithDefault("KILL_COUNT", 1, true)
	if err != nil {
		slog.Error("While loading chaos_monkey config", "err", err)
		return 1
	}

	sentinels := makeSet(env.SplitNonEmpty(env.StringWithDefault("SENTINELS", "")))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handleSignals(cancel)

	slog.Info("Chaos Monkey started",
		"targets", len(targets), "kill_count", killCount, "sentinels", len(sentinels),
		"kill_interval_seconds", interval, "kill_timeout_seconds", killTimeoutSec)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Chaos Monkey shutting down")
			return 0
		case <-ticker.C:
			victims := pickVictims(rng, targets, killCount, sentinels)
			for _, victim := range victims {
				if err := killContainer(ctx, victim, killTimeout); err != nil {
					slog.Warn("chaos_monkey: kill failed", "container", victim, "err", err)
					continue
				}
				slog.Info("chaos_monkey: killed", "container", victim)
			}
		}
	}
}

func pickVictims(rng *rand.Rand, targets []string, count int, sentinels map[string]struct{}) []string {
	if count > len(targets) {
		count = len(targets)
	}
	perm := rng.Perm(len(targets))[:count]

	out := make([]string, 0, count)
	sentinelChosen := false
	for _, idx := range perm {
		victim := targets[idx]
		if _, isSentinel := sentinels[victim]; isSentinel {
			if sentinelChosen {
				slog.Info("chaos_monkey: skipping extra sentinel this tick", "container", victim)
				continue
			}
			sentinelChosen = true
		}
		out = append(out, victim)
	}
	return out
}

func makeSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		set[v] = struct{}{}
	}
	return set
}

func killContainer(ctx context.Context, name string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "kill", name)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker kill %s: %w (stderr=%s)", name, err, stderr.String())
	}
	return nil
}

func filterExcluded(targets, exclude []string) []string {
	excluded := make(map[string]struct{}, len(exclude))
	for _, e := range exclude {
		excluded[e] = struct{}{}
	}
	out := make([]string, 0, len(targets))
	for _, t := range targets {
		if _, skip := excluded[t]; !skip {
			out = append(out, t)
		}
	}
	return out
}

func handleSignals(cancel context.CancelFunc) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	sig := <-signals
	slog.Info("Signal received, shutting down chaos_monkey", "signal", sig.String())
	cancel()
}

func main() {
	os.Exit(run())
}

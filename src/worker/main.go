package main

import (
	"log/slog"
	"os"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/runtime"

	// Register strategies via blank imports so factories install themselves at startup.
	_ "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/drain"
	_ "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/filter"
	_ "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/joiner"
	_ "github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/strategy/noop"
)

func run() int {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("While loading worker config", "err", err)
		return 1
	}

	slog.SetDefault(slog.Default().With("strategy", cfg.StrategyName, "replica_id", cfg.ReplicaID))

	w, err := runtime.New(cfg)
	if err != nil {
		slog.Error("While initializing worker", "err", err)
		return 1
	}

	if err := w.Run(); err != nil {
		slog.Error("Worker stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

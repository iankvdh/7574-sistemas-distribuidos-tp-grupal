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
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("While loading worker config", "err", err)
		return 1
	}

	w, err := runtime.New(cfg, logger.With("strategy", cfg.StrategyName, "replica_id", cfg.ReplicaID))
	if err != nil {
		logger.Error("While initializing worker", "err", err)
		return 1
	}

	if err := w.Run(); err != nil {
		logger.Error("Worker stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

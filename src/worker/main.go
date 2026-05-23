package main

import (
	"log/slog"
	"os"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/logging"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/worker/runtime"
)

func run() int {
	if err := logging.Init(); err != nil {
		slog.Error("While initializing logger", "err", err)
		return 1
	}

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

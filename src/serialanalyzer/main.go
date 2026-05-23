package main

import (
	"log/slog"
	"os"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/logging"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/serialanalyzer/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/serialanalyzer/serialanalyzer"
)

func run() int {
	if err := logging.Init(); err != nil {
		slog.Error("While initializing logger", "err", err)
		return 1
	}

	cfg, err := config.Load()
	if err != nil {
		slog.Error("While loading serial analyzer config", "err", err)
		return 1
	}

	analyzer, err := serialanalyzer.NewSerialAnalyzer(cfg)
	if err != nil {
		slog.Error("While initializing serial analyzer", "err", err)
		return 1
	}

	if err := analyzer.Run(); err != nil {
		slog.Error("Serial analyzer stopped with error", "err", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}

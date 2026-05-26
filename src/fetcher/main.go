package main

import (
	"log/slog"
	"os"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/logging"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/fetcher/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/fetcher/fetcher"
)

func run() int {
	if err := logging.Init(); err != nil {
		slog.Error("While initializing logger", "err", err)
		return 1
	}
	cfg, err := config.Load()
	if err != nil {
		slog.Error("While loading fetcher config", "err", err)
		return 1
	}
	if err := fetcher.New(cfg).Run(); err != nil {
		slog.Error("Fetcher stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

package main

import (
	"log/slog"
	"os"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/gateway/config"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/gateway/gateway"
)

func run() int {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("While loading gateway config", "err", err)
		return 1
	}

	server, err := gateway.NewGateway(cfg)
	if err != nil {
		slog.Error("While initializing gateway", "err", err)
		return 1
	}

	if err := server.Run(); err != nil {
		slog.Error("Gateway stopped with error", "err", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}

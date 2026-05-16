package main

import (
	"log/slog"
	"os"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/client/client"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/client/config"
)

func run() int {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("While loading client config", "err", err)
		return 1
	}

	newClient, err := client.NewClient(cfg)
	if err != nil {
		slog.Error("While connecting to gateway", "err", err)
		return 1
	}

	if err := newClient.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

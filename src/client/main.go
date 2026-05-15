package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/client/client"
)

func loadConfig() (client.ClientConfig, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	inputTransactions := os.Getenv("INPUT_TRANSACTIONS")
	if inputTransactions == "" {
		return client.ClientConfig{}, errors.New("INPUT_TRANSACTIONS environment variable is required")
	}

	inputAccounts := os.Getenv("INPUT_ACCOUNTS")
	if inputAccounts == "" {
		return client.ClientConfig{}, errors.New("INPUT_ACCOUNTS environment variable is required")
	}

	rawBatchSize := os.Getenv("BATCH_SIZE")
	if rawBatchSize == "" {
		return client.ClientConfig{}, errors.New("BATCH_SIZE environment variable is required")
	}
	batchSize, err := strconv.Atoi(rawBatchSize)
	if err != nil || batchSize <= 0 {
		return client.ClientConfig{}, errors.New("BATCH_SIZE must be a positive integer")
	}

	return client.ClientConfig{
		ServerHost:        serverHost,
		ServerPort:        serverPort,
		InputTransactions: inputTransactions,
		InputAccounts:     inputAccounts,
		BatchSize:         batchSize,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	new_client, err := client.NewClient(config)
	if err != nil {
		slog.Error("While connecting to server", "err", err)
		return 1
	}

	if err := new_client.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}

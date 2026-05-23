// Package logging configures the default slog logger from the LOG_LEVEL env var.
package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

const defaultLevel = slog.LevelInfo

// Init installs a text slog handler on stdout using the LOG_LEVEL env var
// ("debug", "info", "warn", "error"; case-insensitive). Unset falls back to info.
func Init() error {
	level, err := parseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		return err
	}
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))
	return nil
}

func parseLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return defaultLevel, nil
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid LOG_LEVEL: %q", raw)
	}
}

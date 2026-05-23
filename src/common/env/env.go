// Package env provides helpers for reading and parsing environment variables.
package env

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func RequiredString(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("%s environment variable is required", name)
	}
	return value, nil
}

func StringWithDefault(name, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return defaultValue
	}
	return value
}

func IntWithDefault(name string, defaultValue int, mustBePositive bool) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %s", name, raw)
	}
	if mustBePositive && parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func RequiredInt(name string, mustBePositive bool) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0, fmt.Errorf("%s environment variable is required", name)
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %s", name, raw)
	}
	if mustBePositive && parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

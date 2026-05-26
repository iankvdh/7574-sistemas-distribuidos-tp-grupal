package config

import (
	"fmt"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
)

const (
	defaultMomPort           = 5672
	defaultAPIURL            = "https://api.frankfurter.dev/v2"
	defaultQueuePrefix       = "conversions"
	defaultBaseCurrency      = "USD"
	defaultRequestTimeoutSec = 30
)

type FetcherConfig struct {
	MomHost           string
	MomPort           int
	APIURL            string
	BaseCurrency      string
	Period1Start      string // YYYY-MM-DD (ISO)
	Period1End        string // YYYY-MM-DD (ISO)
	QueuePrefix       string
	NMicroTxCounter   int
	GatewayID         int
	RequestTimeoutSec int
}

func Load() (FetcherConfig, error) {
	momHost, err := env.RequiredString("MOM_HOST")
	if err != nil {
		return FetcherConfig{}, err
	}
	momPort, err := env.IntWithDefault("MOM_PORT", defaultMomPort, true)
	if err != nil {
		return FetcherConfig{}, err
	}
	p1Start, err := env.RequiredString("PERIOD1_START")
	if err != nil {
		return FetcherConfig{}, err
	}
	p1End, err := env.RequiredString("PERIOD1_END")
	if err != nil {
		return FetcherConfig{}, err
	}
	p1StartISO, err := toISODate(p1Start)
	if err != nil {
		return FetcherConfig{}, fmt.Errorf("PERIOD1_START: %w", err)
	}
	p1EndISO, err := toISODate(p1End)
	if err != nil {
		return FetcherConfig{}, fmt.Errorf("PERIOD1_END: %w", err)
	}
	n, err := env.RequiredInt("N_MICRO_TX_COUNTER", true)
	if err != nil {
		return FetcherConfig{}, err
	}
	gatewayID, err := env.IntWithDefault("GATEWAY_ID", 1, true)
	if err != nil {
		return FetcherConfig{}, err
	}
	timeout, err := env.IntWithDefault("REQUEST_TIMEOUT_SEC", defaultRequestTimeoutSec, true)
	if err != nil {
		return FetcherConfig{}, err
	}

	return FetcherConfig{
		MomHost:           momHost,
		MomPort:           momPort,
		APIURL:            env.StringWithDefault("CONVERSIONS_API_URL", defaultAPIURL),
		BaseCurrency:      env.StringWithDefault("CONVERSIONS_BASE_CURRENCY", defaultBaseCurrency),
		Period1Start:      p1StartISO,
		Period1End:        p1EndISO,
		QueuePrefix:       env.StringWithDefault("CONVERSIONS_QUEUE_PREFIX", defaultQueuePrefix),
		NMicroTxCounter:   n,
		GatewayID:         gatewayID,
		RequestTimeoutSec: timeout,
	}, nil
}

func toISODate(yyyymmdd string) (string, error) {
	t, err := time.Parse("20060102", yyyymmdd)
	if err != nil {
		return "", fmt.Errorf("invalid date %q (expected YYYYMMDD): %w", yyyymmdd, err)
	}
	return t.Format("2006-01-02"), nil
}

package middleware

import (
	"fmt"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/env"
)

func NewConnSettings(host string, port int) (ConnSettings, error) {
	maxOutstanding, err := env.IntWithDefault("MAX_OUTSTANDING_PUBLISHES", DefaultMaxOutstandingPublishes, true)
	if err != nil {
		return ConnSettings{}, err
	}
	if maxOutstanding < PrefetchCount {
		return ConnSettings{}, fmt.Errorf(
			"MAX_OUTSTANDING_PUBLISHES (%d) must be >= PREFETCH_COUNT (%d)", maxOutstanding, PrefetchCount)
	}

	ratio, err := env.FloatWithDefault("PUBLISHER_FLUSH_HIGH_WATER_RATIO", DefaultFlushHighWaterRatio)
	if err != nil {
		return ConnSettings{}, err
	}
	if ratio <= 0 || ratio > 1 {
		return ConnSettings{}, fmt.Errorf("PUBLISHER_FLUSH_HIGH_WATER_RATIO must be in (0, 1], got %v", ratio)
	}

	heartbeatSeconds, err := env.IntWithDefault("AMQP_HEARTBEAT_SECONDS", DefaultHeartbeatSeconds, true)
	if err != nil {
		return ConnSettings{}, err
	}

	return ConnSettings{
		Hostname:                host,
		Port:                    port,
		MaxOutstandingPublishes: maxOutstanding,
		FlushHighWaterRatio:     ratio,
		HeartbeatSeconds:        heartbeatSeconds,
	}, nil
}

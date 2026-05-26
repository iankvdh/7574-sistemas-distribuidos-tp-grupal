package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/middleware"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/fetcher/config"
)

const frankfurterClientID inner.ClientID = "frankfurter"

type Fetcher struct {
	cfg config.FetcherConfig
}

func New(cfg config.FetcherConfig) *Fetcher {
	return &Fetcher{cfg: cfg}
}

func (f *Fetcher) Run() error {
	rates, err := f.fetchRates()
	if err != nil {
		return fmt.Errorf("fetch rates: %w", err)
	}
	slog.Info("Fetched conversion rates", "days", len(rates.Rates), "base", f.cfg.BaseCurrency)

	if err := f.publishToAllQueues(rates); err != nil {
		return fmt.Errorf("publish rates: %w", err)
	}
	slog.Info("Fetcher published rates to all conversions queues; exiting", "queues", f.cfg.NMicroTxCounter)
	return nil
}

func (f *Fetcher) fetchRates() (*inner.Q5Conversions, error) {
	request_url := fmt.Sprintf("%s/rates?from=%s&to=%s&base=%s", f.cfg.APIURL, f.cfg.Period1Start, f.cfg.Period1End, f.cfg.BaseCurrency)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(f.cfg.RequestTimeoutSec)*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, request_url, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", request_url, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return nil, fmt.Errorf("GET %s returned %d: %s", request_url, response.StatusCode, string(body))
	}

	var entries []struct {
		Date  string  `json:"date"`
		Quote string  `json:"quote"`
		Rate  float64 `json:"rate"`
	}
	if err := json.NewDecoder(response.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("decode frankfurter response: %w", err)
	}

	formatted_rates := &inner.Q5Conversions{Rates: make(map[uint32]map[string]float64)}
	for _, entry := range entries {
		key, err := fromISODate(entry.Date)
		if err != nil {
			slog.Warn("Skipping unparseable rate date", "date", entry.Date, "err", err)
			continue
		}
		if _, ok := formatted_rates.Rates[key]; !ok {
			formatted_rates.Rates[key] = make(map[string]float64)
		}
		formatted_rates.Rates[key][entry.Quote] = entry.Rate
	}
	return formatted_rates, nil
}

func (f *Fetcher) publishToAllQueues(rates *inner.Q5Conversions) error {
	payload, err := inner.SerializeQ5Conversions(rates)
	if err != nil {
		return fmt.Errorf("serialize Q5Conversions: %w", err)
	}
	conn := middleware.ConnSettings{Hostname: f.cfg.MomHost, Port: f.cfg.MomPort}
	gatewayID := inner.GatewayID(f.cfg.GatewayID)

	for i := 0; i < f.cfg.NMicroTxCounter; i++ {
		queueName := fmt.Sprintf("%s_%d", f.cfg.QueuePrefix, i)
		mw, err := middleware.CreateQueueMiddleware(queueName, conn)
		if err != nil {
			return fmt.Errorf("connect to %s: %w", queueName, err)
		}
		if err := f.publishOne(mw, gatewayID, payload); err != nil {
			_ = mw.Close()
			return fmt.Errorf("publish to %s: %w", queueName, err)
		}
		if err := mw.Close(); err != nil {
			slog.Warn("While closing conversions queue middleware", "queue", queueName, "err", err)
		}
	}
	return nil
}

func (f *Fetcher) publishOne(mw middleware.Middleware, gatewayID inner.GatewayID, payload []byte) error {
	batch, err := inner.SerializeInnerBatch(5, inner.Q5ConversionsItem, gatewayID, frankfurterClientID, [][]byte{payload})
	if err != nil {
		return err
	}
	if err := mw.Send(*batch); err != nil {
		return err
	}
	eof, err := inner.SerializeInternalEOF(gatewayID, frankfurterClientID, 1, 5)
	if err != nil {
		return err
	}
	return mw.Send(*eof)
}

func fromISODate(iso string) (uint32, error) {
	if len(iso) != 10 || iso[4] != '-' || iso[7] != '-' {
		return 0, fmt.Errorf("invalid ISO date %q", iso)
	}
	n, err := strconv.ParseUint(iso[0:4]+iso[5:7]+iso[8:10], 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(n), nil
}

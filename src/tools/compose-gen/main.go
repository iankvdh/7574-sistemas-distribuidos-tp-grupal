package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type workerSpec struct {
	Name       string
	Strategy   string
	Replicas   int
	Inputs     []string
	MatchCount int
	Outputs    []string
	Env        map[string]string
	Volumes    []string
}

type fetcherSpec struct {
	Name string
	Env  map[string]string
}

type chaosSpec struct {
	Exclude     []string
	TargetsOnly []string // if non-empty, restrict chaos targets to this list
	Env         map[string]string
}

type spec struct {
	Gateways          int
	Sentinels         int
	Transactions      string
	Accounts          string
	Dataset           string
	LogLevel          string
	ExpectedQueryEOFs int
	Workers           []workerSpec
	Fetchers          []fetcherSpec
	ChaosMonkey       *chaosSpec
	SentinelsEnv      map[string]string
}

func datasetFromTransactions(transactions string) string {
	base := filepath.Base(transactions)
	if idx := strings.Index(base, "LI-"); idx >= 0 {
		rest := base[idx+3:]
		if end := strings.IndexByte(rest, '_'); end >= 0 {
			return strings.ToLower(rest[:end])
		}
	}
	return "default"
}

const defaultLogLevel = "info"

var strategiesWithRing = map[string]struct{}{
	"filter_period1":                    {},
	"filter_wire_ach":                   {},
	"filter_currency_usd_p1":            {},
	"filter_currency_usd_p2":            {},
	"filter_currency_usd_other_periods": {},
	"filter_period2":                    {},
	"filter_amount_lt_50":               {},
	"joiner_usd":                        {},
	"sharder_q4":                        {},
	"sharder_q1":                        {},
	"max_q2":                            {},
	"bank_aggregator":                   {},
	"sum_q3":                            {},
	"filter_q3":                         {},
	"micro_transaction_counter":         {},
}

func strategyUsesRing(strategy string) bool {
	_, ok := strategiesWithRing[strategy]
	return ok
}

var stageTypeByStrategy = map[string]string{
	"filter_period1":                    "FilterPeriod1",
	"filter_wire_ach":                   "FilterWireACH",
	"filter_currency_usd_p1":            "FilterCurrencyUsdP1",
	"filter_period2":                    "FilterPeriod2",
	"filter_currency_usd_p2":            "FilterCurrencyUsdP2",
	"filter_currency_usd_other_periods": "FilterCurrencyUsdOther",
	"joiner_usd":                        "JoinerUSD",
	"filter_amount_lt_50":               "FilterAmountLt50",
	"sharder_q1":                        "SharderQ1",
	"sharder_q4":                        "SharderQ4",
	"suspicious_account_filter":         "SuspiciousFilter",
	"path_finder_q4":                    "PathFinder",
	"counter_q4":                        "CounterQ4",
	"max_q2":                            "MaxQ2",
	"bank_aggregator":                   "BankAggregator",
	"aggregator_q2":                     "AggregatorQ2",
	"sum_q3":                            "SumQ3",
	"aggregator_q3":                     "AggregatorQ3",
	"filter_q3":                         "FilterQ3",
	"micro_transaction_counter":         "MicroTransactionCounter",
	"aggregator_q5":                     "AggregatorQ5",
	"final_joiner":                      "FinalJoiner",
}

func stageTypeFor(strategy string) string {
	stageType, ok := stageTypeByStrategy[strategy]
	if !ok {
		panic(fmt.Sprintf("compose-gen: no STAGE_TYPE mapping for strategy %q (add it to stageTypeByStrategy)", strategy))
	}
	return stageType
}

const workerDataDir = "/data"

func workerDataVolume(serviceName string) string {
	return "data_" + serviceName
}

const sharedNetworkName = "tp_grupal_network"

const (
	defaultSentinelReplicas = 3
	sentinelTCPPort         = 8090
	sentinelUDPPort         = 8092
	sentinelWorkerPort      = 8091
	sentinelStartGrace      = 90
	heartbeatIntervalS      = 5
	heartbeatJitterMs       = 1000
	dockerSocketMount       = "/var/run/docker.sock:/var/run/docker.sock"
)

func sentinelName(i int) string { return fmt.Sprintf("sentinel_%d", i) }

func sentinelHeartbeatTargets(replicas int) string {
	parts := make([]string, 0, replicas)
	for i := 0; i < replicas; i++ {
		parts = append(parts, fmt.Sprintf("%s:%d", sentinelName(i), sentinelWorkerPort))
	}
	return strings.Join(parts, ",")
}

var datasetFilePaths = map[string][2]string{
	"small":  {"/datasets/LI-Small_Trans.csv", "/datasets/LI-Small_accounts.csv"},
	"medium": {"/datasets/LI-Medium_Trans.csv", "/datasets/LI-Medium_accounts.csv"},
	"large":  {"/datasets/LI-Large_Trans.csv", "/datasets/LI-Large_accounts.csv"},
}

func main() {
	if len(os.Args) < 2 || len(os.Args) > 5 {
		fmt.Fprintln(os.Stderr, "usage: compose-gen <spec.yaml> [server [DATASET] | client [LABEL [DATASET]]]")
		os.Exit(2)
	}
	body, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
	s, err := parseSpec(string(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse: %v\n", err)
		os.Exit(1)
	}
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: compose-gen <spec.yaml> server [DATASET] | client [LABEL [DATASET]]")
		os.Exit(2)
	}
	mode := os.Args[2]
	clientLabel := "0"
	datasetOverride := ""
	switch mode {
	case "server":
		if len(os.Args) >= 4 {
			datasetOverride = os.Args[3]
		}
	case "client":
		if len(os.Args) >= 4 {
			clientLabel = os.Args[3]
		}
		if len(os.Args) >= 5 {
			datasetOverride = os.Args[4]
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q; expected server or client\n", mode)
		os.Exit(2)
	}
	if paths, ok := datasetFilePaths[datasetOverride]; ok {
		s.Transactions = paths[0]
		s.Accounts = paths[1]
		s.Dataset = datasetOverride
	} else if datasetOverride != "" {
		fmt.Fprintf(os.Stderr, "unknown dataset %q; expected small, medium or large\n", datasetOverride)
		os.Exit(2)
	}
	var renderErr error
	switch mode {
	case "server":
		renderErr = s.renderServer(os.Stdout)
	case "client":
		renderErr = s.renderClient(os.Stdout, clientLabel)
	}
	if renderErr != nil {
		fmt.Fprintf(os.Stderr, "render: %v\n", renderErr)
		os.Exit(1)
	}
}

func parseSpec(src string) (*spec, error) {
	s := &spec{Gateways: 1}
	var cur *workerSpec
	var curFetcher *fetcherSpec
	var section, currentList string
	flushWorker := func() {
		if cur != nil {
			s.Workers = append(s.Workers, *cur)
			cur = nil
		}
	}
	flushFetcher := func() {
		if curFetcher != nil {
			s.Fetchers = append(s.Fetchers, *curFetcher)
			curFetcher = nil
		}
	}
	for _, raw := range strings.Split(src, "\n") {
		line := strings.TrimRight(raw, " \r")
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " "))
		trimmed := strings.TrimSpace(line)

		switch {
		case indent == 0:
			flushWorker()
			flushFetcher()
			key, value := splitKV(trimmed)
			switch key {
			case "gateways":
				s.Gateways, _ = strconv.Atoi(value)
			case "sentinels":
				s.Sentinels, _ = strconv.Atoi(value)
			case "transactions":
				s.Transactions = value
			case "accounts":
				s.Accounts = value
			case "dataset":
				s.Dataset = value
			case "log_level":
				s.LogLevel = value
			case "expected_query_eofs":
				s.ExpectedQueryEOFs, _ = strconv.Atoi(value)
			case "chaos_monkey":
				s.ChaosMonkey = &chaosSpec{Env: map[string]string{}}
			case "sentinels_env":
				if s.SentinelsEnv == nil {
					s.SentinelsEnv = map[string]string{}
				}
			}
			section = key
			currentList = ""
		case section == "workers" && indent == 2 && strings.HasPrefix(trimmed, "- "):
			flushWorker()
			cur = &workerSpec{Replicas: 1, Env: map[string]string{}}
			currentList = ""
			rest := strings.TrimPrefix(trimmed, "- ")
			if k, v := splitKV(rest); k != "" {
				applyWorkerField(cur, k, v)
			}
		case section == "workers" && indent == 4:
			currentList = ""
			key, value := splitKV(trimmed)
			if value == "" && (key == "outputs" || key == "env" || key == "volumes" || key == "inputs") {
				currentList = key
				continue
			}
			applyWorkerField(cur, key, value)
		case section == "workers" && indent >= 6:
			if cur == nil {
				continue
			}
			if strings.HasPrefix(trimmed, "- ") {
				value := strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
				switch currentList {
				case "outputs":
					cur.Outputs = append(cur.Outputs, value)
				case "inputs":
					cur.Inputs = append(cur.Inputs, value)
				case "volumes":
					cur.Volumes = append(cur.Volumes, value)
				}
				continue
			}
			if currentList == "env" {
				key, value := splitKV(trimmed)
				if key != "" {
					cur.Env[key] = value
				}
			}
		case section == "fetchers" && indent == 2 && strings.HasPrefix(trimmed, "- "):
			flushFetcher()
			curFetcher = &fetcherSpec{Env: map[string]string{}}
			currentList = ""
			rest := strings.TrimPrefix(trimmed, "- ")
			if k, v := splitKV(rest); k != "" && k == "name" {
				curFetcher.Name = v
			}
		case section == "fetchers" && indent == 4:
			currentList = ""
			key, value := splitKV(trimmed)
			if value == "" && key == "env" {
				currentList = key
				continue
			}
			if key == "name" {
				curFetcher.Name = value
			}
		case section == "fetchers" && indent >= 6:
			if curFetcher == nil {
				continue
			}
			if currentList == "env" {
				key, value := splitKV(trimmed)
				if key != "" {
					curFetcher.Env[key] = value
				}
			}
		case section == "chaos_monkey" && indent == 2:
			currentList = ""
			key, value := splitKV(trimmed)
			if value == "" && (key == "env" || key == "exclude" || key == "targets_only") {
				currentList = key
			} else if key == "exclude" {
				s.ChaosMonkey.Exclude = appendCSVValues(s.ChaosMonkey.Exclude, value)
			}
		case section == "chaos_monkey" && indent >= 4:
			if s.ChaosMonkey == nil {
				continue
			}
			if (currentList == "exclude" || currentList == "targets_only") && strings.HasPrefix(trimmed, "- ") {
				value := strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
				if value != "" {
					if currentList == "exclude" {
						s.ChaosMonkey.Exclude = append(s.ChaosMonkey.Exclude, value)
					} else {
						s.ChaosMonkey.TargetsOnly = append(s.ChaosMonkey.TargetsOnly, value)
					}
				}
				continue
			}
			if currentList == "env" {
				key, value := splitKV(trimmed)
				if key != "" {
					s.ChaosMonkey.Env[key] = value
				}
			}
		case section == "sentinels_env" && indent >= 2:
			key, value := splitKV(trimmed)
			if key != "" && s.SentinelsEnv != nil {
				s.SentinelsEnv[key] = value
			}
		}
	}
	flushWorker()
	flushFetcher()
	return s, nil
}

func applyWorkerField(ws *workerSpec, key, value string) {
	if ws == nil {
		return
	}
	switch key {
	case "name":
		ws.Name = value
	case "strategy":
		ws.Strategy = value
	case "replicas":
		ws.Replicas, _ = strconv.Atoi(value)
	case "input":
		// Legacy single-input form. Stored as a singleton slice so the renderer
		// can treat single- and multi-input workers uniformly.
		ws.Inputs = []string{value}
	case "match_count":
		ws.MatchCount, _ = strconv.Atoi(value)
	}
}

func splitKV(line string) (string, string) {
	idx := strings.Index(line, ":")
	if idx == -1 {
		return strings.TrimSpace(line), ""
	}
	key := strings.TrimSpace(line[:idx])
	value := stripInlineComment(strings.TrimSpace(line[idx+1:]))
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
			value = value[1 : len(value)-1]
		}
	}
	return key, value
}

func appendCSVValues(values []string, raw string) []string {
	for _, part := range strings.Split(raw, ",") {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}
	return values
}

func stripInlineComment(s string) string {
	if s == "" {
		return s
	}
	var quote byte
	if s[0] == '"' || s[0] == '\'' {
		quote = s[0]
	}
	inQuote := quote != 0
	for i := 0; i < len(s); i++ {
		if inQuote {
			if s[i] == quote {
				inQuote = false
			}
			continue
		}
		if s[i] == '#' && (i == 0 || s[i-1] == ' ' || s[i-1] == '\t') {
			return strings.TrimRight(s[:i], " \t")
		}
	}
	return s
}

func injectDerivedEnvs(workers []workerSpec, fetchers []fetcherSpec) {
	r := func(name string) int {
		for _, w := range workers {
			if w.Name == name {
				return w.Replicas
			}
		}
		return 0
	}
	itoa := strconv.Itoa
	set := func(env map[string]string, key, val string) {
		if _, ok := env[key]; !ok {
			env[key] = val
		}
	}

	joinerUSDUpstreams := 0
	for _, w := range workers {
		for _, o := range w.Outputs {
			if strings.Contains(o, "joiner_usd_input") {
				joinerUSDUpstreams++
				break
			}
		}
	}

	for i := range workers {
		env := workers[i].Env
		switch workers[i].Strategy {
		case "sharder_q1":
			if n := r("final_joiner"); n > 0 {
				set(env, "N_FINAL_JOINERS", itoa(n))
			}
		case "max_q2":
			if n := r("aggregator_q2"); n > 0 {
				set(env, "K_AGGREGATORS_Q2", itoa(n))
			}
		case "bank_aggregator":
			if n := r("aggregator_q2"); n > 0 {
				set(env, "K_AGGREGATORS_Q2", itoa(n))
			}
		case "aggregator_q2":
			if n := r("max_q2") + r("bank_aggregator"); n > 0 {
				set(env, "EXPECTED_PARTIAL_EOFS", itoa(n))
			}
			if n := r("final_joiner"); n > 0 {
				set(env, "N_FINAL_JOINERS", itoa(n))
			}
		case "sum_q3":
			if n := r("aggregator_q3"); n > 0 {
				set(env, "K_AGGREGATORS_Q3", itoa(n))
			}
		case "aggregator_q3":
			if n := r("sum_q3"); n > 0 {
				set(env, "EXPECTED_PARTIAL_EOFS", itoa(n))
			}
			if n := r("filter_q3"); n > 0 {
				set(env, "N_FILTER_Q3", itoa(n))
			}
		case "filter_q3":
			if n := r("final_joiner"); n > 0 {
				set(env, "N_FINAL_JOINERS", itoa(n))
			}
		case "sharder_q4":
			if n := r("suspicious_account_filter"); n > 0 {
				set(env, "K_SUSPICIOUS_FILTERS", itoa(n))
			}
		case "suspicious_account_filter":
			if n := r("sharder_q4"); n > 0 {
				set(env, "N_SHARDERS", itoa(n))
			}
			if n := r("path_finder_q4"); n > 0 {
				set(env, "K_PATH_FINDERS", itoa(n))
			}
		case "path_finder_q4":
			if n := r("suspicious_account_filter"); n > 0 {
				set(env, "N_SUSPICIOUS_FILTERS", itoa(n))
			}
			if n := r("counter_q4"); n > 0 {
				set(env, "K_COUNTERS", itoa(n))
			}
		case "counter_q4":
			if n := r("path_finder_q4"); n > 0 {
				set(env, "N_PATH_FINDERS", itoa(n))
			}
			if n := r("final_joiner"); n > 0 {
				set(env, "N_FINAL_JOINERS", itoa(n))
			}
		case "micro_transaction_counter":
			if n := r("aggregator_q5"); n > 0 {
				set(env, "N_AGGREGATOR_Q5", itoa(n))
			}
		case "aggregator_q5":
			if n := r("micro_transaction_counter"); n > 0 {
				set(env, "N_MICRO_TX_COUNTER", itoa(n))
			}
			if n := r("final_joiner"); n > 0 {
				set(env, "N_FINAL_JOINERS", itoa(n))
			}
		case "joiner_usd":
			if joinerUSDUpstreams > 0 {
				set(env, "EXPECTED_EOFS", itoa(joinerUSDUpstreams))
			}
		case "final_joiner":
			if n := r("sharder_q1"); n > 0 {
				set(env, "EXPECTED_EOFS_Q1", itoa(n))
			}
			if n := r("aggregator_q2"); n > 0 {
				set(env, "EXPECTED_EOFS_Q2", itoa(n))
			}
			if n := r("filter_q3"); n > 0 {
				set(env, "EXPECTED_EOFS_Q3", itoa(n))
			}
			if n := r("counter_q4"); n > 0 {
				set(env, "EXPECTED_EOFS_Q4", itoa(n))
			}
			if r("aggregator_q5") > 0 {
				set(env, "EXPECTED_EOFS_Q5", "1")
			}
		}
	}

	for i := range fetchers {
		switch fetchers[i].Name {
		case "fetcher_q5":
			if n := r("micro_transaction_counter"); n > 0 {
				set(fetchers[i].Env, "N_MICRO_TX_COUNTER", itoa(n))
			}
		}
	}
}

func expandOutputs(outputs []string, gateways int) []string {
	result := make([]string, 0, len(outputs))
	for _, o := range outputs {
		if o == "final_queues" {
			for i := 1; i <= gateways; i++ {
				result = append(result, fmt.Sprintf("final_queue:final_%d", i))
			}
		} else {
			result = append(result, o)
		}
	}
	return result
}

func (s *spec) resolveParams() (transactions, accounts, dataset, logLevel string) {
	transactions = s.Transactions
	if transactions == "" {
		transactions = "/datasets/LI-Small_Trans.csv"
	}
	accounts = s.Accounts
	if accounts == "" {
		accounts = "/datasets/LI-Small_accounts.csv"
	}
	dataset = s.Dataset
	if dataset == "" {
		dataset = datasetFromTransactions(transactions)
	}
	logLevel = s.LogLevel
	if logLevel == "" {
		logLevel = defaultLogLevel
	}
	return
}

func (s *spec) renderServer(w io.Writer) error {
	fmt.Fprintln(w, "services:")
	_, _, _, logLevel := s.resolveParams()
	sentinels := s.Sentinels
	if sentinels <= 0 {
		sentinels = defaultSentinelReplicas
	}
	nTxShards := shardCountForExchange(s.Workers, "all_transactions")
	nAcctShards := shardCountForExchange(s.Workers, "all_accounts")
	var gatewayNames []string
	for i := 1; i <= s.Gateways; i++ {
		writeGateway(w, i, logLevel, s.ExpectedQueryEOFs, sentinels, nTxShards, nAcctShards)
		gatewayNames = append(gatewayNames, gatewayName(i))
	}
	injectDerivedEnvs(s.Workers, s.Fetchers)
	var workerNames []string
	for _, ws := range s.Workers {
		for r := 0; r < ws.Replicas; r++ {
			writeWorker(w, ws, r, logLevel, s.Gateways, sentinels)
			workerNames = append(workerNames, serviceName(ws, r))
		}
	}
	for _, fs := range s.Fetchers {
		writeFetcher(w, fs, logLevel)
	}
	var sentinelNames []string
	for i := 0; i < sentinels; i++ {
		sentinelNames = append(sentinelNames, sentinelName(i))
	}
	var fetcherNames []string
	for _, fs := range s.Fetchers {
		fetcherNames = append(fetcherNames, fs.Name)
	}
	monitored := append(append([]string{}, workerNames...), gatewayNames...)
	chaosTargets := append(append(append([]string{}, monitored...), sentinelNames...), fetcherNames...)
	writeSentinels(w, monitored, logLevel, sentinels, s.SentinelsEnv)
	if s.ChaosMonkey != nil {
		finalTargets := chaosTargets
		if len(s.ChaosMonkey.TargetsOnly) > 0 {
			allowed := make(map[string]bool, len(s.ChaosMonkey.TargetsOnly))
			for _, t := range s.ChaosMonkey.TargetsOnly {
				allowed[t] = true
			}
			filtered := finalTargets[:0:0]
			for _, t := range chaosTargets {
				if allowed[t] {
					filtered = append(filtered, t)
				}
			}
			finalTargets = filtered
		}
		writeChaosMonkey(w, s.ChaosMonkey, finalTargets, logLevel)
	}
	writeRabbit(w)
	writeNetwork(w, false)
	writeWorkerVolumes(w, workerNames)
	return nil
}

func writeWorkerVolumes(w io.Writer, workerNames []string) {
	if len(workerNames) == 0 {
		return
	}
	fmt.Fprintln(w, "volumes:")
	for _, name := range workerNames {
		fmt.Fprintf(w, "  %s:\n", workerDataVolume(name))
	}
}

func writeSentinels(w io.Writer, monitored []string, logLevel string, replicas int, extraEnv map[string]string) {
	expected := strings.Join(monitored, ",")
	var extraKeys []string
	for k := range extraEnv {
		extraKeys = append(extraKeys, k)
	}
	sort.Strings(extraKeys)
	for i := 0; i < replicas; i++ {
		var peers []string
		for j := 0; j < replicas; j++ {
			if j == i {
				continue
			}
			peers = append(peers, fmt.Sprintf("%d:%s:%d:%d", j, sentinelName(j), sentinelTCPPort, sentinelUDPPort))
		}
		fmt.Fprintf(w, "  %s:\n", sentinelName(i))
		fmt.Fprintln(w, "    build: { context: ./src/, dockerfile: sentinel/Dockerfile }")
		fmt.Fprintf(w, "    container_name: %s\n", sentinelName(i))
		fmt.Fprintln(w, "    environment:")
		fmt.Fprintf(w, "      - SENTINEL_ID=%d\n", i)
		fmt.Fprintf(w, "      - PEERS=%s\n", strings.Join(peers, ","))
		fmt.Fprintf(w, "      - EXPECTED_CONTAINERS=%s\n", expected)
		fmt.Fprintf(w, "      - SENTINEL_BULLY_TCP_PORT=%d\n", sentinelTCPPort)
		fmt.Fprintf(w, "      - SENTINEL_BULLY_UDP_PORT=%d\n", sentinelUDPPort)
		fmt.Fprintf(w, "      - SENTINEL_UDP_PORT=%d\n", sentinelWorkerPort)
		fmt.Fprintf(w, "      - STARTUP_GRACE_SECONDS=%d\n", sentinelStartGrace)
		fmt.Fprintf(w, "      - LOG_LEVEL=%s\n", logLevel)
		for _, k := range extraKeys {
			fmt.Fprintf(w, "      - %s=%s\n", k, extraEnv[k])
		}
		fmt.Fprintln(w, "    volumes:")
		fmt.Fprintf(w, "      - %s\n", dockerSocketMount)
	}
}

func (s *spec) renderClient(w io.Writer, clientLabel string) error {
	fmt.Fprintln(w, "services:")
	transactions, accounts, dataset, logLevel := s.resolveParams()
	writeClientSplit(w, clientLabel, s.Gateways, transactions, accounts, dataset, logLevel, s.ExpectedQueryEOFs)
	writeNetwork(w, true)
	return nil
}

func writeNetwork(w io.Writer, external bool) {
	fmt.Fprintln(w, "networks:")
	fmt.Fprintln(w, "  default:")
	fmt.Fprintf(w, "    name: %s\n", sharedNetworkName)
	if external {
		fmt.Fprintln(w, "    external: true")
	}
}

func serviceName(ws workerSpec, replica int) string {
	if ws.Replicas == 1 {
		return ws.Name
	}
	return fmt.Sprintf("%s_%d", ws.Name, replica)
}

func writeClientSplit(w io.Writer, label string, gateways int, transactions, accounts, dataset, logLevel string, expectedQueryEOFs int) {
	fmt.Fprintln(w, "  client:")
	fmt.Fprintln(w, "    build: { context: ./src/, dockerfile: client/Dockerfile }")
	fmt.Fprintln(w, "    env_file:")
	fmt.Fprintln(w, "      - .env")
	fmt.Fprintln(w, "    environment:")
	fmt.Fprintf(w, "      - INPUT_TRANSACTIONS=%s\n", transactions)
	fmt.Fprintf(w, "      - INPUT_ACCOUNTS=%s\n", accounts)
	fmt.Fprintln(w, "      - GATEWAY_PREFIX=gateway_")
	fmt.Fprintf(w, "      - GATEWAY_AMOUNT=%d\n", gateways)
	fmt.Fprintln(w, "      - GATEWAY_PORT=5678")
	fmt.Fprintln(w, "      - CONNECT_MAX_ATTEMPTS=6")
	fmt.Fprintln(w, "      - CONNECT_TIMEOUT_MS=800")
	fmt.Fprintln(w, "      - BACKOFF_BASE_MS=200")
	fmt.Fprintln(w, "      - BACKOFF_MAX_MS=3000")
	fmt.Fprintln(w, "      - RESULTS_DIR=/results")
	if expectedQueryEOFs > 0 {
		fmt.Fprintf(w, "      - EXPECTED_QUERY_EOFS=%d\n", expectedQueryEOFs)
	}
	fmt.Fprintf(w, "      - LOG_LEVEL=%s\n", logLevel)
	fmt.Fprintln(w, "    volumes:")
	fmt.Fprintln(w, "      - ./datasets:/datasets")
	fmt.Fprintf(w, "      - ./results/%s/client_%s:/results\n", dataset, label)
}

func gatewayName(id int) string { return fmt.Sprintf("gateway_%d", id) }

func shardCountForExchange(workers []workerSpec, exchange string) int {
	for _, ws := range workers {
		for _, in := range ws.Inputs {
			parts := strings.Split(in, ":")
			if len(parts) >= 3 && parts[0] == "bound_queue" && parts[2] == exchange {
				if ws.Replicas > 0 {
					return ws.Replicas
				}
				return 1
			}
		}
	}
	return 1
}

func writeGateway(w io.Writer, id int, logLevel string, expectedQueryEOFs, sentinels, nTxShards, nAcctShards int) {
	name := gatewayName(id)
	fmt.Fprintf(w, "  %s:\n", name)
	fmt.Fprintln(w, "    build: { context: ./src/, dockerfile: gateway/Dockerfile }")
	fmt.Fprintf(w, "    container_name: %s\n", name)
	fmt.Fprintln(w, "    depends_on: { rabbitmq: { condition: service_healthy } }")
	fmt.Fprintln(w, "    env_file:")
	fmt.Fprintln(w, "      - .env")
	fmt.Fprintln(w, "    environment:")
	fmt.Fprintln(w, "      - ALL_TRANSACTIONS_EXCHANGE=all_transactions")
	fmt.Fprintln(w, "      - ALL_ACCOUNTS_EXCHANGE=all_accounts")
	fmt.Fprintf(w, "      - N_TX_SHARDS=%d\n", nTxShards)
	fmt.Fprintf(w, "      - N_ACCOUNT_SHARDS=%d\n", nAcctShards)
	fmt.Fprintln(w, "      - FINAL_QUEUE=final")
	fmt.Fprintf(w, "      - GATEWAY_ID=%d\n", id)
	fmt.Fprintln(w, "      - SERVER_HOST=0.0.0.0")
	fmt.Fprintln(w, "      - SERVER_PORT=5678")
	if expectedQueryEOFs > 0 {
		fmt.Fprintf(w, "      - EXPECTED_QUERY_EOFS=%d\n", expectedQueryEOFs)
	}
	fmt.Fprintf(w, "      - LOG_LEVEL=%s\n", logLevel)
	fmt.Fprintf(w, "      - CONTAINER_NAME=%s\n", name)
	fmt.Fprintf(w, "      - SENTINEL_UDP=%s\n", sentinelHeartbeatTargets(sentinels))
	fmt.Fprintf(w, "      - HEARTBEAT_INTERVAL_SECONDS=%d\n", heartbeatIntervalS)
	fmt.Fprintf(w, "      - HEARTBEAT_JITTER_MS=%d\n", heartbeatJitterMs)
}

func writeWorker(w io.Writer, ws workerSpec, replica int, logLevel string, gateways, sentinels int) {
	name := serviceName(ws, replica)
	fmt.Fprintf(w, "  %s:\n", name)
	fmt.Fprintln(w, "    build: { context: ./src/, dockerfile: worker/Dockerfile }")
	fmt.Fprintf(w, "    container_name: %s\n", name)
	fmt.Fprintln(w, "    depends_on: { rabbitmq: { condition: service_healthy } }")
	fmt.Fprintln(w, "    env_file:")
	fmt.Fprintln(w, "      - .env")
	fmt.Fprintln(w, "    environment:")
	fmt.Fprintf(w, "      - STRATEGY=%s\n", ws.Strategy)
	fmt.Fprintf(w, "      - STAGE_TYPE=%s\n", stageTypeFor(ws.Strategy))
	fmt.Fprintf(w, "      - REPLICA_ID=%d\n", replica)
	fmt.Fprintf(w, "      - N_REPLICAS=%d\n", ws.Replicas)
	fmt.Fprintf(w, "      - DATA_DIR=%s\n", workerDataDir)
	if len(ws.Inputs) == 1 {
		input := strings.ReplaceAll(ws.Inputs[0], "{REPLICA_ID}", strconv.Itoa(replica))
		fmt.Fprintf(w, "      - INPUT=%s\n", input)
	} else {
		for j, raw := range ws.Inputs {
			input := strings.ReplaceAll(raw, "{REPLICA_ID}", strconv.Itoa(replica))
			fmt.Fprintf(w, "      - INPUT_%d=%s\n", j, input)
		}
	}
	outputs := expandOutputs(ws.Outputs, gateways)
	fmt.Fprintf(w, "      - OUTPUTS_MATCH_COUNT=%d\n", ws.MatchCount)
	for j, o := range outputs {
		fmt.Fprintf(w, "      - OUTPUT_%d=%s\n", j, o)
	}
	fmt.Fprintf(w, "      - LOG_LEVEL=%s\n", logLevel)
	fmt.Fprintf(w, "      - CONTAINER_NAME=%s\n", name)
	fmt.Fprintf(w, "      - SENTINEL_UDP=%s\n", sentinelHeartbeatTargets(sentinels))
	fmt.Fprintf(w, "      - HEARTBEAT_INTERVAL_SECONDS=%d\n", heartbeatIntervalS)
	fmt.Fprintf(w, "      - HEARTBEAT_JITTER_MS=%d\n", heartbeatJitterMs)
	if ws.Replicas > 1 && strategyUsesRing(ws.Strategy) {
		ringIn := fmt.Sprintf("ring_%s_%d", ws.Name, replica)
		ringOut := fmt.Sprintf("ring_%s_%d", ws.Name, (replica+1)%ws.Replicas)
		fmt.Fprintf(w, "      - RING_QUEUE_IN=%s\n", ringIn)
		fmt.Fprintf(w, "      - RING_QUEUE_OUT=%s\n", ringOut)
	}
	if len(ws.Env) > 0 {
		keys := make([]string, 0, len(ws.Env))
		for k := range ws.Env {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "      - %s=%s\n", k, ws.Env[k])
		}
	}
	fmt.Fprintln(w, "    volumes:")
	fmt.Fprintf(w, "      - %s:%s\n", workerDataVolume(name), workerDataDir)
	for _, v := range ws.Volumes {
		fmt.Fprintf(w, "      - %s\n", v)
	}
}

func writeFetcher(w io.Writer, fs fetcherSpec, logLevel string) {
	fmt.Fprintf(w, "  %s:\n", fs.Name)
	fmt.Fprintln(w, "    build: { context: ./src/, dockerfile: fetcher/Dockerfile }")
	fmt.Fprintf(w, "    container_name: %s\n", fs.Name)
	fmt.Fprintln(w, "    depends_on: { rabbitmq: { condition: service_healthy } }")
	fmt.Fprintln(w, "    env_file:")
	fmt.Fprintln(w, "      - .env")
	fmt.Fprintln(w, "    environment:")
	fmt.Fprintf(w, "      - LOG_LEVEL=%s\n", logLevel)
	if len(fs.Env) > 0 {
		keys := make([]string, 0, len(fs.Env))
		for k := range fs.Env {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "      - %s=%s\n", k, fs.Env[k])
		}
	}
}

func writeChaosMonkey(w io.Writer, cs *chaosSpec, targets []string, logLevel string) {
	exclude := []string{"rabbitmq"}
	exclude = appendUnique(exclude, cs.Exclude...)

	fmt.Fprintln(w, "  chaos_monkey:")
	fmt.Fprintln(w, "    build: { context: ./src/, dockerfile: chaos_monkey/Dockerfile }")
	fmt.Fprintln(w, "    container_name: chaos_monkey")
	fmt.Fprintln(w, "    environment:")
	fmt.Fprintf(w, "      - TARGETS=%s\n", strings.Join(targets, ","))
	fmt.Fprintf(w, "      - EXCLUDE=%s\n", strings.Join(exclude, ","))
	fmt.Fprintf(w, "      - LOG_LEVEL=%s\n", logLevel)
	if len(cs.Env) > 0 {
		keys := make([]string, 0, len(cs.Env))
		for k := range cs.Env {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "      - %s=%s\n", k, cs.Env[k])
		}
	}
	fmt.Fprintln(w, "    volumes:")
	fmt.Fprintf(w, "      - %s\n", dockerSocketMount)
}

func appendUnique(values []string, extras ...string) []string {
	seen := make(map[string]struct{}, len(values)+len(extras))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	for _, value := range extras {
		if _, ok := seen[value]; ok {
			continue
		}
		values = append(values, value)
		seen[value] = struct{}{}
	}
	return values
}

func writeRabbit(w io.Writer) {
	fmt.Fprintln(w, "  rabbitmq:")
	fmt.Fprintln(w, "    build: { context: ./src/rabbitmq, dockerfile: Dockerfile }")
	fmt.Fprintln(w, "    volumes:")
	fmt.Fprintln(w, "      - ./src/rabbitmq/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro")
	fmt.Fprintln(w, "    healthcheck:")
	fmt.Fprintln(w, "      interval: 5s")
	fmt.Fprintln(w, "      retries: 10")
	fmt.Fprintln(w, "      start_period: 50s")
	fmt.Fprintln(w, "      test: rabbitmq-diagnostics check_port_connectivity")
	fmt.Fprintln(w, "      timeout: 3s")
	fmt.Fprintln(w, "    ports:")
	fmt.Fprintln(w, "      - 5672:5672")
	fmt.Fprintln(w, "      - 15672:15672")
}

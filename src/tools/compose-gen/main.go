// compose-gen renders a docker-compose.yaml from a tiny declarative spec.
//
// Usage:
//
//	compose-gen <spec.yaml> server [DATASET]          → server compose (gateways+workers+rabbit)
//	compose-gen <spec.yaml> client [LABEL [DATASET]]  → single-client compose
//
// DATASET is one of: small, medium, large (default: small).
// LABEL   is the client identifier used for the results directory (default: 0).
//
// Spec format (whitespace-significant, YAML-subset; no external deps):
//
//	gateways: <int>          how many gateway_N services to declare
//	log_level: <string>      optional; LOG_LEVEL emitted to every service (default: info)
//	expected_query_eofs: <int>  number of query EOFs the gateway/client expect (default: 5)
//	workers:
//	  - name: <prefix>                   docker-compose service name (suffixed by replica index if replicas>1)
//	    strategy: <STRATEGY env value>   which strategy this worker runs
//	    replicas: <int>                  how many replicas; ring queues are auto-derived when >1
//	    input: <kind>:<target>[:<key>]   queue:NAME | direct_exchange:NAME[:KEY]
//	    match_count: <int>               optional; how many of `outputs` are "match" (filters use it; others omit)
//	    outputs:                         ordered list; first match_count are match, rest are no-match
//	      - <kind>:<target>[:<key>]
//	    env:                             optional arbitrary env vars (EXPECTED_EOFS, WINDOW_SIZE, ...)
//	      KEY: value
//
// All worker and gateway services automatically load .env (repo root) so that
// shared variables (MOM_HOST, MOM_PORT, PERIOD1_START, …) are defined once.
//
// New worker families (reducer, aggregator, joiner, ...) plug in by simply
// registering a STRATEGY in the worker binary and adding a block here — the
// generator itself is family-agnostic.
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
	Volumes    []string // optional, mounted on every replica of the worker
}

type fetcherSpec struct {
	Name string
	Env  map[string]string
}

type spec struct {
	Gateways          int
	Transactions      string
	Accounts          string
	Dataset           string // optional; auto-derived from Transactions if empty
	LogLevel          string
	ExpectedQueryEOFs int // 0 means "leave gateway/client defaults" (= 5)
	Workers           []workerSpec
	Fetchers          []fetcherSpec
}

// datasetFromTransactions extracts a lowercase dataset name from a transactions
// path like "/datasets/LI-Small_Trans.csv" → "small".
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

// strategiesWithRing lista las strategies que coordinan EOFs con un anillo
// entre réplicas (RING_QUEUE_IN/OUT). Sólo se generan ring queues para estas
// strategies cuando replicas > 1. Si la strategy no usa anillo (path_finder_q4,
// counter_q4, drain, noop, ...) las ring queues se omiten incluso con varias
// réplicas, evitando colas que nadie consume.
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

// sharedNetworkName is the explicit Docker network that links the server and
// client compose projects when running in split mode.
const sharedNetworkName = "tp_grupal_network"

// datasetFilePaths maps short dataset names (small, medium, large) to their
// transactions and accounts CSV paths inside the container.
var datasetFilePaths = map[string][2]string{
	"small":  {"/datasets/LI-Small_Trans.csv", "/datasets/LI-Small_accounts.csv"},
	"medium": {"/datasets/LI-Medium_Trans.csv", "/datasets/LI-Medium_accounts.csv"},
	"large":  {"/datasets/LI-Large_Trans.csv", "/datasets/LI-Large_accounts.csv"},
}

func main() {
	// usage: compose-gen <spec.yaml> [server [DATASET] | client [LABEL [DATASET]]]
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

// parseSpec is a tiny YAML-subset parser tailored to the format documented above.
// It handles indentation-based scoping but only the constructs the spec uses.
//
// Indentation contract (whitespace-significant):
//
//	0 :  top-level scalars + "workers:"
//	2 :  "- name: ..." starts a new worker
//	4 :  worker fields (strategy, replicas, input, match_count) and the
//	     section headers "outputs:" / "env:"
//	6 :  "- <value>" items of outputs, "KEY: value" entries of env
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
	// Strip optional quoting (env vars often need to be quoted in YAML to keep
	// the parser happy with values like "3" or "*").
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
			value = value[1 : len(value)-1]
		}
	}
	return key, value
}

// stripInlineComment drops YAML-style trailing comments (" # ..." or "\t# ...")
// while leaving '#' characters that are part of quoted values or that aren't
// preceded by whitespace untouched. Without this, a line like
//
//	EXPECTED_EOFS: "3"   # = N_REPLICAS de sharder_q1
//
// would yield value = `"3"   # = N_REPLICAS de sharder_q1` and the worker
// would later fail to parse it as an integer.
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

// injectDerivedEnvs auto-populates all cross-reference env vars (N_FINAL_JOINERS,
// K_AGGREGATORS_Q2, EXPECTED_PARTIAL_EOFS, EXPECTED_EOFS_Q*, etc.) from replica
// counts so specs only need to declare domain-logic params (thresholds, dirs, …).
// Values already set in the spec take precedence (explicit override).
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

	// Count distinct worker types that feed into joiner_usd (each ring-worker
	// emits exactly 1 EOF downstream regardless of replica count).
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

// expandOutputs replaces the magic token "final_queues" with one
// final_queue:final_<i> entry per gateway so the final_joiner always has
// exactly as many outputs as gateways, without manual bookkeeping in specs.
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

// renderServer writes a compose file with all services except clients.
// It creates the shared Docker network so the client compose can attach to it.
func (s *spec) renderServer(w io.Writer) error {
	fmt.Fprintln(w, "services:")
	_, _, _, logLevel := s.resolveParams()
	for i := 1; i <= s.Gateways; i++ {
		writeGateway(w, i, logLevel, s.ExpectedQueryEOFs)
	}
	injectDerivedEnvs(s.Workers, s.Fetchers)
	for _, ws := range s.Workers {
		for r := 0; r < ws.Replicas; r++ {
			writeWorker(w, ws, r, logLevel, s.Gateways)
		}
	}
	for _, fs := range s.Fetchers {
		writeFetcher(w, fs, logLevel)
	}
	writeRabbit(w)
	writeNetwork(w, false)
	return nil
}

// renderClient writes a compose file with only the client services.
// It joins the shared Docker network created by the server compose.
// No depends_on references to server services — the client has its own retry
// logic (CONNECT_MAX_ATTEMPTS / BACKOFF_BASE_MS) to wait for the gateway.
func (s *spec) renderClient(w io.Writer, clientLabel string) error {
	fmt.Fprintln(w, "services:")
	transactions, accounts, dataset, logLevel := s.resolveParams()
	writeClientSplit(w, clientLabel, s.Gateways, transactions, accounts, dataset, logLevel, s.ExpectedQueryEOFs)
	writeNetwork(w, true)
	return nil
}

// writeNetwork appends the networks section that pins the default network to
// sharedNetworkName. external=true is used by the client compose (joins an
// existing network); external=false is used by the server compose (creates it).
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

// writeClientSplit is like writeClient but omits depends_on entirely — used in
// the split-compose client file where all server services are in another project.
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

func writeGateway(w io.Writer, id int, logLevel string, expectedQueryEOFs int) {
	fmt.Fprintf(w, "  gateway_%d:\n", id)
	fmt.Fprintln(w, "    build: { context: ./src/, dockerfile: gateway/Dockerfile }")
	fmt.Fprintln(w, "    depends_on: { rabbitmq: { condition: service_healthy } }")
	fmt.Fprintln(w, "    env_file:")
	fmt.Fprintln(w, "      - .env")
	fmt.Fprintln(w, "    environment:")
	fmt.Fprintln(w, "      - ALL_TRANSACTIONS_QUEUE=all_transactions")
	fmt.Fprintln(w, "      - ALL_ACCOUNTS_QUEUE=all_accounts")
	fmt.Fprintln(w, "      - FINAL_QUEUE=final")
	fmt.Fprintf(w, "      - GATEWAY_ID=%d\n", id)
	fmt.Fprintln(w, "      - SERVER_HOST=0.0.0.0")
	fmt.Fprintln(w, "      - SERVER_PORT=5678")
	if expectedQueryEOFs > 0 {
		fmt.Fprintf(w, "      - EXPECTED_QUERY_EOFS=%d\n", expectedQueryEOFs)
	}
	fmt.Fprintf(w, "      - LOG_LEVEL=%s\n", logLevel)
}

func writeWorker(w io.Writer, ws workerSpec, replica int, logLevel string, gateways int) {
	fmt.Fprintf(w, "  %s:\n", serviceName(ws, replica))
	fmt.Fprintln(w, "    build: { context: ./src/, dockerfile: worker/Dockerfile }")
	fmt.Fprintln(w, "    depends_on: { rabbitmq: { condition: service_healthy } }")
	fmt.Fprintln(w, "    env_file:")
	fmt.Fprintln(w, "      - .env")
	fmt.Fprintln(w, "    environment:")
	fmt.Fprintf(w, "      - STRATEGY=%s\n", ws.Strategy)
	fmt.Fprintf(w, "      - REPLICA_ID=%d\n", replica)
	fmt.Fprintf(w, "      - N_REPLICAS=%d\n", ws.Replicas)
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
	if ws.Replicas > 1 && strategyUsesRing(ws.Strategy) {
		ringIn := fmt.Sprintf("ring_%s_%d", ws.Name, replica)
		ringOut := fmt.Sprintf("ring_%s_%d", ws.Name, (replica+1)%ws.Replicas)
		fmt.Fprintf(w, "      - RING_QUEUE_IN=%s\n", ringIn)
		fmt.Fprintf(w, "      - RING_QUEUE_OUT=%s\n", ringOut)
	}
	// Deterministic output order for extra env vars so the diff between two
	// generated composes only reflects real spec changes.
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
	if len(ws.Volumes) > 0 {
		fmt.Fprintln(w, "    volumes:")
		for _, v := range ws.Volumes {
			fmt.Fprintf(w, "      - %s\n", v)
		}
	}
}

func writeFetcher(w io.Writer, fs fetcherSpec, logLevel string) {
	fmt.Fprintf(w, "  %s:\n", fs.Name)
	fmt.Fprintln(w, "    build: { context: ./src/, dockerfile: fetcher/Dockerfile }")
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

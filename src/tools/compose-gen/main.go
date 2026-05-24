// compose-gen renders a docker-compose.yaml from a tiny declarative spec.
//
// Usage:
//   compose-gen <spec.yaml>          → writes the compose to stdout
//
// Spec format (whitespace-significant, YAML-subset; no external deps):
//
//	clients: <int>           how many client_N services to declare
//	gateways: <int>          how many gateway_N services to declare
//	log_level: <string>      optional; LOG_LEVEL emitted to every service (default: info)
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
	"sort"
	"strconv"
	"strings"
)

type workerSpec struct {
	Name       string
	Strategy   string
	Replicas   int
	Input      string
	MatchCount int
	Outputs    []string
	Env        map[string]string
	Volumes    []string // optional, mounted on every replica of the worker
}

type spec struct {
	Clients      int
	Gateways     int
	Transactions string
	Accounts     string
	LogLevel     string
	Workers      []workerSpec
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
}

func strategyUsesRing(strategy string) bool {
	_, ok := strategiesWithRing[strategy]
	return ok
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: compose-gen <spec.yaml>")
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
	if err := s.render(os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "render: %v\n", err)
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
	s := &spec{Clients: 1, Gateways: 1}
	var cur *workerSpec
	var section, currentList string
	for _, raw := range strings.Split(src, "\n") {
		line := strings.TrimRight(raw, " \r")
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " "))
		trimmed := strings.TrimSpace(line)

		switch {
		case indent == 0:
			key, value := splitKV(trimmed)
			switch key {
			case "clients":
				s.Clients, _ = strconv.Atoi(value)
			case "gateways":
				s.Gateways, _ = strconv.Atoi(value)
			case "transactions":
				s.Transactions = value
			case "accounts":
				s.Accounts = value
			case "log_level":
				s.LogLevel = value
			}
			section = key
			currentList = ""
		case section == "workers" && indent == 2 && strings.HasPrefix(trimmed, "- "):
			if cur != nil {
				s.Workers = append(s.Workers, *cur)
			}
			cur = &workerSpec{Replicas: 1, Env: map[string]string{}}
			currentList = ""
			rest := strings.TrimPrefix(trimmed, "- ")
			if k, v := splitKV(rest); k != "" {
				applyWorkerField(cur, k, v)
			}
		case section == "workers" && indent == 4:
			currentList = ""
			key, value := splitKV(trimmed)
			if value == "" && (key == "outputs" || key == "env" || key == "volumes") {
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
		}
	}
	if cur != nil {
		s.Workers = append(s.Workers, *cur)
	}
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
		ws.Input = value
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
//   EXPECTED_EOFS: "3"   # = N_REPLICAS de sharder_q1
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

func (s *spec) render(w io.Writer) error {
	fmt.Fprintln(w, "services:")

	// Build the list of every worker service the clients should wait for.
	dependencies := make([]string, 0)
	for _, ws := range s.Workers {
		for r := 0; r < ws.Replicas; r++ {
			dependencies = append(dependencies, serviceName(ws, r))
		}
	}

	transactions := s.Transactions
	if transactions == "" {
		transactions = "/datasets/LI-Small_Trans.csv"
	}
	accounts := s.Accounts
	if accounts == "" {
		accounts = "/datasets/LI-Small_accounts.csv"
	}
	logLevel := s.LogLevel
	if logLevel == "" {
		logLevel = defaultLogLevel
	}
	for i := 0; i < s.Clients; i++ {
		writeClient(w, i, s.Gateways, dependencies, transactions, accounts, logLevel)
	}
	for i := 1; i <= s.Gateways; i++ {
		writeGateway(w, i, logLevel)
	}
	for _, ws := range s.Workers {
		for r := 0; r < ws.Replicas; r++ {
			writeWorker(w, ws, r, logLevel)
		}
	}
	writeRabbit(w)
	return nil
}

func serviceName(ws workerSpec, replica int) string {
	if ws.Replicas == 1 {
		return ws.Name
	}
	return fmt.Sprintf("%s_%d", ws.Name, replica)
}

func writeClient(w io.Writer, idx, gateways int, deps []string, transactions, accounts, logLevel string) {
	fmt.Fprintf(w, "  client_%d:\n", idx)
	fmt.Fprintln(w, "    build: { context: ./src/, dockerfile: client/Dockerfile }")
	fmt.Fprintln(w, "    depends_on:")
	fmt.Fprintln(w, "      rabbitmq: { condition: service_healthy }")
	for i := 1; i <= gateways; i++ {
		fmt.Fprintf(w, "      gateway_%d: { condition: service_started }\n", i)
	}
	for _, d := range deps {
		fmt.Fprintf(w, "      %s: { condition: service_started }\n", d)
	}
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
	fmt.Fprintf(w, "      - LOG_LEVEL=%s\n", logLevel)
	fmt.Fprintln(w, "    volumes:")
	fmt.Fprintln(w, "      - ./datasets:/datasets")
	fmt.Fprintf(w, "      - ./results/client_%d:/results\n", idx)
}

func writeGateway(w io.Writer, id int, logLevel string) {
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
	fmt.Fprintf(w, "      - LOG_LEVEL=%s\n", logLevel)
}

func writeWorker(w io.Writer, ws workerSpec, replica int, logLevel string) {
	fmt.Fprintf(w, "  %s:\n", serviceName(ws, replica))
	fmt.Fprintln(w, "    build: { context: ./src/, dockerfile: worker/Dockerfile }")
	fmt.Fprintln(w, "    depends_on: { rabbitmq: { condition: service_healthy } }")
	fmt.Fprintln(w, "    env_file:")
	fmt.Fprintln(w, "      - .env")
	fmt.Fprintln(w, "    environment:")
	fmt.Fprintf(w, "      - STRATEGY=%s\n", ws.Strategy)
	fmt.Fprintf(w, "      - REPLICA_ID=%d\n", replica)
	fmt.Fprintf(w, "      - N_REPLICAS=%d\n", ws.Replicas)
	input := strings.ReplaceAll(ws.Input, "{REPLICA_ID}", strconv.Itoa(replica))
	fmt.Fprintf(w, "      - INPUT=%s\n", input)
	fmt.Fprintf(w, "      - OUTPUTS_MATCH_COUNT=%d\n", ws.MatchCount)
	for j, o := range ws.Outputs {
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

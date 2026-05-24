package config

import "testing"

func TestParseInputQueue(t *testing.T) {
	b, err := ParseInput("queue:all_transactions")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Kind != KindQueue || b.Name != "all_transactions" {
		t.Fatalf("unexpected outputConfig: %+v", b)
	}
}

func TestParseInputDirectExchangeWithoutKey(t *testing.T) {
	b, err := ParseInput("direct_exchange:period1_transactions")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Kind != KindDirectExchange || b.Name != "period1_transactions" || b.RoutingKey != "" {
		t.Fatalf("unexpected outputConfig: %+v", b)
	}
}

func TestParseInputDirectExchangeWithKey(t *testing.T) {
	b, err := ParseInput("direct_exchange:usd_sharded:client-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Kind != KindDirectExchange || b.Name != "usd_sharded" || b.RoutingKey != "client-1" {
		t.Fatalf("unexpected outputConfig: %+v", b)
	}
}

func TestParseInputRejectsShardedQueues(t *testing.T) {
	if _, err := ParseInput("sharded_queues:joiner_usd_input:4"); err == nil {
		t.Fatalf("expected error: sharded_queues is not valid as input")
	}
}

func TestParseInputErrors(t *testing.T) {
	cases := []string{"", "queue:", "direct_exchange:", "foo:bar", "no_separator"}
	for _, raw := range cases {
		if _, err := ParseInput(raw); err == nil {
			t.Fatalf("expected error for input %q", raw)
		}
	}
}

func TestParseOutputConfigShardedQueues(t *testing.T) {
	b, err := parseOutputConfig("sharded_queues:joiner_usd_input:4", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Kind != KindShardedQueues || b.Name != "joiner_usd_input" || b.ShardCount != 4 {
		t.Fatalf("unexpected outputConfig: %+v", b)
	}
}

func TestParseOutputConfigShardedQueuesInvalid(t *testing.T) {
	cases := []string{
		"sharded_queues:joiner_usd_input",   // missing K
		"sharded_queues:joiner_usd_input:0", // K must be >0
		"sharded_queues::4",                 // empty prefix
		"sharded_queues:prefix:abc",         // non-numeric K
	}
	for _, raw := range cases {
		if _, err := parseOutputConfig(raw, 0); err == nil {
			t.Fatalf("expected error for %q", raw)
		}
	}
}

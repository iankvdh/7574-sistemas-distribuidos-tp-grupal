package client

import (
	"reflect"
	"testing"
	"time"
)

func TestBuildGatewayAddresses(t *testing.T) {
	config := ClientConfig{
		GatewayPrefix: "tp_grupal-gateway-",
		GatewayAmount: 3,
		GatewayPort:   "5678",
	}

	got := buildGatewayAddresses(config)
	want := []string{
		"tp_grupal-gateway-1:5678",
		"tp_grupal-gateway-2:5678",
		"tp_grupal-gateway-3:5678",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected gateway addresses:\n got: %v\nwant: %v", got, want)
	}
}

func TestComputeBackoffCap(t *testing.T) {
	base := 200 * time.Millisecond
	max := 3 * time.Second

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 200 * time.Millisecond},
		{attempt: 2, want: 400 * time.Millisecond},
		{attempt: 3, want: 800 * time.Millisecond},
		{attempt: 4, want: 1600 * time.Millisecond},
		{attempt: 5, want: 3 * time.Second},
		{attempt: 6, want: 3 * time.Second},
	}

	for _, tc := range tests {
		got := computeBackoffCap(base, max, tc.attempt)
		if got != tc.want {
			t.Fatalf("attempt %d: got %s want %s", tc.attempt, got, tc.want)
		}
	}
}

package hashing

import (
	"fmt"
	"testing"
)

func TestShard_Deterministic(t *testing.T) {
	cases := []string{"client-1", "client-2", "abc", "", "11111111-2222-3333-4444-555555555555"}
	for _, c := range cases {
		got1 := Shard(c, 4)
		got2 := Shard(c, 4)
		if got1 != got2 {
			t.Fatalf("Shard(%q,4) not deterministic: %d vs %d", c, got1, got2)
		}
	}
}

func TestShard_InRange(t *testing.T) {
	for _, K := range []int{1, 2, 3, 4, 8, 16} {
		for i := 0; i < 1000; i++ {
			s := Shard(fmt.Sprintf("client-%d", i), K)
			if s < 0 || s >= K {
				t.Fatalf("Shard out of range for K=%d: %d", K, s)
			}
		}
	}
}

func TestShard_KLessOrEqualOne(t *testing.T) {
	if Shard("anything", 1) != 0 {
		t.Fatalf("Shard with K=1 must return 0")
	}
	if Shard("anything", 0) != 0 {
		t.Fatalf("Shard with K=0 must return 0")
	}
}

func TestShard_BasicDistribution(t *testing.T) {
	K := 4
	counts := make([]int, K)
	N := 10000
	for i := 0; i < N; i++ {
		counts[Shard(fmt.Sprintf("client-%d", i), K)]++
	}
	min := N / K / 2
	for i, c := range counts {
		if c < min {
			t.Fatalf("degenerate distribution: bucket %d got %d (expected >= %d)", i, c, min)
		}
	}
}

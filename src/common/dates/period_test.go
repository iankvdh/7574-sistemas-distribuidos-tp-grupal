package dates

import "testing"

func TestInPeriod1(t *testing.T) {
	cases := []struct {
		date uint32
		want bool
	}{
		{20220831, false},
		{20220901, true},
		{20220903, true},
		{20220905, true},
		{20220906, false},
		{20220915, false},
		{20220916, false},
	}
	for _, c := range cases {
		if got := InPeriod1(c.date); got != c.want {
			t.Fatalf("InPeriod1(%d) = %v, want %v", c.date, got, c.want)
		}
	}
}

func TestInPeriod2(t *testing.T) {
	cases := []struct {
		date uint32
		want bool
	}{
		{20220831, false},
		{20220905, false},
		{20220906, true},
		{20220910, true},
		{20220915, true},
		{20220916, false},
	}
	for _, c := range cases {
		if got := InPeriod2(c.date); got != c.want {
			t.Fatalf("InPeriod2(%d) = %v, want %v", c.date, got, c.want)
		}
	}
}

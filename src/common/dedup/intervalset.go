package dedup

import (
	"encoding/json"
	"sort"
)

type IntervalSet struct {
	ranges []rng
}

type rng struct {
	Lo uint64
	Hi uint64
}

func (s *IntervalSet) Contains(id uint64) bool {
	i := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].Hi >= id })
	return i < len(s.ranges) && s.ranges[i].Lo <= id
}

func (s *IntervalSet) Add(id uint64) bool {
	i := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].Hi >= id })
	if i < len(s.ranges) && s.ranges[i].Lo <= id {
		return false
	}
	mergePrev := i > 0 && s.ranges[i-1].Hi != ^uint64(0) && s.ranges[i-1].Hi+1 == id
	mergeNext := i < len(s.ranges) && id != ^uint64(0) && s.ranges[i].Lo == id+1
	switch {
	case mergePrev && mergeNext:
		s.ranges[i-1].Hi = s.ranges[i].Hi
		s.ranges = append(s.ranges[:i], s.ranges[i+1:]...)
	case mergePrev:
		s.ranges[i-1].Hi = id
	case mergeNext:
		s.ranges[i].Lo = id
	default:
		s.ranges = append(s.ranges, rng{})
		copy(s.ranges[i+1:], s.ranges[i:])
		s.ranges[i] = rng{Lo: id, Hi: id}
	}
	return true
}

func (s *IntervalSet) IsEmpty() bool { return len(s.ranges) == 0 }

func (s *IntervalSet) NumRanges() int { return len(s.ranges) }

func (s *IntervalSet) MarshalJSON() ([]byte, error) {
	out := make([][2]uint64, len(s.ranges))
	for i, r := range s.ranges {
		out[i] = [2]uint64{r.Lo, r.Hi}
	}
	return json.Marshal(out)
}

func (s *IntervalSet) UnmarshalJSON(data []byte) error {
	var in [][2]uint64
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	s.ranges = make([]rng, len(in))
	for i, p := range in {
		s.ranges[i] = rng{Lo: p[0], Hi: p[1]}
	}
	return nil
}

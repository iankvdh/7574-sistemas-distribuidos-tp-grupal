package hashing

import "hash/fnv"

func Shard(clientID string, K int) int {
	if K <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(clientID))
	return int(h.Sum32() % uint32(K))
}

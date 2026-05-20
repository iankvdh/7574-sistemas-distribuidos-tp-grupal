package hashing

import "hash/fnv"

// Shard returns fnv1a(clientID) % K. Producers writing to the same set of K
// queues must use this function so messages for a given client always land on
// the same queue.
func Shard(clientID string, K int) int {
	if K <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(clientID))
	return int(h.Sum32() % uint32(K))
}

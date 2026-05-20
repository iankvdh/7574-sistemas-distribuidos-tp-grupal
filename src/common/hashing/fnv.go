package hashing

import "hash/fnv"

// Shard devuelve el shard al que pertenece clientID, calculado como
// fnv1a(clientID) % K. Es determinístico y debe usarse por TODOS los
// productores que escriban a un conjunto de K queues nombradas, de
// modo que los datos y EOFs de un mismo cliente caigan siempre en la
// misma queue (y por ende sean consumidos por la misma réplica).
//
// Mismo patrón que el TP2 (Sum → K Aggregators por hash de fruta).
func Shard(clientID string, K int) int {
	if K <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(clientID))
	return int(h.Sum32() % uint32(K))
}

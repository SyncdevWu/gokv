package hash

const prime32 = uint32(16777619)

func Fnv32(key string) int32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return int32(hash)
}

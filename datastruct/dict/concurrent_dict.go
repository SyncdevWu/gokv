package dict

import (
	"go.uber.org/zap"
	"gokv/interface/datastruct"
	"gokv/lib/hash"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
)

const (
	minMapSize = 16
)

type ConcurrentHashDict struct {
	table      []*shard // 分段
	count      int32    // key总数
	shardCount int32    // 分段数量 相当于并发度 一个分段会有一把锁
}

type shard struct {
	m     map[string]any // golang内置的map
	mutex sync.RWMutex   // 读写锁
}

func computeCapacity(size int32) int32 {
	if size <= minMapSize {
		return minMapSize
	}
	n := size - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

func NewConcurrentHashDict(shardCount int32) *ConcurrentHashDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := int32(0); i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]any),
		}
	}
	return &ConcurrentHashDict{
		table:      table,
		shardCount: shardCount,
	}
}

// spread 根据hashCode返回map中table的索引
func (m *ConcurrentHashDict) spread(hashCode int32) int32 {
	if m == nil {
		zap.L().Panic("ConcurrentHashDict is nil")
	}
	return (m.shardCount - 1) & hashCode
}

func (m *ConcurrentHashDict) getShard(hashCode int32) *shard {
	if m == nil {
		zap.L().Panic("ConcurrentHashDict is nil")
	}
	index := m.spread(hashCode)
	return m.table[index]
}

func (m *ConcurrentHashDict) Get(key string) (val any, exists bool) {
	hashCode := hash.Fnv32(key)
	s := m.getShard(hashCode)
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	val, exists = s.m[key]
	return
}

func (m *ConcurrentHashDict) Len() int32 {
	if m == nil {
		zap.L().Panic("ConcurrentHashDict is nil")
	}
	return atomic.LoadInt32(&m.count)
}

func (m *ConcurrentHashDict) Put(key string, val any) (result int32) {
	hashCode := hash.Fnv32(key)
	s := m.getShard(hashCode)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// key不存在 则是新增 count + 1
	if _, ok := s.m[key]; !ok {
		m.addCount()
	}
	s.m[key] = val
	return 1
}

func (m *ConcurrentHashDict) PutIfAbsent(key string, val any) (result int32) {
	hashCode := hash.Fnv32(key)
	s := m.getShard(hashCode)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// key存在则不修改 不存在
	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	return 1
}

func (m *ConcurrentHashDict) PutIfExists(key string, val any) (result int32) {
	hashCode := hash.Fnv32(key)
	s := m.getShard(hashCode)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// key存在则修改 不存在的话直接返回
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

func (m *ConcurrentHashDict) Remove(key string) (result int32) {
	hashCode := hash.Fnv32(key)
	s := m.getShard(hashCode)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.m[key]; !ok {
		return 0
	}
	delete(s.m, key)
	m.decreaseCount()
	return 1
}

func (m *ConcurrentHashDict) addCount() int32 {
	if m == nil {
		zap.L().Panic("ConcurrentHashDict is nil")
	}
	return atomic.AddInt32(&m.count, 1)
}

func (m *ConcurrentHashDict) decreaseCount() int32 {
	if m == nil {
		zap.L().Panic("ConcurrentHashDict is nil")
	}
	return atomic.AddInt32(&m.count, -1)
}

func (m *ConcurrentHashDict) ForEach(consumer datastruct.Consumer) {
	if m == nil {
		zap.L().Panic("ConcurrentHashDict is nil")
	}
	for _, s := range m.table {
		s.mutex.RLock()
		func() {
			defer s.mutex.RUnlock()
			for key, val := range s.m {
				if continues := consumer(key, val); !continues {
					return
				}
			}
		}()
	}
}

func (m *ConcurrentHashDict) Keys() []string {
	keys := make([]string, m.Len())
	var index int32
	m.ForEach(func(key string, val any) bool {
		if int(index) < len(keys) {
			keys[index] = key
			index++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

func (m *ConcurrentHashDict) RandomKeys(limit int32) []string {
	if m == nil {
		zap.L().Panic("ConcurrentHashDict is nil")
	}
	keys := make([]string, limit)
	for i := int32(0); i < limit; {
		// 随机选取shard
		index := rand.Int31n(int32(m.shardCount))
		s := m.table[index]
		if s == nil {
			continue
		}
		if key := s.randomKey(); key != "" {
			keys[i] = key
			i++
		}
	}
	return keys
}

func (m *ConcurrentHashDict) RandomDistinctKeys(limit int32) []string {
	if limit >= m.Len() {
		return m.Keys()
	}
	shardCount := m.shardCount
	keySet := make(map[string]struct{})
	for len(keySet) < int(limit) {
		index := rand.Int31n(int32(shardCount))
		s := m.table[index]
		if s == nil {
			continue
		}
		if key := s.randomKey(); key != "" {
			keySet[key] = struct{}{}
		}
	}
	result := make([]string, limit)
	i := 0
	for key, _ := range keySet {
		result[i] = key
		i++
	}
	return result
}

func (m *ConcurrentHashDict) Clear() {
	*m = *NewConcurrentHashDict(m.shardCount)
}

func (s *shard) randomKey() string {
	if s == nil {
		zap.L().Panic("shard is nil")
	}
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	// 本身就是随机
	for key := range s.m {
		return key
	}
	return ""
}

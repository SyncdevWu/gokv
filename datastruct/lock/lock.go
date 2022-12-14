package lock

import (
	"go.uber.org/zap"
	"gokv/lib/hash"
	"math"
	"sort"
	"sync"
)

type Locks struct {
	table []*sync.RWMutex
}

func NewLocks(size int32) *Locks {
	size = computeCapacity(size)
	table := make([]*sync.RWMutex, size)
	for i := 0; i < int(size); i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{
		table: table,
	}
}

func computeCapacity(param int32) (size int32) {
	if param <= 16 {
		return 16
	}
	n := param - 1
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

func (l *Locks) spread(hashCode int32) int32 {
	if l == nil {
		zap.L().Panic("Locks is nil")
	}
	return int32(len(l.table)-1) & hashCode
}

func (l *Locks) Lock(key string) {
	hashCode := hash.Fnv32(key)
	index := l.spread(hashCode)
	mu := l.table[index]
	mu.Lock()
}

func (l *Locks) RLock(key string) {
	hashCode := hash.Fnv32(key)
	index := l.spread(hashCode)
	mu := l.table[index]
	mu.RLock()
}

func (l *Locks) Unlock(key string) {
	hashCode := hash.Fnv32(key)
	index := l.spread(hashCode)
	mu := l.table[index]
	mu.Unlock()
}

func (l *Locks) RUnlock(key string) {
	hashCode := hash.Fnv32(key)
	index := l.spread(hashCode)
	mu := l.table[index]
	mu.RUnlock()
}

// toLockIndices 有序输出index
func (l *Locks) toLockIndices(keys []string, reverse bool) []int32 {
	indexSet := make(map[int32]struct{})
	for _, key := range keys {
		hashCode := hash.Fnv32(key)
		// 第几个锁
		index := l.spread(hashCode)
		indexSet[index] = struct{}{}
	}
	indexSlices := make([]int32, len(indexSet))
	i := 0
	for key := range indexSet {
		indexSlices[i] = key
		i++
	}
	sort.Slice(indexSlices, func(i, j int) bool {
		if reverse {
			return indexSlices[i] > indexSlices[j]
		} else {
			return indexSlices[i] < indexSlices[j]
		}
	})
	return indexSlices
}

func (l *Locks) Locks(keys ...string) {
	lockIndices := l.toLockIndices(keys, false)
	for _, index := range lockIndices {
		l.table[index].Lock()
	}
}

func (l *Locks) RLocks(keys ...string) {
	lockIndices := l.toLockIndices(keys, false)
	for _, index := range lockIndices {
		l.table[index].RLock()
	}
}

func (l *Locks) Unlocks(keys ...string) {
	lockIndices := l.toLockIndices(keys, true)
	for _, index := range lockIndices {
		l.table[index].Unlock()
	}
}

func (l *Locks) RUnlocks(keys ...string) {
	lockIndices := l.toLockIndices(keys, true)
	for _, index := range lockIndices {
		l.table[index].RUnlock()
	}
}

func (l *Locks) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := l.toLockIndices(keys, false)
	writeIndexSet := make(map[int32]struct{})
	for _, key := range writeKeys {
		hashCode := hash.Fnv32(key)
		index := l.spread(hashCode)
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		mu := l.table[index]
		if _, ok := writeIndexSet[index]; ok {
			// 写锁
			mu.Lock()
		} else {
			// 读锁
			mu.RLock()
		}
	}
}

func (l *Locks) RWUnlocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := l.toLockIndices(keys, true)
	writeIndexSet := make(map[int32]struct{})
	for _, key := range writeKeys {
		hashCode := hash.Fnv32(key)
		index := l.spread(hashCode)
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		mu := l.table[index]
		if _, ok := writeIndexSet[index]; ok {
			// 写锁
			mu.Unlock()
		} else {
			// 读锁
			mu.RUnlock()
		}
	}
}

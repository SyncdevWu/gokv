package atomic

import "sync/atomic"

// Boolean 是一个bool值 其所有的操作是原子性的
type Boolean uint32

// Get 原子读
func (b *Boolean) Get() bool {
	return atomic.LoadUint32((*uint32)(b)) != 0
}

// Set 原子写
func (b *Boolean) Set(v bool) {
	if v {
		atomic.StoreUint32((*uint32)(b), 1)
	} else {
		atomic.StoreUint32((*uint32)(b), 0)
	}
}

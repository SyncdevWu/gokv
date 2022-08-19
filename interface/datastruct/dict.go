package datastruct

// Consumer 用于遍历map， 如果返回false则终止遍历
type Consumer func(key string, val any) bool

type Dict interface {
	Get(key string) (val any, exists bool)
	Len() int32
	Put(key string, val any) (result int32)
	PutIfAbsent(key string, val any) (result int32)
	PutIfExists(key string, val any) (result int32)
	Remove(key string) (result int32)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int32) []string
	RandomDistinctKeys(limit int32) []string
	Clear()
}

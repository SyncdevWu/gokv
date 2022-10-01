package protocol

var (
	noBytes             = []byte("")
	okBytes             = []byte("+OK\r\n")
	pongBytes           = []byte("+PONG\r\n")
	nullBulkBytes       = []byte("$-1\r\n")
	nullMultiBulkBytes  = []byte("*-1\r\n")
	emptyMultiBulkBytes = []byte("*0\r\n")
	queuedBytes         = []byte("+QUEUED\r\n")
)

var (
	thePonyReply           = &PongReply{}
	theOkReply             = &OkReply{}
	theNoReply             = &NoReply{}
	theNullBulkReply       = &NullBulkReply{}
	theNullMultiBulkReply  = &NullMultiBulkReply{}
	theEmptyMultiBulkReply = &EmptyMultiBulkReply{}
	theQueuedReply         = &QueuedReply{}
)

// PongReply +PONG
type PongReply struct{}

// ToBytes 序列化
func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

func NewPongReply() *PongReply {
	return thePonyReply
}

// OkReply is +OK
type OkReply struct{}

// ToBytes 序列化
func (r *OkReply) ToBytes() []byte {
	return okBytes
}

// NewOkReply 返回一个OKReply
func NewOkReply() *OkReply {
	return theOkReply
}

// NullBulkReply 空的二进制安全字符串
type NullBulkReply struct{}

// ToBytes 序列化
func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

// NewNullBulkReply 创建一个NullBulkReply实例 并返回其指针
func NewNullBulkReply() *NullBulkReply {
	return theNullBulkReply
}

// NullBulkReply 空的二进制安全字符串
type NullMultiBulkReply struct{}

// ToBytes 序列化
func (r *NullMultiBulkReply) ToBytes() []byte {
	return nullMultiBulkBytes
}

// NewNullMultiBulkReply 创建一个空NullMultiBulkReply实例 并返回其指针
func NewNullMultiBulkReply() *NullMultiBulkReply {
	return theNullMultiBulkReply
}

// EmptyMultiBulkReply 空list
type EmptyMultiBulkReply struct{}

// ToBytes 序列化
func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

// NewEmptyMultiBulkReply 创建一个EmptyMultiBulkReply实例
func NewEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return theEmptyMultiBulkReply
}

// NoReply 无回复 如subscribe命令
type NoReply struct{}

// ToBytes 序列化
func (r *NoReply) ToBytes() []byte {
	return noBytes
}

func NewNoReply() *NoReply {
	return theNoReply
}

// QueuedReply is +QUEUED
type QueuedReply struct{}

// ToBytes 序列化
func (r *QueuedReply) ToBytes() []byte {
	return queuedBytes
}

// NewQueuedReply 返回一个QueuedReply实例
func NewQueuedReply() *QueuedReply {
	return theQueuedReply
}

package redis

type Reply interface {
	ToBytes() []byte
}

// ErrorReply 继承Reply接口
type ErrorReply interface {
	Error() string // 返回错误信息
	Reply
}

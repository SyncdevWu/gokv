package protocol

import (
	"bytes"
	"gokv/interface/redis"
	"strconv"
)

var (
	nullBulkReplyBytes = []byte("$-1") // 空字符串 如get的key不存在时的响应
	CRLF               = "\r\n"
)

// BulkReply 二进制安全字符串
type BulkReply struct {
	Arg []byte
}

// NewBulkReply 根据字节数组创建BulkReply
func NewBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

// ToBytes 序列化
func (r *BulkReply) ToBytes() []byte {
	if len(r.Arg) == 0 {
		return nullBulkReplyBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

// MultiBulkReply 存储Bulk String数组
type MultiBulkReply struct {
	Args [][]byte
}

// NewMultiBulkReply 创建一个MultiBulkReply实例
func NewMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

// ToBytes 序列化
func (r *MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

// MultiRawReply 存储复杂的list结构, 如GeoPos命令
type MultiRawReply struct {
	Replies []redis.Reply
}

// NewMultiRawReply 创建一个MultiRawReply实例
func NewMultiRawReply(replies []redis.Reply) *MultiRawReply {
	return &MultiRawReply{
		Replies: replies,
	}
}

// ToBytes 序列化
func (r *MultiRawReply) ToBytes() []byte {
	argLen := len(r.Replies)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Replies {
		buf.Write(arg.ToBytes())
	}
	return buf.Bytes()
}

// StatusReply 存储简单的状态字符串 如OK
type StatusReply struct {
	Status string
}

// NewStatusReply 创建一个StatusReply实例
func NewStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

// ToBytes 序列化
func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

// IsOKReply 当给定的reply为+OK时返回true
func IsOKReply(reply redis.Reply) bool {
	return string(reply.ToBytes()) == "+OK\r\n"
}

// IntReply 存储int64
type IntReply struct {
	Code int64
}

// NewIntReply 创建一个IntReply实例
func NewIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// ToBytes 序列化
func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

// StandardErrReply 表示一个服务器错误
type StandardErrReply struct {
	Status string
}

// NewErrReply 创建一个StandardErrReply实例
func NewErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

// IsErrorReply 当给定的reply是错误是返回true
func IsErrorReply(reply redis.Reply) bool {
	return reply.ToBytes()[0] == '-'
}

// ToBytes 序列化
func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

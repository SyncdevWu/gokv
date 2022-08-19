package aof

import (
	"gokv/interface/redis"
	"gokv/redis/protocol"
	"strconv"
	"time"
)

var pExpireAtBytes = []byte("PEXPIREAT")

// NewExpireCmd 生成给定key的过期时间的命令
func NewExpireCmd(key string, expireAt time.Time) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return protocol.NewMultiBulkReply(args)
}

func EntityToCmd(key string, entity *redis.DataEntity) *protocol.MultiBulkReply {
	if entity == nil {
		return nil
	}
	switch val := entity.Data.(type) {
	case []byte:
		return stringToCmd(key, val)
	}
	return nil
}

var setCmd = []byte("SET")

func stringToCmd(key string, val []byte) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = val
	return protocol.NewMultiBulkReply(args)
}

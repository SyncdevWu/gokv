package exec

import (
	"gokv/interface/redis"
	"gokv/redis/aof"
	"gokv/redis/database"
	"gokv/redis/protocol"
	"gokv/redis/router"
	"gokv/redis/utils"
	"strconv"
	"time"
)

func execPExpireAt(sdb *database.SingleDB, args [][]byte) redis.Reply {
	key := string(args[0])
	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.NewErrReply("ERR value is not an integer or out of range")
	}
	expireAt := time.Unix(0, raw*int64(time.Millisecond))
	_, exists := sdb.GetEntity(key)
	// 无修改
	if !exists {
		return protocol.NewIntReply(0)
	}
	// 时间轮任务
	sdb.Expire(key, expireAt)
	// AOF日志
	sdb.AddAof(aof.NewExpireCmd(key, expireAt).Args)
	return protocol.NewIntReply(1)
}

func init() {
	router.RegisterCommand("PExpireAt", execPExpireAt, transaction.WriteFirstKey, nil, 3, router.FlagWrite)
}

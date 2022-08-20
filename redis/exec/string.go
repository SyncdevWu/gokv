package exec

import (
	"gokv/interface/redis"
	"gokv/redis/aof"
	"gokv/redis/database"
	"gokv/redis/protocol"
	"gokv/redis/router"
	"gokv/redis/utils"
	"gokv/utils"
	"strconv"
	"strings"
	"time"
)

const (
	upsertPolicy = iota // default
	insertPolicy        // set nx
	updatePolicy        // set ex
)

const unlimitedTTL int64 = 0

func execGet(sdb *database.SingleDB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := sdb.GetAsByteSlice(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return protocol.NewNullBulkReply()
	}
	return protocol.NewBulkReply(bytes)
}

func execGetEX(sdb *database.SingleDB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := sdb.GetAsByteSlice(key)
	ttl := unlimitedTTL
	if err != nil {
		return err
	}
	if bytes == nil {
		return protocol.NewNullBulkReply()
	}
	for i := 1; i < len(args); i++ {
		switch arg := strings.ToUpper(string(args[i])); arg {
		case "PX":
			// PX用于设置过期时间 单位ms 在碰到PX之前ttl必须为无过期的，否则就是属于语法错误
			// PX和EX只能出现一个
			if ttl != unlimitedTTL {
				return protocol.NewSyntaxErrReply()
			}
			if i+1 >= len(args) {
				return protocol.NewSyntaxErrReply()
			}
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return protocol.NewSyntaxErrReply()
			}
			if ttlArg <= 0 {
				return protocol.NewSyntaxErrReply()
			}
			ttl = ttlArg
			i++ // 跳过数字参数
		case "EX":
			// EX用于设置过期时间 单位s 在碰到EX之前ttl必须为无过期的，否则就是属于语法错误
			// PX和EX只能出现一个
			if ttl != unlimitedTTL {
				return protocol.NewSyntaxErrReply()
			}
			if i+1 >= len(args) {
				return protocol.NewSyntaxErrReply()
			}
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return protocol.NewSyntaxErrReply()
			}
			if ttlArg <= 0 {
				return protocol.NewSyntaxErrReply()
			}
			ttl = ttlArg * 1000
			i++ // 跳过数字参数
		case "PERSIST":
			if ttl != unlimitedTTL {
				return protocol.NewSyntaxErrReply()
			}
			if i+1 >= len(arg) {
				return protocol.NewSyntaxErrReply()
			}
			sdb.Persist(key)
		}
	}
	if len(args) > 1 {
		if ttl != unlimitedTTL { // PX or EX
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			// 添加到ttl dict中, 并向time wheel添加任务
			sdb.Expire(key, expireTime)
			sdb.AddAof(aof.NewExpireCmd(key, expireTime).Args)
		} else {
			// PERSIST
			sdb.AddAof(utils.ToCmdLine2("persist", args[0]))
		}
	}
	return protocol.NewBulkReply(bytes)
}

func execSet(sdb *database.SingleDB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[1]
	policy := upsertPolicy
	ttl := unlimitedTTL

	// set key val options.... (eg. nx xx px...)
	for i := 2; i < len(args); i++ {
		switch arg := strings.ToUpper(string(args[i])); arg {
		case "NX":
			// 仅当key不存在时才能设置成功 insert
			if policy == updatePolicy {
				return protocol.NewSyntaxErrReply()
			}
			policy = insertPolicy
		case "XX":
			// 仅当key存在时才能设置成功 update
			if policy == insertPolicy {
				return protocol.NewSyntaxErrReply()
			}
			policy = updatePolicy
		case "EX":
			// EX用于设置过期时间 单位s 在碰到EX之前ttl必须为无过期的，否则就是属于语法错误
			if ttl != unlimitedTTL {
				return protocol.NewSyntaxErrReply()
			}
			// EX 后面要带数字 否则就是属于语法错误
			if i+1 >= len(args) {
				return protocol.NewSyntaxErrReply()
			}
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return protocol.NewSyntaxErrReply()
			}
			if ttlArg <= 0 {
				return protocol.NewErrReply("ERR invalid expire time in set")
			}
			ttlArg *= 1000
			ttl = ttlArg
			i++ // 跳过EX后面的参数
		case "PX":
			// PX用于设置过期时间 单位ms 在碰到PX之前ttl必须为无过期的，否则就是属于语法错误
			// PX和EX只能出现一个
			if ttl != unlimitedTTL {
				return protocol.NewSyntaxErrReply()
			}
			// EX 后面要带数字 否则就是属于语法错误
			if i+1 >= len(args) {
				return protocol.NewSyntaxErrReply()
			}
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return protocol.NewSyntaxErrReply()
			}
			if ttlArg <= 0 {
				return protocol.NewErrReply("ERR invalid expire time in set")
			}
			ttl = ttlArg
			i++ // 跳过PX后面的参数
		default:
			return protocol.NewSyntaxErrReply()
		}
	}

	entity := &redis.DataEntity{
		Data: value,
	}

	var result int32
	switch policy {
	case upsertPolicy:
		result = sdb.PutEntity(key, entity)
	case insertPolicy:
		result = sdb.PutIfAbsent(key, entity)
	case updatePolicy:
		result = sdb.PutIfExists(key, entity)
	}

	if result > 0 {
		if ttl != unlimitedTTL {
			// 带有过期时间的key ttl单位是ms 转成ns
			// ttl表示的是key的存活时间 将其转成过期时间戳
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			// 添加到ttl dict中, 并向time wheel添加任务
			sdb.Expire(key, expireTime)
			sdb.AddAof([][]byte{
				[]byte("set"),
				args[0],
				args[1],
			})
			sdb.AddAof(aof.NewExpireCmd(key, expireTime).Args)
		} else {
			// 可能原先是带有过期时间的 更新后修改为永久 因此需要取消time wheel并从ttl dict移除
			sdb.Persist(key)
			sdb.AddAof(utils.ToCmdLine2("set", args...))
		}
		return &protocol.OkReply{}
	}
	return &protocol.NullBulkReply{}
}

func init() {
	router.RegisterCommand("Set", execSet, transaction.WriteFirstKey, nil, -3, router.FlagWrite)
	router.RegisterCommand("Get", execGet, transaction.ReadFirstKey, nil, 2, router.FlagReadOnly)
	router.RegisterCommand("GetEX", execGetEX, transaction.WriteFirstKey, nil, -2, router.FlagWrite)
}

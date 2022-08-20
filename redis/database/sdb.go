package database

import (
	"gokv/datastruct/dict"
	"gokv/datastruct/lock"
	"gokv/interface/datastruct"
	"gokv/interface/redis"
	"gokv/lib/timewheel"
	"gokv/redis/protocol"

	"gokv/config"
	"strings"
	"time"

	"go.uber.org/zap"
)

// SingleDB 存储数据和用户输入的命令
type SingleDB struct {
	index   int             // 实例索引
	data    datastruct.Dict // key -> *DataEntity
	ttl     datastruct.Dict // key -> expireTime(time.Time)
	version datastruct.Dict // key -> version(int)
	locks   *lock.Locks     // 用于同时锁住多个key 适用于rpush incr命令..
	addAof  func(CmdLine)   // 实际添加aof命令的操作函数 本质上是调用mdb的aofHandler.AddAof向aofChan发送消息
}

// newSingleDB 创建一个只具有并发安全的DB实例
func newSingleDB() *SingleDB {
	return &SingleDB{
		data:    dict.NewConcurrentHashDict(config.Conf.DataDictSize),
		ttl:     dict.NewConcurrentHashDict(config.Conf.TtlDictSize),
		version: dict.NewConcurrentHashDict(config.Conf.DataDictSize),
		locks:   lock.NewLocks(config.Conf.LockerSize),
		addAof:  func(line CmdLine) {},
	}
}

// newBasicSingleDB 创建一个只具有非并发安全的DB实例
func newBasicSingleDB() *SingleDB {
	return &SingleDB{
		data:    dict.NewSimpleDict(),
		ttl:     dict.NewSimpleDict(),
		version: dict.NewSimpleDict(),
		locks:   lock.NewLocks(1),
		addAof:  func(line CmdLine) {},
	}
}

func (sdb *SingleDB) Exec(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "multi" {
		// 开启事务命令 不能携带额外的参数
		if len(cmdLine) != 1 {
			return protocol.NewArgNumErrReply(cmdName)
		}
		// 开启事务 本质上是以conn为单位做一个标志位 然后后续的事务中的命令会添加到conn的队列中去
		return sdb.StartMulti(conn)
	} else if cmdName == "discard" {
		// 取消事务命令 不能携带额外的参数
		if len(cmdLine) != 1 {
			return protocol.NewArgNumErrReply(cmdName)
		}
		// 丢弃事务
		return sdb.DiscardMulti(conn)
	} else if cmdName == "exec" {
		// 执行事务命令 不能携带额外的参数
		if len(cmdLine) != 1 {
			return protocol.NewArgNumErrReply(cmdName)
		}
		// 执行事务
		return sdb.DoMulti(conn)
	} else if cmdName == "watch" {
		// 至少需要2个参数 watch k1 k2 k3...
		if !validateArity(-2, cmdLine) {
			return protocol.NewArgNumErrReply(cmdName)
		}
		// 添加key到conn的watching字典里
		return sdb.Watch(conn, cmdLine)
	}
	// 开启事务 添加命令到队列中
	if conn != nil && conn.InMultiState() {
		return sdb.EnqueueCmd(conn, cmdLine)
	}
	// 普通命令 set k v
	return sdb.execNormalCommand(cmdLine)
}

func (sdb *SingleDB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := CmdTable[cmdName]
	if !ok {
		return protocol.NewErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.Arity, cmdLine) {
		return protocol.NewArgNumErrReply(cmdName)
	}
	prepare := cmd.Prepare
	writeKeys, readKeys := prepare(cmdLine[1:])
	sdb.AddVersion(writeKeys...)
	sdb.RWLocks(writeKeys, readKeys)
	defer sdb.RWUnlocks(writeKeys, readKeys)
	executor := cmd.Executor
	return executor(sdb, cmdLine[1:])
}

// execWithLock 这个方法与execNormalCommand基本相同，不同之处在于该方法内部没有提供加锁机制 由调用方来自定义需要加的锁
func (sdb *SingleDB) execWithLock(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := CmdTable[cmdName]
	if !ok {
		return protocol.NewErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.Arity, cmdLine) {
		return protocol.NewArgNumErrReply(cmdName)
	}
	executor := cmd.Executor
	return executor(sdb, cmdLine[1:])
}

// validateArity 校验参数个数(包含命令)
// 如: watch 则 可以传入 arity = -2 表示至少需要2个参数 如果 arity >= 0 则表示一定要arity个参数
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/** data access **/

func (sdb *SingleDB) GetEntity(key string) (*redis.DataEntity, bool) {
	raw, ok := sdb.data.Get(key)
	if !ok {
		return nil, false
	}
	// 惰性删除 只有key被访问的时候会被删除
	if sdb.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*redis.DataEntity)
	return entity, true
}

func (sdb *SingleDB) PutEntity(key string, entity *redis.DataEntity) int32 {
	return sdb.data.Put(key, entity)
}

func (sdb *SingleDB) PutIfExists(key string, entity *redis.DataEntity) int32 {
	return sdb.data.PutIfExists(key, entity)
}

func (sdb *SingleDB) PutIfAbsent(key string, entity *redis.DataEntity) int32 {
	return sdb.data.PutIfAbsent(key, entity)
}

// Remove 删除key 这个函数会同时从data和ttl的dict中删除 并取消时间轮中的任务
func (sdb *SingleDB) Remove(key string) int32 {
	result := sdb.data.Remove(key)
	_ = sdb.ttl.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
	return result
}

func (sdb *SingleDB) Removes(keys ...string) int32 {
	var result int32
	for _, key := range keys {
		result += sdb.Remove(key)
	}
	return result
}

/** add version **/

func (sdb *SingleDB) Watch(conn redis.Connection, args [][]byte) redis.Reply {
	watching := conn.GetWatching()
	for _, arg := range args[1:] {
		key := string(arg)
		// 保存当前版本号 这样在乐观锁能避免ABA问题
		watching[key] = sdb.GetVersion(key)
	}
	return protocol.NewOkReply()
}

func (sdb *SingleDB) AddVersion(writeKeys ...string) {
	for _, key := range writeKeys {
		versionCode := sdb.GetVersion(key)
		sdb.version.Put(key, versionCode+1)
	}
}

func (sdb *SingleDB) GetVersion(key string) uint32 {
	val, exists := sdb.version.Get(key)
	if !exists {
		return 0
	}
	return val.(uint32)
}

/** Lock **/

// RWLocks 一次性对多个key分别上写锁 读锁
func (sdb *SingleDB) RWLocks(writeKeys []string, readKeys []string) {
	sdb.locks.RWLocks(writeKeys, readKeys)
}

func (sdb *SingleDB) RWUnlocks(writeKeys []string, readKeys []string) {
	sdb.locks.RWUnlocks(writeKeys, readKeys)
}

/** TTL **/

// Persist 将原先有过期时间的key转成无过期时间
func (sdb *SingleDB) Persist(key string) {
	sdb.ttl.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// genExpireTask 生成时间轮里task的key
func genExpireTask(key string) string {
	return "expire:" + key
}

// IsExpired 判断Key是否已经过期
func (sdb *SingleDB) IsExpired(key string) bool {
	rawExpireTime, exists := sdb.ttl.Get(key)
	// 不在ttl dict里的是无限期key
	if !exists {
		return false
	}
	// 在访问的时候会检测这个key是否过期了 如果过期了则将他从ttl中删除 属于惰性删除
	// 而时间轮的作用是定期删除 只有时间轮走到了当时保存这个过期key的位置的时候，在遍历链表的时候才会删除
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		sdb.ttl.Remove(key)
	}
	return expired
}

// Expire 给key添加过期时间 该方法会把key添加到ttl dict中 同时创建时间轮任务 定期删除
func (sdb *SingleDB) Expire(key string, expireTime time.Time) {
	sdb.ttl.Put(key, expireTime)
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		// 具体的执行
		// 检查key是否已经过期，已经过期则从sdb中移除
		keys := []string{key}
		sdb.RWLocks(keys, nil)
		defer sdb.RWUnlocks(keys, nil)
		zap.L().Info("expire " + key)
		rawExpireTime, ok := sdb.ttl.Get(key)
		if !ok {
			return
		}
		if expireTime, ok = rawExpireTime.(time.Time); !ok {
			return
		} else {
			expired := time.Now().After(expireTime)
			if expired {
				sdb.Remove(key)
			}
		}
	})
}

// ForEach 遍历 consumer参数是可以在遍历中座的事 如果consumer函数返回了false则会提前终止遍历
func (sdb *SingleDB) ForEach(consumer func(key string, data *redis.DataEntity, expiration *time.Time) bool) {
	sdb.data.ForEach(func(key string, val any) bool {
		entity, ok := val.(*redis.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := sdb.ttl.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}
		return consumer(key, entity, expiration)
	})
}

// GetAsByteSlice 将DataEntity中的内容转换成字节切片 主要用于字符串
func (sdb *SingleDB) GetAsByteSlice(key string) ([]byte, redis.ErrorReply) {
	entity, exists := sdb.GetEntity(key)
	if !exists {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, protocol.NewWrongTypeErrReply()
	}
	return bytes, nil
}

/** aof **/

func (sdb *SingleDB) AddAof(cmdLine CmdLine) {
	sdb.addAof(cmdLine)
}

/** multi **/

func (sdb *SingleDB) StartMulti(conn redis.Connection) redis.Reply {
	if conn.InMultiState() {
		// 事务嵌套事务
		return protocol.NewErrReply("ERR MULTI calls can not be nested")
	}
	// 设置事务开启标志位
	conn.SetMultiState(true)
	return protocol.NewOkReply()
}

func (sdb *SingleDB) DoMulti(conn redis.Connection) redis.Reply {
	// 当前的客户端并没有处理开启事务的状态
	if !conn.InMultiState() {
		return protocol.NewErrReply("ERR EXEC without MULTI")
	}
	// 事务中存在语法错误命令 终止事务 重置字段
	if conn.GetAbort() {
		conn.ClearQueuedCmds()
		conn.SetAbort(false)
		conn.SetMultiState(false)
		return protocol.NewErrReply("EXECABORT Transaction discarded because of previous errors.")
	}
	defer conn.SetMultiState(false)
	// 执行事务的核心逻辑
	return sdb.ExecMulti(conn, conn.GetWatching(), conn.GetQueuedCmdLine())
}

func (sdb *SingleDB) DiscardMulti(conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return protocol.NewErrReply("ERR DISCARD without MULTI")
	}
	// 重置conn内的字段
	conn.ClearQueuedCmds()
	conn.SetAbort(false)
	conn.SetMultiState(false)
	return protocol.NewOkReply()
}

// EnqueueCmd 添加命令到conn内的字段 这些命令会在客户端发送exec的时候被执行
func (sdb *SingleDB) EnqueueCmd(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := CmdTable[cmdName]
	if !ok {
		conn.SetAbort(true)
		return protocol.NewErrReply("ERR unknown command '" + cmdName + "'")
	}
	if cmd.Prepare == nil {
		conn.SetAbort(true)
		return protocol.NewErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}
	if !validateArity(cmd.Arity, cmdLine) {
		conn.SetAbort(true)
		return protocol.NewArgNumErrReply(cmdName)
	}
	conn.EnqueueCmd(cmdLine)
	return protocol.NewQueuedReply()
}

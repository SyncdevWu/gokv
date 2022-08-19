package database

import (
	"gokv/config"
	"gokv/interface/redis"
	"gokv/redis/aof"
	"gokv/redis/protocol"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type CmdLine = [][]byte

var CmdTable = make(map[string]*Command)

type ExecFunc func(db *SingleDB, args [][]byte) redis.Reply

type PreFunc func(args [][]byte) ([]string, []string)

type UndoFunc func(db *SingleDB, args [][]byte) []CmdLine

type Command struct {
	Executor ExecFunc // 执行函数
	Prepare  PreFunc  // 预处理函数 主要是分析哪些key需要加读锁 哪些key需要加写锁
	Undo     UndoFunc // 回滚指令生成函数 在执行事务中的命令之前 会先生成对应的逆操作来进行回滚 类似git revert
	Arity    int      // 命令所需的参数数量 > 0 表示为固定长度 < 0表示可变长度 如-2表示最少需要2个参数 (get k1 k2)
	Flags    int      // 读写flag
}

type MultiDB struct {
	dbSet      []*atomic.Value // 保存SDB SDB是单个数据库
	aofHandler *aof.Handler    // aof

	// TODO pubsub aof

	// TODO replication

}

// NewStandaloneServer 创建一个标准的单机的redis数据库
func NewStandaloneServer() *MultiDB {
	mdb := &MultiDB{}
	// 默认16个SDB
	if config.Conf.Databases == 0 {
		config.Conf.Databases = 16
	}
	mdb.dbSet = make([]*atomic.Value, config.Conf.Databases)
	// SDB是用atomic.Value包装
	for i := 0; i < len(mdb.dbSet); i++ {
		sdb := newSingleDB()
		sdb.index = i
		holder := &atomic.Value{}
		holder.Store(sdb)
		mdb.dbSet[i] = holder
	}
	// TODO pubsub

	// AOF
	validAof := false
	// 仅AOF持久化
	if config.Conf.AppendOnly {
		aofHandler, err := aof.NewAofHandler(mdb, func() redis.EmbedDB {
			// 没有一次性加多个锁(Locks)的MDB 也没有aof等功能
			// 这个mdb的主要作用就是aof重写的时候充当fork的作用 会根据copy出来的aof文件给该数据库回复 然后再根据改数据库内存中的key val进行重写
			return NewBasicMultiDB()
		})
		if err != nil {
			zap.L().Panic("MultiDB NewStandaloneServer() aof.NewAofHandler failed ", zap.Error(err))
		}
		mdb.aofHandler = aofHandler
		for _, holder := range mdb.dbSet {
			sdb := holder.Load().(*SingleDB)
			// SDB的AddAof函数的目的就是调用mdb里持有的aofHandler 目的其实就是向aof协程发送一条持久化命令
			sdb.addAof = func(cmdLine CmdLine) {
				mdb.aofHandler.AddAof(sdb.index, cmdLine)
			}
		}
		validAof = true
	}

	// TODO LOAD RDB 仅RDb持久化
	if config.Conf.RDBFilename != "" && !validAof {

	}
	return mdb
}

// NewBasicMultiDB 创建一个标准的仅具有基本的读写能力的redis数据库
func NewBasicMultiDB() *MultiDB {
	mdb := &MultiDB{}
	mdb.dbSet = make([]*atomic.Value, config.Conf.Databases)
	for i := 0; i < len(mdb.dbSet); i++ {
		sdb := newBasicSingleDB()
		sdb.index = i
		holder := &atomic.Value{}
		holder.Store(sdb)
		mdb.dbSet[i] = holder
	}
	return mdb
}

// Exec redis数据库的核心处理流程 conn是封装过的net.Conn并保存了事务状态 事务开启时队列保存的命令 选中的当前数据库等 cmdLine是客户端发送过来的命令
func (mdb *MultiDB) Exec(conn redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			zap.L().Error("MultiDB.Exec() failed. ", zap.Error(err.(error)))
			result = protocol.NewUnknownErrReply()
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	// TODO authenticate slaveof .....

	// select命令
	if cmdName == "select" {
		// 目前不支持在开启事务的时候切换数据库 但是redis原生应该是支持的
		if conn != nil && conn.InMultiState() {
			return protocol.NewErrReply("cannot select database within multi")
		}
		// select dbIndex
		if len(cmdLine) != 2 {
			return protocol.NewArgNumErrReply("select")
		}
		return execSelect(conn, mdb, cmdLine[1:])
	}

	// 执行普通的命令
	// 因为可能存在多个客户端 每个客户端当前选中的数据库是可以不同的 所以把客户端与数据库绑定
	// 如果在这之前 执行了select命令 则也是更新了conn的dbIndex 等到要执行普通命令的时候再直接根据idx从mdb里取
	dbIndex := conn.GetDBIndex()
	selectedDB, errReply := mdb.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return selectedDB.Exec(conn, cmdLine)
}

// execSelect 切换客户端所选中的数据库 本质上就是修改conn中保存的dbIndex 在后续执行普通命令的时候会根据conn保存的dbIndex选择对应的sdb
func execSelect(conn redis.Connection, mdb *MultiDB, args [][]byte) redis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.NewErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.NewErrReply("ERR DB index is out of range")
	}
	// 只是讲切换后的数据库保存到conn里
	conn.SelectDB(dbIndex)
	return protocol.NewOkReply()
}

func (mdb *MultiDB) AfterClientClose(c redis.Connection) {
	//TODO implement me
}

func (mdb *MultiDB) Close() {
	//TODO
	if mdb.aofHandler != nil {
		mdb.aofHandler.Close()
	}
}

// mustSelectDB 如果切换数据库失败则会panic
func (mdb *MultiDB) mustSelectDB(dbIndex int) *SingleDB {
	selectedDB, err := mdb.selectDB(dbIndex)
	if err != nil {
		zap.L().Panic("MultiDB.mustSelectDB() failed. ", zap.Error(err))
	}
	return selectedDB
}

// selectDB 如果切换数据库失败则返回error 可以根据err != nil 来判断数据库是否切换成功
func (mdb *MultiDB) selectDB(dbIndex int) (*SingleDB, *protocol.StandardErrReply) {
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return nil, protocol.NewErrReply("ERR DB index out of range")
	}
	return mdb.dbSet[dbIndex].Load().(*SingleDB), nil
}

func (mdb *MultiDB) flushDB(dbIndex int) redis.Reply {
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.NewErrReply("ERR DB index out of range")
	}
	sdb := newSingleDB()
	mdb.loadDB(dbIndex, sdb)
	return &protocol.OkReply{}
}

func (mdb *MultiDB) loadDB(dbIndex int, newDB *SingleDB) redis.Reply {
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.NewErrReply("ERR DB index out of range")
	}
	oldDB := mdb.mustSelectDB(dbIndex)
	newDB.index = dbIndex
	newDB.addAof = oldDB.addAof
	mdb.dbSet[dbIndex].Store(newDB)
	return &protocol.OkReply{}
}

// ForEach 遍历某个SDB中的数据
func (mdb *MultiDB) ForEach(dbIndex int, consumer func(key string, data *redis.DataEntity, expiration *time.Time) bool) {
	mdb.mustSelectDB(dbIndex).ForEach(consumer)
}

// GetDBSize 返回某个SDB的所有key数量、未过期的key数量
func (mdb *MultiDB) GetDBSize(dbIndex int) (int, int) {
	db := mdb.mustSelectDB(dbIndex)
	return int(db.data.Len()), int(db.ttl.Len())
}

// ExecWithLock 这个方法内部没有提供加锁机制 由调用方来自定义需要加的锁 即Command中Executor来提供
func (mdb *MultiDB) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	sdb, errReply := mdb.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return sdb.execWithLock(cmdLine)
}

// ExecMulti 执行事务 TODO 貌似是给集群版使用的?
func (mdb *MultiDB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	sdb, errReply := mdb.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return sdb.ExecMulti(conn, watching, cmdLines)
}

// GetUndoLogs 生成对应的回滚命令 TODO 貌似是给集群版使用的?
func (mdb *MultiDB) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
	return mdb.mustSelectDB(dbIndex).GetUndoLogs(cmdLine)
}

// RWLocks TODO 貌似是给集群版使用的?
func (mdb *MultiDB) RWLocks(dbIndex int, writeKeys, readKeys []string) {
	mdb.mustSelectDB(dbIndex).RWLocks(writeKeys, readKeys)
}

// RWUnlocks TODO 貌似是给集群版使用的?
func (mdb *MultiDB) RWUnlocks(dbIndex int, writeKeys, readKeys []string) {
	mdb.mustSelectDB(dbIndex).RWUnlocks(writeKeys, readKeys)
}

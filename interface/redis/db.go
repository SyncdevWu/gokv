package redis

import (
	"time"
)

type CmdLine = [][]byte

type DataEntity struct {
	Data any
}

type DB interface {
	Exec(conn Connection, cmdLine [][]byte) (result Reply)
	AfterClientClose(c Connection)
	Close()
}

type EmbedDB interface {
	DB
	ExecWithLock(conn Connection, cmdLine [][]byte) Reply
	ExecMulti(conn Connection, watching map[string]uint32, cmdLines []CmdLine) Reply
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine
	ForEach(dbIndex int, consumer func(key string, data *DataEntity, expiration *time.Time) bool)
	RWLocks(dbIndex int, writeKeys, readKeys []string)
	RWUnlocks(dbIndex int, writeKeys, readKeys []string)
	GetDBSize(dbIndex int) (int, int)
}

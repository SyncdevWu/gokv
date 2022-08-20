package database

import (
	"gokv/interface/redis"
	"gokv/redis/protocol"
	"strings"
)

// ExecMulti 执行事务的核心逻辑
func (sdb *SingleDB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	writeKeys := make([]string, 0)
	readKeys := make([]string, 0)
	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		// 根据cmdName获取注册的Command
		cmd, ok := CmdTable[cmdName]
		// 未注册的命令
		if !ok {
			return protocol.NewErrReply("ERR unknown command '" + cmdName + "'")
		}
		// 事务执行前分析需要加锁的key 并对他们分别加读锁或写锁 而且是必须一次性加上
		prepare := cmd.Prepare
		write, read := prepare(cmdLine[1:])
		writeKeys = append(writeKeys, write...)
		readKeys = append(readKeys, read...)
	}
	// 乐观锁观察key
	watchingKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}
	// 这里的情况可能是存在其他的客户端开启事务的时间线比当前conn的晚 但是先执行了exec命令
	// 这个情况是会修改版本号的 因此只要conn的事务在正式执行命令前判断一次就行 如果发现版本已经被修改了就不执行了
	// 后面就不用考虑这个问题了 因为加了读锁 其他conn是不能修改key的
	readKeys = append(readKeys, watchingKeys...)
	sdb.RWLocks(writeKeys, readKeys)
	defer sdb.RWUnlocks(writeKeys, readKeys)
	//  watch的值被改变 主要是比较版本号
	if isWatchingChanged(sdb, watching) {
		return protocol.NewEmptyMultiBulkReply()
	}
	// 每条命令对应的reply
	results := make([]redis.Reply, 0, len(cmdLines))
	aborted := false
	// 这里要用[]CmdLine 是因为一条命令的回滚可能需要多条命令
	undoCmdLines := make([][]CmdLine, 0, len(cmdLines))
	for _, cmdLine := range cmdLines {
		// 在执行命令前先构造undo log
		undoCmdLines = append(undoCmdLines, sdb.GetUndoLogs(cmdLine))
		// 执行命令
		reply := sdb.execWithLock(cmdLine)
		// 单行命令执行错误
		if protocol.IsErrorReply(reply) {
			// 事务中有命令执行出错标志
			aborted = true
			// 执行出错的命令不需要回滚
			undoCmdLines = undoCmdLines[:len(undoCmdLines)-1]
			// 后续不需要执行 准备回滚
			// 这里的处理与原生的redis是不同的 原生的redis是事务中的命令在执行出错的时候 会继续执行后续的命令的
			break
		}
		results = append(results, reply)
	}
	// 事务中的所有命令执行成功
	if !aborted {
		sdb.AddVersion(writeKeys...)
		return protocol.NewMultiRawReply(results)
	}
	// 执行事务失败 开始回滚
	size := len(undoCmdLines)
	// 从后往前回滚 即先撤销最新的操作
	for i := size - 1; i >= 0; i-- {
		// 命令需要的回滚命令 可能是多条
		curCmdLines := undoCmdLines[i]
		if len(curCmdLines) == 0 {
			continue
		}
		for j := 0; j < len(curCmdLines); j++ {
			sdb.execWithLock(curCmdLines[j])
		}
	}
	return protocol.NewErrReply("EXECABORT Transaction discarded because of previous errors.")
}

func (sdb *SingleDB) GetUndoLogs(cmdLine CmdLine) []CmdLine {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := CmdTable[cmdName]
	if !ok {
		return nil
	}
	undo := cmd.Undo
	if undo == nil {
		return nil
	}
	// 根据cmdLine[1:]生成undo命令
	return undo(sdb, cmdLine[1:])
}

// isWatchingChanged 判断key是否被修改过了
func isWatchingChanged(sdb *SingleDB, watching map[string]uint32) bool {
	for key, version := range watching {
		currentVersion := sdb.GetVersion(key)
		if currentVersion != version {
			return true
		}
	}
	return false
}

package redis

type Connection interface {
	Write([]byte) error
	SetPassword(string)
	GetPassword() string

	Subscribe(channel string)
	UnSubscribe(channel string)
	SubsCount() int
	GetChannels() []string

	InMultiState() bool             // 是否是在事务中
	SetMultiState(bool)             // 设置事务是否开始的标识符
	GetAbort() bool                 // 事务已经发生了语法错误
	SetAbort(bool)                  // 设置事务中命令语法错误标志位
	GetQueuedCmdLine() [][][]byte   // 获取开启事务后 队列中的所有命令
	EnqueueCmd([][]byte)            // 添加命令到队列中
	ClearQueuedCmds()               // 清空队列中的命令
	GetWatching() map[string]uint32 // 乐观锁

	GetDBIndex() int // 获取当前选中的数据库
	SelectDB(int)    // 切换数据库

	GetRole() int32
	SetRole(int32)
}

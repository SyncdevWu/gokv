package client

import (
	"bytes"
	"gokv/lib/sync/wait"
	"net"
	"sync"
	"time"
)

const (
	// NormalCli is client with user
	NormalCli = iota
	// ReplicationRecvCli is fake client with replication master
	ReplicationRecvCli
)

type RedisClientConnection struct {
	conn         net.Conn
	waitingReply wait.Wait
	mu           sync.Mutex
	password     string // 密码可能在运行时被修改 因此保存密码

	multiState bool              // 事务开启标志
	queue      [][][]byte        // 事务开启后的所有待执行的命令
	abort      bool              // 事务中的命令是否有语法错误
	watching   map[string]uint32 //乐观锁
	selectedDB int               // 客户端当前选中的db
	role       int32
}

func NewConnection(conn net.Conn) *RedisClientConnection {
	return &RedisClientConnection{
		conn: conn,
	}
}

func (c *RedisClientConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *RedisClientConnection) Close() error {
	// 等待客户端数据发送完毕或超时
	// 如果数据还未发送完毕，则此时c.wg.Wait()会阻塞，因此在客户端发送数据前使用了c.wg.Add(1)
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

func (c *RedisClientConnection) Write(bytes []byte) error {
	if len(bytes) == 0 {
		return nil
	}
	// TODO 写锁 保证命令写回客户端的完整？
	c.mu.Lock()
	// 超时等待 这里的目的是为了在关闭redis服务器的时候，能够先保证处理完向客户端发送的数据 再关闭连接
	// 因为只要这里的waitingReply里Add和Done不配对，c.waitingReply.Wait()就会阻塞住， 直到对应次数的Done()被调用
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()
	_, err := c.conn.Write(bytes)
	return err
}

func (c *RedisClientConnection) SetPassword(s string) {
	//TODO implement me
}

func (c *RedisClientConnection) GetPassword() string {
	//TODO implement me
	return ""
}

func (c *RedisClientConnection) Subscribe(channel string) {
	//TODO implement me

}

func (c *RedisClientConnection) UnSubscribe(channel string) {
	//TODO implement me

}

func (c *RedisClientConnection) SubsCount() int {
	//TODO implement me
	return 0
}

func (c *RedisClientConnection) GetChannels() []string {
	//TODO implement me
	return nil
}

func (c *RedisClientConnection) InMultiState() bool {
	return c.multiState
}

func (c *RedisClientConnection) SetMultiState(multiState bool) {
	c.multiState = multiState
}

func (c *RedisClientConnection) GetAbort() bool {
	return c.abort
}

func (c *RedisClientConnection) SetAbort(abort bool) {
	c.abort = abort
}

func (c *RedisClientConnection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

func (c *RedisClientConnection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)

}

func (c *RedisClientConnection) ClearQueuedCmds() {
	c.queue = nil
}

func (c *RedisClientConnection) GetWatching() map[string]uint32 {
	return c.watching
}

func (c *RedisClientConnection) GetDBIndex() int {
	return c.selectedDB
}

func (c *RedisClientConnection) SelectDB(dbIndex int) {
	c.selectedDB = dbIndex
}

func (c *RedisClientConnection) GetRole() int32 {
	//TODO implement me
	return 0
}

func (c *RedisClientConnection) SetRole(i int32) {
	//TODO implement me
}

// FakeConnection 目前是aof持久化在做恢复的时候使用 构建一个非真实客户端连接来根据aof文件的命令读写数据库以达到数据恢复的目的
// 也可以作为test使用
type FakeConnection struct {
	RedisClientConnection
	buffer bytes.Buffer
}

// Write TODO pubsub使用
func (c *FakeConnection) Write(bytes []byte) error {
	_, err := c.buffer.Write(bytes)
	return err
}

// Bytes TODO pubsub使用
func (c *FakeConnection) Bytes() []byte {
	return c.buffer.Bytes()
}

// Clean TODO pubsub使用
func (c *FakeConnection) Clean() {
	c.buffer.Reset()
}

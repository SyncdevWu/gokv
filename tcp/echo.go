package tcp

import (
	"bufio"
	"context"
	"gokv/lib/sync/atomic"
	"gokv/lib/sync/wait"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// EchoHandler 输出收到的消息给客户端 主要用于测试
type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}

// NewEchoHandler 创建一个EchoHandler实例
func NewEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

// Handle 服务端处理函数
func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	// 服务端已经调用了Handler的Close函数了
	if h.closing.Get() {
		// handler 正在关闭 拒绝新连接
		_ = conn.Close()
		return
	}
	// 保存连接
	client := &EchoClient{
		Conn: conn,
	}
	// 保存在map中 用Map来代替Set
	h.activeConn.Store(client, struct{}{})
	// 读取conn中的数据
	reader := bufio.NewReader(conn)
	for {
		// 可能的错误: client EOF, client timeout, server early close
		// 以'\n'为分隔符解决粘包拆包问题 reader.ReadString会阻塞到发现'\n'才返回
		msg, err := reader.ReadString('\n')
		if err != nil {
			// 流末尾 EOF的时候应该是客户端已经尝试关闭连接了，所以在这调用client.Close() 否则被动关闭方会出于Close Wait状态
			if err == io.EOF {
				log.Printf("connection close")
				h.activeConn.Delete(client)
				_ = client.Close()
			} else {
				log.Printf("%v", err)
			}
			return
		}
		// 向客户端发送数据前先设置Waiting状态，防止连接被关闭
		client.Waiting.Add(1)
		b := []byte(msg)
		_, _ = conn.Write(b)
		// 发送完毕，结束Waiting
		// 如果这里write时间过长，而client.Close()已经达到超时时间退出后，其内部调用的go routine会短暂的泄漏
		// 此时gone routine 阻塞在 w.wg.Wait()
		// 但是超时后Conn.Close()会得到执行，得关闭了连接，conn.Write会返回err
		// 接着会执行Done() 使得 go routine 从 w.wg.Wait() 处返回
		client.Waiting.Done()
	}
}

func (h *EchoHandler) Close() error {
	log.Printf("handler shutting down...")
	// 设置Handler关闭中标志位
	h.closing.Set(true)
	h.activeConn.Range(func(key, value any) bool {
		client, ok := key.(*EchoClient)
		if ok {
			_ = client.Close()
		}
		h.activeConn.Delete(key)
		return true
	})
	return nil
}

// EchoClient 用于EchoHandler的客户端 主要用于测试
type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

// Close 关闭connection
func (c *EchoClient) Close() error {
	// 等待客户端数据发送完毕或超时
	// 如果数据还未发送完毕，则此时c.wg.Wait()会阻塞，因此在客户端发送数据前使用了c.wg.Add(1)
	c.Waiting.WaitWithTimeout(10 * time.Second)
	_ = c.Conn.Close()
	return nil
}

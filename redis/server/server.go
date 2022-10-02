package server

import (
	"context"
	"gokv/config"
	"gokv/interface/redis"
	"gokv/lib/sync/atomic"
	"gokv/redis/client"
	"gokv/redis/database"
	_ "gokv/redis/exec"
	"gokv/redis/parser"
	"gokv/redis/protocol"
	"io"
	"net"
	"strings"
	"sync"

	"go.uber.org/zap"
)

// Handler 抽象redis服务器 实现了tcp.Handler接口
type Handler struct {
	db         redis.DB       // 持有一个MDB
	activeConn sync.Map       // map代替set
	closing    atomic.Boolean // 正在关闭的标识符 当调用了Handler.Close()方法会被设为true 此后会拒绝所有新的连接
}

func NewHandler() *Handler {
	var mdb redis.DB
	var cfg = config.Conf
	if cfg.Self != "" && len(cfg.Peers) > 0 {
		// TODO 集群
	} else {
		// mdb *MultiDB 是一个多数据库的db
		mdb = database.NewStandaloneServer()
	}
	return &Handler{
		db: mdb,
	}
}

// closeClient 关闭连接
func (h *Handler) closeClient(client *client.RedisClientConnection) {
	// 底部会等待服务端向客户端发送数据完成或超时后才关闭net.Conn
	_ = client.Close()
	h.db.AfterClientClose(client)
	// 从map中移除
	h.activeConn.Delete(client)
}

// Handle tcp连接建立后 每个连接实际上的处理函数
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// Redis服务器正在关闭中, 拒绝新连接
		_ = conn.Close()
		return
	}
	// 将tcp的conn封装成redis.Connection 这个客户端连接会记录这个客户端当前选中的db 或者开启事务时的命令队列
	clientConn := client.NewConnection(conn)
	// 保存该连接 activeConn是拿map当set用
	h.activeConn.Store(clientConn, struct{}{})
	// 解析客户端发送过来的命令 子协程解析后的命令会通过发送给ch ParseStream只会返回一个只读chan
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if err := payload.Err; err != nil {
			// 客户端关闭连接 服务器也要关闭连接 否则被动关闭方会卡在close wait状态
			if err == io.EOF ||
				err == io.ErrUnexpectedEOF ||
				strings.Contains(err.Error(), "use of closed network connection") {
				h.closeClient(clientConn)
				zap.L().Info("connection closed: " + clientConn.RemoteAddr().String())
				return
			}
			// reply err
			errReply := protocol.NewProtocolErrReply(err.Error())
			// 向客户端发送reply
			err = clientConn.Write(errReply.ToBytes())
			// 发生IO错误 关闭连接
			if err != nil {
				h.closeClient(clientConn)
				return
			}
			continue
		}
		// nil返回 客户端发送过来的命令解析成空
		if payload.Data == nil {
			zap.L().Warn("Handler.Handle() empty payload")
			continue
		}
		// 所有的客户端命令都是Bulk String数组 所以必须是MultiBulkReply类型 Reply在序列化(ToBytes())的时候会转换成RESP格式的字符串
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			zap.L().Warn("require multi bulk reply")
			continue
		}
		// 调用数据库引擎执行 Args 是具体的参数 result是服务器执行后的相应 用于发送给客户端
		// r.Args 其实就是 类似 set key val 这3个字符串被转成了[][]byte
		result := h.db.Exec(clientConn, r.Args)
		if result != nil {
			_ = clientConn.Write(result.ToBytes())
		} else {
			_ = clientConn.Write(protocol.NewUnknownErrReply().ToBytes())
		}
	}
}

func (h *Handler) Close() error {
	zap.L().Info("handler shutting down...")
	// 设置正在关闭标志位 标志位为true则后续Handler不会与新的请求建立连接
	h.closing.Set(true)
	// 关闭所有客户端连接 但是会等服务端向客户端数据发送完毕才关闭或者达到了超时时间
	h.activeConn.Range(func(key, value any) bool {
		clientConn, ok := key.(*client.RedisClientConnection)
		if ok {
			_ = clientConn.Close()
		}
		return true
	})
	// 关闭mdb数据库
	h.db.Close()
	return nil
}

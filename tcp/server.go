package tcp

import (
	"context"
	"gokv/interface/tcp"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Address string
	MaxConn uint32
	TimeOut time.Duration
}

// ListenAndServe 启动tcp服务器 持有redis server Handler 实现了 tcp.Handler接口 handler其实就是tcp连接建立后 需要做的事
func ListenAndServe(cfg *Config, handler tcp.Handler) error {
	// tcp服务器关闭channel
	closeChan := make(chan struct{})
	// 中断信号关闭channel
	signalChan := make(chan os.Signal)
	// 中断信号监听
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		// 阻塞等待中断信号
		sig := <-signalChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			// 发送tcp服务器关闭信号 准备进行tcp服务器优雅关闭
			closeChan <- struct{}{}
		}
	}()
	// tcp服务器端口监听
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		log.Fatalf("listen err: %v", err)
		return err
	}
	log.Printf("bind port%s, start listening...", cfg.Address)
	listenAndServeInternal(listener, handler, closeChan)
	return nil
}

// listenAndServeInternal 具有优雅关闭的tcp连接处理
func listenAndServeInternal(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// 等待close信号 一旦接收到信号 则开始优雅退出
	go func() {
		<-closeChan
		log.Printf("shutting down....")
		// 停止监听客户端的连接请求 但是已经建立的连接不会被关闭
		_ = listener.Close()
		// redis服务器关闭
		_ = handler.Close()
	}()

	// panic时关闭资源 作为一个兜底
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()

	// wg用来等待所有连接处理完毕
	var wg sync.WaitGroup
	for {
		// 建立连接
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		log.Printf("accpet connection")
		// 与wg.Done()对应 即必须等到所有的连接都处理完才会执行wg.Done() 外面
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()
			// 处理客户端连接
			handler.Handle(ctx, conn)
		}()
	}
	// 阻塞在这 除非wg为0了才会返回
	wg.Wait()
}

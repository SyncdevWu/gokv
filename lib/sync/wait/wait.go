package wait

import (
	"context"
	"sync"
	"time"
)

// Wait 封装了WaitGroup 拓展了超时等待功能
type Wait struct {
	wg sync.WaitGroup
}

func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

func (w *Wait) Done() {
	w.wg.Done()
}

func (w *Wait) Wait() {
	w.wg.Wait()
}

// WaitWithTimeout blocks until the WaitGroup counter is zero or timeout
// returns true if timeout
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	// 这里必须使用带缓冲区的chan
	// 如果使用非缓冲的，可能发生go routine泄漏
	// 1. 如果是在10s内客户端发送数据完成，go routine的w.wg.Wait() 会先返回 然后向通道c发送数据，此时select能接收到通道c的数据 正常返回
	// 2. 如果是超时退出，则select代码块会先退出，然后待客户端发送数据完成，go routine的w.wg.Wait() 会先返回 然后向通道c发送数据，此时没有接收者，会永远阻塞在这 导致内存泄漏
	c := make(chan bool, 1)
	go func() {
		defer close(c)
		w.Wait()
		c <- true
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func (w *Wait) WaitWithTimeoutContext(timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	go func() {
		defer cancel()
		w.Wait()
	}()

	select {
	case <-ctx.Done():
		switch ctx.Err() {
		case context.DeadlineExceeded:
			return true
		case context.Canceled:
			return false
		default:
			return false
		}
	}
}

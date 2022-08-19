package tcp

import (
	"bufio"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestListenAndServe(t *testing.T) {

	cfg := &Config{
		Address: ":8000",
		MaxConn: 10,
		TimeOut: time.Minute,
	}
	// 启动服务端
	go ListenAndServe(cfg, NewEchoHandler())
	// 保证服务端启动完毕
	time.Sleep(time.Second * 2)
	// 客户端连接
	conn, err := net.Dial("tcp", cfg.Address)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 10; i++ {
		val := strconv.Itoa(rand.Int())
		_, err = conn.Write([]byte(val + "\n"))
		if err != nil {
			t.Error(err)
			return
		}
		bufReader := bufio.NewReader(conn)
		line, _, err := bufReader.ReadLine()
		if err != nil {
			t.Error(err)
			return
		}
		assert.Equal(t, string(line), val)
	}
	_ = conn.Close()

	//for i := 0; i < 5; i++ {
	//	// create idle connection
	//	_, _ = net.Dial("tcp", cfg.Address)
	//}

	select {}
}

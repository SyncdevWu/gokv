package parser

import (
	"bufio"
	"bytes"
	"errors"
	"gokv/interface/redis"
	"gokv/redis/protocol"
	"io"
	"log"
	"runtime/debug"
	"strconv"
	"strings"
)

var NoProtocol = errors.New("reply error")

const (
	CR             = '\r'
	LF             = '\n'
	CRLF           = "\r\n"
	Star           = '*'
	Dollar         = '$'
	Positive       = '+'
	Negative       = '-'
	Colon          = ':'
	NullBulkHeader = -1
)

type Payload struct {
	Data redis.Reply
	Err  error
}

type readState struct {
	readingRepl       bool
	readingMultiLine  bool     // 是否正在读取BulkString数组
	msgType           byte     // 消息类型 * $
	expectedArgsCount int64    // 参数数量3 Set key value => *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
	bulkLen           int64    // bulk String的长度
	args              [][]byte // 参数slice
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && int64(len(s.args)) == s.expectedArgsCount
}

// ParseStream 通过 io.Reader 读取数据并将结果通过 channel 将结果返回给调用者 适合供客户端/服务端使用
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parseInternal(reader, ch)
	return ch
}

// ParseBytes 从字节数组中读取全部数据并返回
func ParseBytes(data []byte) (result []redis.Reply, err error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parseInternal(reader, ch)
	// for range遍历channel需要注意读写都要在子go routine中
	// 如果写在子go routine 而读在主go routine 且ch没有被关闭 则会发生死锁
	for payload := range ch {
		if payload == nil {
			return nil, NoProtocol
		}
		if payload.Err != nil {
			return nil, payload.Err
		}
		result = append(result, payload.Data)
	}
	return result, nil
}

// ParseOne 从字节数组中读取一条数据并返回
func ParseOne(data []byte) (result redis.Reply, err error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parseInternal(reader, ch)
	payload := <-ch
	if payload == nil {
		return nil, NoProtocol
	}
	if payload.Err != nil {
		return nil, payload.Err
	}
	return payload.Data, nil
}

// parseInternal 从流中解析数据，并通过channel发送 该函数在子协程中调用
func parseInternal(reader io.Reader, ch chan<- *Payload) {
	// recover
	defer func() {
		if err := recover(); err != nil {
			log.Printf("err: %v %s", err, string(debug.Stack()))
		}
	}()
	// 带有默认缓冲区的流 缓冲区大小4096
	var bufReader = bufio.NewReader(reader)
	var state = readState{}
	var err error
	var msg []byte

	for {
		var ioError bool
		msg, ioError, err = readLine(bufReader, &state)
		if err != nil {
			// io错误直接结束解析 该io错误由readLine中的reader.ReadBytes(LF)返回
			if ioError {
				ch <- &Payload{
					Data: nil,
					Err:  err,
				}
				close(ch)
				return
			}
			// 协议错误，重置readState
			ch <- &Payload{
				Data: nil,
				Err:  err,
			}
			state = readState{}
			continue
		}
		if !state.readingMultiLine {
			// 解析单行 包括非二进制安全字符串、bulk String的第一行以及bulk String数组第一行
			var firstByte = msg[0]
			if firstByte == Star {
				// 数组 Multi Bulk String 包含客户端发送的指令以及lrange等命令响应的格式
				// 解析 Multi Bulk String 第一行
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: err,
					}
					state = readState{}
					continue
				}
				// 空数组 *0\r\n
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &protocol.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else if firstByte == Dollar {
				// 二进制安全的字符串 Bulk String 比如get等命令返回值的格式
				// 解析 Bulk String 第一行
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: err,
					}
					state = readState{}
					continue
				}
				// 空字符串 $-1\r\n
				if state.bulkLen == NullBulkHeader {
					ch <- &Payload{
						Data: &protocol.NullBulkReply{},
					}
					state = readState{}
					continue
				}
			} else {
				// 非二进制安全
				reply, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: reply,
					Err:  err,
				}
				state = readState{}
				continue
			}
		} else {
			// 解析bulk String 包括解析数组中的每一个bulk String 或者 单独的bulk String
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: err,
				}
				state = readState{}
				continue
			}
			// bulk String数组或bulk String解析完成
			if state.finished() {
				var reply redis.Reply
				if state.msgType == Star {
					reply = protocol.NewMultiBulkReply(state.args)
				} else if state.msgType == Dollar && len(state.args) > 0 {
					reply = protocol.NewBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: reply,
				}
				state = readState{}
			}
		}
	}

}

// readLine 从带缓冲的流中读取一行
func readLine(reader *bufio.Reader, state *readState) (msg []byte, ioError bool, err error) {
	// state.bulkLen == 0 表示读取状态刚被初始化过，一般是出现err或者已经完整读完了
	// state.bulkLen只会在readBody和parseBulkHeader两个函数中被修改
	if state.bulkLen == 0 {
		// 非二进制安全的读取一行
		// 如果是非bulk String的话这里就已经成功读取完这行字符串了
		// 如果是bulk String($、*)则这一次的读取是读去$3、*3, $3表示的是字符串的长度 *3表示的是bulk String数组的数量
		// 如果bulkLen != 0 则属于是二进制安全的字符串 不能使用ReadBytes读取 因为可能字符串中包含了'\n'被提前截断
		msg, err = reader.ReadBytes(LF)
		if err != nil {
			return nil, true, err
		}
		// 没读到数据或者不是\r\n(CRLF)结尾 注: msg[len(msg)-1] 是 '\n'
		if len(msg) == 0 || msg[len(msg)-len(CRLF)] != CR {
			return nil, false, NoProtocol
		}
	} else {
		// 一般情况，是需要\r\n(CRLF) 但在RDB和AOF持久化中不需要
		bulkLen := state.bulkLen + int64(len(CRLF))
		// TODO readingRepl的含义
		if state.readingRepl {
			bulkLen -= int64(len(CRLF))
		}
		msg = make([]byte, bulkLen)
		_, err = io.ReadFull(reader, msg)
		if err != nil {
			return nil, true, err
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

// parseMultiBulkHeader 解析 Bulk String 数组的第一行 主要是读取参数个数 并根据该数据初始化readingState
// 这个函数不会修改bulkLen 所以
func parseMultiBulkHeader(msg []byte, state *readState) (err error) {
	var expectedArgsCount uint64
	// 读取数组有多少元素
	// 形如*3\r\n格式
	// 如果是 *-3 则此处err != nil
	expectedArgsCount, err = strconv.ParseUint(string(msg[1:len(msg)-len(CRLF)]), 10, 64)
	if err != nil {
		return NoProtocol
	}
	// expectedArgsCount >= 0
	// *0\r\n 空数组
	if expectedArgsCount == 0 {
		state.expectedArgsCount = 0
	} else if expectedArgsCount > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int64(expectedArgsCount)
		state.args = make([][]byte, 0, expectedArgsCount)
	}
	return nil
}

// parseBulkHeader 解析单独的Bulk String的长度 这个函数会修改bulkLen
func parseBulkHeader(msg []byte, state *readState) (err error) {
	// $3\r\n
	var bulkLen int64
	bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-len(CRLF)]), 10, 64)
	if err != nil {
		return NoProtocol
	}
	state.bulkLen = bulkLen
	// $-1 表示get时找不到key 空返回
	if state.bulkLen == NullBulkHeader {
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
	} else {
		return NoProtocol
	}
	return nil
}

func parseSingleLineReply(msg []byte) (reply redis.Reply, err error) {
	str := strings.Trim(string(msg), CRLF)
	status := str[1:]
	switch msg[0] {
	case Positive:
		// +OK
		reply = protocol.NewStatusReply(status)
	case Negative:
		// -ERR
		reply = protocol.NewErrReply(status)
	case Colon:
		// :1
		num, err := strconv.ParseInt(status, 10, 64)
		if err != nil {
			return nil, NoProtocol
		}
		reply = protocol.NewIntReply(num)
	default:
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, val := range strs {
			args[i] = []byte(val)
		}
		reply = protocol.NewMultiBulkReply(args)
	}
	return reply, nil
}

func readBody(msg []byte, state *readState) (err error) {
	str := msg[:len(msg)-len(CRLF)]
	if str[0] == Dollar {
		var bulkLen int64
		bulkLen, err = strconv.ParseInt(string(str[1:]), 10, 64)
		if err != nil {
			return NoProtocol
		}
		state.bulkLen = bulkLen
		// $-1 NullBulkString null
		// ["hello",nil,"world"] 在数组中也可能存在nil nil也表示 $-1\r\n
		// $0 Empty String 空串
		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, str)
	}
	return nil
}

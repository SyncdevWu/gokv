package aof

import (
	"go.uber.org/zap"
	"gokv/config"
	"gokv/interface/redis"
	"gokv/redis/client"
	"gokv/redis/parser"
	"gokv/redis/protocol"
	"gokv/utils"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	aofQueueSize    = 1 << 16
	aofSyncEverysec = "everysec"
	aofSyncAlways   = "always"
	aofSyncNo       = "no"
)

type CmdLine = [][]byte

type PayLoad struct {
	cmdLine CmdLine
	dbIndex int
}

type Handler struct {
	db                    redis.EmbedDB        // mdb
	tmpDBMaker            func() redis.EmbedDB // 返回一个基本功能的DB 用于aof重写
	aofChan               chan *PayLoad        // 用于发送command命令
	aofFile               *os.File             // aof文件
	aofFileName           string               // aof文件路径
	aofSyncMode           string               // aof落盘模式
	aofSyncTicker         *time.Ticker         // aof每秒落盘ticker
	aofSyncTickerStopChan chan struct{}        // aof每秒落盘ticker关闭chan
	aofFinishedChan       chan struct{}        // 通知主协程aof任务完成
	pausingAof            sync.RWMutex         // aof重写开始、结束时暂停aof日志，同时也用与保证aof日志一条命令写入的完整
	currentDBIndex        int                  // 当前db
}

func NewAofHandler(db redis.EmbedDB, tmpDBMaker func() redis.EmbedDB) (*Handler, error) {
	aofFileName := config.Conf.AppendFilename
	if aofFileName == "" {
		zap.L().Fatal("aof file path must not null")
	}
	// 这里不能先打开文件 因为后面需要进行数据恢复
	handler := &Handler{
		db:          db,
		aofFileName: aofFileName,
		tmpDBMaker:  tmpDBMaker,
	}
	// 0表示全部读取 启动redis时恢复aof持久化保存的数据
	handler.LoadAof(0)
	// 无文件时创建新文件 有文件时追加内容
	aofFile, err := os.OpenFile(aofFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		zap.L().Error("aof file path must not null")
		return nil, err
	}
	handler.aofFile = aofFile
	// 落盘模式
	handler.aofSyncMode = config.Conf.AppendMode
	// 每秒落盘
	if aofSyncEverysec == handler.aofSyncMode {
		handler.aofSyncTicker = time.NewTicker(time.Second)
		handler.aofSyncTickerStopChan = make(chan struct{})
		go func() {
			handler.everySecSync()
		}()
	}
	// 必须是带有缓冲的channel
	handler.aofChan = make(chan *PayLoad, aofQueueSize)
	go func() {
		handler.handlerAof()
	}()
	return handler, nil
}

func (handler *Handler) LoadAof(maxBytes int) {
	// 临时删除aofChan 防止重复写
	aofChan := handler.aofChan
	handler.aofChan = nil
	defer func(aofChan chan *PayLoad) {
		handler.aofChan = aofChan
	}(aofChan)

	// 只读
	file, err := os.Open(handler.aofFileName)
	// 第一次启动服务器应该是没有aof文件的
	// NewAofHandler的LoadAof后才创建了aof文件
	if err != nil {
		// 不存在aof文件
		if _, ok := err.(*os.PathError); ok {
			return
		}
		// 其他错误
		zap.L().Error("Aof Handler LoadAof() ", zap.Error(err))
		return
	}
	defer file.Close()

	var reader io.Reader
	// maxBytes 为从文件流中读取的最大字节数
	if maxBytes > 0 {
		reader = io.LimitReader(reader, int64(maxBytes))
	} else {
		reader = file
	}
	// 解析aof文件中的命令
	ch := parser.ParseStream(reader)
	conn := &client.FakeConnection{}
	for payload := range ch {
		if err = payload.Err; err != nil {
			if err == io.EOF {
				break
			}
			// 协议错误
			zap.L().Error("Aof Handler LoadAof() ", zap.Error(err))
			continue
		}
		if payload.Data == nil {
			zap.L().Warn("Aof Handler LoadAof() empty payLoad")
			continue
		}
		reply, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			zap.L().Error("Aof Handler LoadAof() require multi bulk reply")
			continue
		}
		result := handler.db.Exec(conn, reply.Args)
		if protocol.IsErrorReply(result) {
			zap.L().Error("Aof Handler LoadAof() db exec " + result.(redis.ErrorReply).Error())
		}
	}
}

func (handler *Handler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Conf.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &PayLoad{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

// everySecSync 每秒刷新
func (handler *Handler) everySecSync() {
	if handler.aofSyncTicker == nil {
		return
	}
	defer handler.aofSyncTicker.Stop()
	for {
		// 当ticker还没到时间间隔的时候会被阻塞住 如果这个时候aofSyncTickerStopChan收到了数据则就直接退出 并会调用ticker.Stop
		select {
		case <-handler.aofSyncTicker.C:
			_ = handler.aofFile.Sync()
		case <-handler.aofSyncTickerStopChan:
			return
		}
	}
}

func (handler *Handler) handlerAof() {
	// 默认选中db0 因为对于每个客户端它们的当前dbIndex都是不同的 但是conn里的index的字段默认也是0 因此在还原的时候就能正确恢复到db0上
	// 如果命令的db于aof记录的当前的db不同 持久化之前要加上切换成他们当前的db 在后续数据恢复的时候才能恢复到对应的db
	handler.currentDBIndex = 0
	for p := range handler.aofChan {
		// 加读锁 防止其他协程暂停aof操作 如aof重写的开始或完成
		handler.pausingAof.RLock()
		if p.dbIndex != handler.currentDBIndex {
			// 切换数据库
			multiBulk := protocol.NewMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(multiBulk)
			if err != nil {
				zap.L().Warn("Aof Handler handlerAof ", zap.Error(err))
				continue
			}
			handler.currentDBIndex = p.dbIndex
		}
		// 具体的命令
		multiBulk := protocol.NewMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(multiBulk)
		if err != nil {
			zap.L().Warn("Aof Handler handlerAof ", zap.Error(err))
			continue
		}
		// 每指令落盘
		if aofSyncAlways == handler.aofSyncMode {
			_ = handler.aofFile.Sync()
		}
		handler.pausingAof.RUnlock()
	}
	// 通知主协程已经完成aof
	handler.aofFinishedChan <- struct{}{}
}

func (handler *Handler) Close() {
	// Close函数的关闭必须保证其他的发送方已经不在发送
	// 不然关闭后 还有发送者向其发送消息会panic
	// 此Close函数目前只会在db Close时调用
	// 而db Close则在所有活跃客户端连接关闭后才调用 因此能保证所有没有新的发送者向其aofChan发送消息
	if handler.aofFile != nil {
		close(handler.aofChan)
		<-handler.aofFinishedChan
		err := handler.aofFile.Close()
		if err != nil {
			zap.L().Warn("Aof Handler Close ", zap.Error(err))
		}
		if handler.aofSyncTicker != nil && handler.aofSyncTickerStopChan != nil {
			// 通知子协程退出对ticker的监听
			handler.aofSyncTickerStopChan <- struct{}{}
			close(handler.aofSyncTickerStopChan)
		}
	}
}

package aof

import (
	"go.uber.org/zap"
	"gokv/config"
	"gokv/interface/redis"
	"gokv/redis/protocol"
	"gokv/utils"
	"os"
	"path"
	"strconv"
	"time"
)

type Context struct {
	tmpFile        *os.File // 临时的aof文件
	fileSize       int64    // aof文件大小
	currentDBIndex int      // 在启动aof重写时 服务器被选中的数据库
}

func (handler *Handler) newRewriteHandler() *Handler {
	return &Handler{
		db:      handler.tmpDBMaker(), // 只具有基本功能的db 用来aof重写用 aof文件的命令会加载到这个db中
		aofFile: handler.aofFile,      // 这里传入旧的aofFileName用来构建旧的内存块来替代原先redis的fork
	}
}

func (handler *Handler) Rewrite() error {
	context, err := handler.StartRewrite()
	if err != nil {
		return err
	}
	err = handler.DoRewrite(context)
	if err != nil {
		return err
	}
	err = handler.FinishRewrite(context)
	if err != nil {
		return err
	}
	return nil
}

func (handler *Handler) StartRewrite() (*Context, error) {
	handler.pausingAof.Lock()
	defer handler.pausingAof.Unlock()
	// 落盘操作
	err := handler.aofFile.Sync()
	if err != nil {
		zap.L().Warn("Rewrite StartRewrite() aof file sync failed", zap.Error(err))
		return nil, err
	}
	// 读取下aof文件大小
	stat, _ := os.Stat(handler.aofFileName)
	fileSize := stat.Size()
	// 创建tmp.aof 作为新的aof文件 最后会用该文件替换redis.aof
	tmpFile, err := os.CreateTemp(path.Dir(handler.aofFileName), "*.aof")
	if err != nil {
		zap.L().Warn("Rewrite StartRewrite() temp file create failed", zap.Error(err))
		return nil, err
	}
	return &Context{
		tmpFile:        tmpFile,
		fileSize:       fileSize,
		currentDBIndex: handler.currentDBIndex,
	}, nil
}

func (handler *Handler) DoRewrite(context *Context) error {
	tmpFile := context.tmpFile
	//
	rewriteHandler := handler.newRewriteHandler()
	// 只读取aof重写前的数据 把这些数据加载到tmp数据库中
	rewriteHandler.LoadAof(int(context.fileSize))
	// 根据tmp数据库中内存数据生成新的aof文件
	for i := 0; i < config.Conf.Databases; i++ {
		// 切换数据库
		_, err := tmpFile.Write(protocol.NewMultiBulkReply(utils.ToCmdLine2("SELECT", strconv.Itoa(i))).ToBytes())
		if err != nil {
			zap.L().Error("Rewrite DoRewrite() write failed", zap.Error(err))
			return err
		}
		rewriteHandler.db.ForEach(i, func(key string, data *redis.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, data)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				if cmd = NewExpireCmd(key, *expiration); cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}

func (handler *Handler) FinishRewrite(context *Context) error {
	// 暂停原aof文件的写入
	handler.pausingAof.Lock()
	defer handler.pausingAof.Unlock()
	// 以只读方式打开原aof文件
	srcFile, err := os.Open(handler.aofFileName)
	if err != nil {
		zap.L().Error("Handler FinishRewrite() open src file failed ", zap.Error(err))
		return err
	}
	defer srcFile.Close()
	// 从aof重写后开始读取
	_, err = srcFile.Seek(context.fileSize, 0)
	if err != nil {
		zap.L().Error("Handler FinishRewrite() seek failed ", zap.Error(err))
		return err
	}
	// 切换数据库为重写开始时的被选中的数据库
	tmpFile := context.tmpFile
	data := protocol.NewMultiBulkReply(utils.ToCmdLine2("SELECT", strconv.Itoa(context.currentDBIndex))).ToBytes()
	_, err = tmpFile.Write(data)
	if err != nil {
		zap.L().Error("Handler FinishRewrite() write failed ", zap.Error(err))
		return err
	}
	// 从原aof文件拷贝在重写期间接收到的命令
	_, err = tmpFile.ReadFrom(srcFile)
	if err != nil {
		zap.L().Error("Handler FinishRewrite() copy failed ", zap.Error(err))
		return err
	}
	// 关闭aof持久化中的aof文件
	_ = handler.aofFile.Close()
	// move操作用tmpFile覆盖aofFile
	_ = os.Rename(tmpFile.Name(), handler.aofFileName)
	// 打开新的aof文件 并让handler持有
	aofFile, err := os.OpenFile(handler.aofFileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		zap.L().Panic("Handler FinishRewrite() open new aof file failed ", zap.Error(err))
		return err
	}
	handler.aofFile = aofFile

	// 额外加一条切换数据库的命令 保证跟当前服务器被选中的数据库一致
	data = protocol.NewMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(handler.currentDBIndex))).ToBytes()
	_, err = handler.aofFile.Write(data)
	if err != nil {
		zap.L().Panic("Handler FinishRewrite() change db failed ", zap.Error(err))
		return err
	}
	return nil
}

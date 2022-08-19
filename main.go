package main

import (
	"fmt"
	"gokv/config"
	"gokv/logger"
	"gokv/redis/server"
	"gokv/tcp"

	"go.uber.org/zap"
)

const banner = `
   _____       _  ____      __
  / ____|     | |/ /\ \    / /
 | |  __  ___ | ' /  \ \  / / 
 | | |_ |/ _ \|  <    \ \/ /  
 | |__| | (_) | . \    \  /   
  \_____|\___/|_|\_\    \/
`

func main() {
	fmt.Printf(banner)
	// 加载配置
	if err := config.Init(); err != nil {
		fmt.Printf("init config failed, err:%v\n", err)
		return
	}
	// 初始化日志
	if err := logger.Init(config.Conf.LogConfig); err != nil {
		fmt.Printf("init logger failed, err:%v\n", err)
		return
	}
	defer zap.L().Sync()
	if err := tcp.ListenAndServe(
		&tcp.Config{Address: fmt.Sprintf("%s:%d", config.Conf.Bind, config.Conf.Port)},
		server.NewHandler(),
	); err != nil {
		zap.L().Error("ListenAndServe() failed ", zap.Error(err))
	}
}

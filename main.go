package main

import (
	"goGameServer/netlayer"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func registerHandlers(server *netlayer.Server) {
	mgr := server.SessionManager()
	router := server.Router()
	handlerMgr := netlayer.NewServiceHandler(mgr)

	router.Register(netlayer.MsgIDLogin, handlerMgr.HandleLogin)
	router.Register(netlayer.MsgIDEcho, handlerMgr.HandleEcho)
}

func main() {
	// 1. 创建服务器
	router := netlayer.NewRouter()
	server := netlayer.NewServer(netlayer.DefaultConfig(), router)

	// 2. 注册业务 handler（登录、聊天等）
	registerHandlers(server)

	// 3. 启动
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}

	// 4. 等待中断信号
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	// 5. 优雅关闭
	server.Stop()
}

package main

import (
	"goGameServer/actorcore"
	"goGameServer/netlayer"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// registerRoutes 注册消息路由表：MsgID → Actor
func registerRoutes(gate *netlayer.Gate) {
	// 登录 → 命名服务 "login"（Phase 3 实现 login service）
	gate.MustRegisterRoute(netlayer.MsgIDLogin, netlayer.MsgRoute{
		Target:  netlayer.TargetNamed,
		Name:    "login",
		Decoder: netlayer.MakeDecoder(func() any { return &netlayer.LoginMessage{} }),
	})

	// 回显 → 玩家 Actor（Phase 3 注册 player handler）
	gate.MustRegisterRoute(netlayer.MsgIDEcho, netlayer.MsgRoute{
		Target:  netlayer.TargetPlayer,
		Decoder: netlayer.MakeDecoder(func() any { return &netlayer.EchoMessage{} }),
	})
}

func main() {
	// 1. 获取 ActorManager 单例
	mgr := actorcore.GetActorManager()

	// 2. 创建 Gate 网关（netlayer → actorcore 单向依赖）
	gate := netlayer.NewGate(mgr)

	// 3. 创建服务器
	server := netlayer.NewServer(netlayer.DefaultConfig(), gate)

	// 4. 注册消息路由
	registerRoutes(gate)

	// 5. 启动
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}

	// 6. 等待中断信号
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	// 7. 优雅关闭
	server.Stop()
	mgr.Stop()
}

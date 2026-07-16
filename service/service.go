// Package service 实现业务服务层。
// 包含命名服务（登录、排行榜等）和玩家 Actor 的消息处理器。
package service

import (
	"log"

	"goGameServer/actorcore"
	"goGameServer/netlayer"
)

// Init 初始化所有业务服务，在服务器启动前调用。
//   - 创建 login 命名服务（串行处理，workers=1）
//   - 注册 PlayerActor 全局 handler（echo 等）
func Init(mgr *actorcore.ActorManager, sessMgr *netlayer.SessionManager) {
	// 1. 创建 login 命名服务
	_, loginActor, err := mgr.CreateNamedActor("login", 1)
	if err != nil {
		log.Fatalf("[service] create login service: %v", err)
	}
	loginSvc := NewLoginService(sessMgr, mgr)
	loginActor.MustRegisterHandler(netlayer.MsgIDLogin, loginSvc.HandleLogin)

	// 2. 注册 PlayerActor 全局 handler
	mgr.MustRegisterPlayerHandler(netlayer.MsgIDEcho, HandleEcho)

	log.Println("[service] all services initialized: login, echo")
}

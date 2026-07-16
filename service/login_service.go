package service

import (
	"fmt"
	"log"

	"goGameServer/actorcore"
	"goGameServer/netlayer"
)

// LoginService 登录服务，作为命名服务 Actor 运行（名称 "login"）。
// 处理客户端登录请求：绑定 RoleID → 创建 PlayerActor → 回复 LoginAck。
type LoginService struct {
	sessMgr  *netlayer.SessionManager
	actorMgr *actorcore.ActorManager
}

// NewLoginService 创建登录服务
func NewLoginService(sessMgr *netlayer.SessionManager, actorMgr *actorcore.ActorManager) *LoginService {
	return &LoginService{
		sessMgr:  sessMgr,
		actorMgr: actorMgr,
	}
}

// HandleLogin 处理登录消息。
// 消息流：客户端 → Gate(解码 LoginMessage) → login NamedActor → HandleLogin
func (s *LoginService) HandleLogin(ctx *actorcore.ActorContext, msg *actorcore.ActorMessage) error {
	loginMsg, ok := msg.Data.(*netlayer.LoginMessage)
	if !ok {
		return fmt.Errorf("invalid login message type: %T", msg.Data)
	}

	// Responder 就是客户端的 Session（由 Gate 设置）
	sess, ok := ctx.Responder.(*netlayer.Session)
	if !ok {
		return fmt.Errorf("responder is not a session: %T", ctx.Responder)
	}

	// 1. 绑定 RoleID 到 Session（若已有同 RoleID 的旧 Session 会被顶掉）
	s.sessMgr.BindRole(sess, loginMsg.RoleID)

	// 2. 创建/获取玩家 Actor，绑定 Session 为 Responder
	//    后续该玩家的消息将由 Gate 直接路由到此 PlayerActor
	s.actorMgr.CreatePlayerActor(loginMsg.RoleID, sess)

	log.Printf("[LoginService] login success: roleID=%d, session=%d, remote=%s",
		loginMsg.RoleID, sess.ID(), sess.RemoteAddr())

	// 3. 回复登录成功
	return ctx.Reply(netlayer.MsgIDLoginAck, netlayer.LoginAckMessage{
		Success: true,
		Message: "login ok",
	})
}

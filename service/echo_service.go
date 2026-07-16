package service

import (
	"fmt"

	"goGameServer/actorcore"
	"goGameServer/netlayer"
)

// HandleEcho 处理回显消息，注册为全局 PlayerActor handler。
// 玩家登录后发送 Echo 消息，此 handler 在玩家的 PlayerActor 中串行执行。
// 消息流：客户端 → Gate(解码 EchoMessage) → PlayerActor → HandleEcho → 回复 EchoAck
func HandleEcho(ctx *actorcore.ActorContext, msg *actorcore.ActorMessage) error {
	echoMsg, ok := msg.Data.(*netlayer.EchoMessage)
	if !ok {
		return fmt.Errorf("invalid echo message type: %T", msg.Data)
	}

	return ctx.Reply(netlayer.MsgIDEchoAck, netlayer.EchoAckMessage{
		Text: echoMsg.Text,
	})
}

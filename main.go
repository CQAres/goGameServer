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

	// 登录 handler：解码 → 绑定 RoleID
	router.Register(netlayer.MsgIDLogin, func(sess *netlayer.Session, payload []byte) error {
		var msg netlayer.LoginMessage
		if err := netlayer.DecodePayload(payload, &msg); err != nil {
			return err
		}
		mgr.BindRole(sess, msg.RoleID)
		return sess.Send(netlayer.MsgIDLoginAck, netlayer.LoginAckMessage{
			Success: true,
			Message: "login ok",
		})
	})

	// Echo handler
	router.Register(netlayer.MsgIDEcho, func(sess *netlayer.Session, payload []byte) error {
		var msg netlayer.EchoMessage
		if err := netlayer.DecodePayload(payload, &msg); err != nil {
			return err
		}
		return sess.Send(netlayer.MsgIDEchoAck, netlayer.EchoAckMessage{Text: msg.Text})
	})
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

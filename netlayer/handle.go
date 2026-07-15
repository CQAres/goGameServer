package netlayer

// ServiceHandlerMgr 业务层消息处理器
type ServiceHandlerMgr struct {
	mgr *SessionManager
}

func NewServiceHandler(mgr *SessionManager) *ServiceHandlerMgr {
	return &ServiceHandlerMgr{mgr: mgr}
}

// HandleLogin 登录 handler：解码 → 绑定 RoleID
func (s *ServiceHandlerMgr) HandleLogin(sess *Session, payload []byte) error {
	var msg LoginMessage
	if err := DecodePayload(payload, &msg); err != nil {
		return err
	}
	s.mgr.BindRole(sess, msg.RoleID)
	return sess.Send(MsgIDLoginAck, LoginAckMessage{
		Success: true,
		Message: "login ok",
	})
}

// HandleEcho Echo 回显 handler
func (s *ServiceHandlerMgr) HandleEcho(sess *Session, payload []byte) error {
	var msg EchoMessage
	if err := DecodePayload(payload, &msg); err != nil {
		return err
	}
	return sess.Send(MsgIDEchoAck, EchoAckMessage{Text: msg.Text})
}

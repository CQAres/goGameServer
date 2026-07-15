package netlayer

// MsgID 消息 ID 定义（独立于内部 EventType，由映射表关联）
const (
	MsgIDHeartbeat uint16 = 0 // 心跳
	MsgIDLogin     uint16 = 1 // 登录
	MsgIDLoginAck  uint16 = 2 // 登录回复
	MsgIDEcho      uint16 = 3 // 回显测试
	MsgIDEchoAck   uint16 = 4 // 回显回复
)

// LoginMessage 登录消息（不做鉴权，客户端直接发 RoleID）
type LoginMessage struct {
	RoleID uint64
}

// LoginAckMessage 登录回复
type LoginAckMessage struct {
	Success bool
	Message string
}

// EchoMessage 回显消息
type EchoMessage struct {
	Text string
}

// EchoAckMessage 回显回复
type EchoAckMessage struct {
	Text string
}

// ChatMessage 聊天消息
type ChatMessage struct {
	From    uint64
	To      uint64
	Content string
}

// ChatAckMessage 聊天回复
type ChatAckMessage struct {
	Success bool
	Message string
}

// HeartbeatMessage 心跳消息
type HeartbeatMessage struct {
	Timestamp int64
}

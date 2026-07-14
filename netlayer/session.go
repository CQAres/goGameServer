package netlayer

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// sendItem 发送队列项
type sendItem struct {
	msgID   uint16
	payload []byte
}

// Session 表示一个客户端会话，封装 Connection 并提供异步发送
type Session struct {
	id         uint64
	conn       Connection
	isWS       bool
	roleID     atomic.Uint64 // 登录后设置
	sendQueue  chan sendItem // 异步发送队列
	lastActive atomic.Int64  // unix nano，最后活跃时间
	closeOnce  sync.Once
	done       chan struct{}
	onClose    func(*Session) // 关闭回调
}

// NewSession 创建会话并启动发送协程
func NewSession(id uint64, conn Connection, isWS bool, queueSize int, onClose func(*Session)) *Session {
	s := &Session{
		id:        id,
		conn:      conn,
		isWS:      isWS,
		sendQueue: make(chan sendItem, queueSize),
		done:      make(chan struct{}),
		onClose:   onClose,
	}
	s.lastActive.Store(time.Now().UnixNano())
	go s.sendLoop()
	return s
}

// sendLoop 串行发送，避免并发写 socket
func (s *Session) sendLoop() {
	defer close(s.done)
	for item := range s.sendQueue {
		if err := s.conn.WriteFrame(item.msgID, item.payload); err != nil {
			log.Printf("[Session] write failed: id=%d, roleID=%d, err=%v",
				s.id, s.RoleID(), err)
			return
		}
	}
}

// ID 返回 Session ID
func (s *Session) ID() uint64 { return s.id }

// RoleID 返回玩家 ID（未登录返回 0）
func (s *Session) RoleID() uint64 { return s.roleID.Load() }

// SetRoleID 绑定玩家 ID（登录成功后调用）
func (s *Session) SetRoleID(roleID uint64) { s.roleID.Store(roleID) }

// IsWebSocket 是否为 WebSocket 连接
func (s *Session) IsWebSocket() bool { return s.isWS }

// RemoteAddr 远端地址
func (s *Session) RemoteAddr() string { return s.conn.RemoteAddr() }

// UpdateActive 更新最后活跃时间
func (s *Session) UpdateActive() { s.lastActive.Store(time.Now().UnixNano()) }

// LastActive 最后活跃时间
func (s *Session) LastActive() time.Time {
	return time.Unix(0, s.lastActive.Load())
}

// Send 投递消息到发送队列（非阻塞，队列满时返回错误）
func (s *Session) Send(msgID uint16, payload any) error {
	data, err := EncodePayload(payload)
	if err != nil {
		return err
	}
	select {
	case s.sendQueue <- sendItem{msgID: msgID, payload: data}:
		return nil
	default:
		return errSendQueueFull(s.id, msgID)
	}
}

// Done 返回关闭信号 channel
func (s *Session) Done() <-chan struct{} { return s.done }

// Close 关闭会话（幂等）
func (s *Session) Close() {
	s.closeOnce.Do(func() {
		close(s.sendQueue)
		err := s.conn.Close()
		if err != nil {
		}
		<-s.done
		if s.onClose != nil {
			s.onClose(s)
		}
	})
}

func errSendQueueFull(id uint64, msgID uint16) error {
	return &sendQueueFullError{sessionID: id, msgID: msgID}
}

type sendQueueFullError struct {
	sessionID uint64
	msgID     uint16
}

func (e *sendQueueFullError) Error() string {
	return "send queue full"
}

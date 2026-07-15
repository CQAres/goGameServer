package netlayer

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ErrorCodeSessionClosed = 1
	ErrorCodeSendQueueFull = 2
	ErrorCodeWriteFailed   = 3
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
	done       chan struct{}  // sendLoop 退出信号
	onClose    func(*Session) // 关闭回调
	closeSig   chan struct{}  // 关闭信号
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
		closeSig:  make(chan struct{}),
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

	// 存在竟态窗口，用 recover 兜底，把 panic 转成 error 返回
	defer func() {
		if r := recover(); r != nil {
			err = SendError(s.id, msgID, ErrorCodeSessionClosed)
		}
	}()

	select { // select 是选择一个可执行的case执行
	case <-s.closeSig:
		return SendError(s.id, msgID, ErrorCodeSessionClosed)
	case s.sendQueue <- sendItem{msgID: msgID, payload: data}:
		return nil
	default: // 如果没有 default select会阻塞等待某个case就绪
		return SendError(s.id, msgID, ErrorCodeSendQueueFull)
	}
}

// Done 返回关闭信号 channel
func (s *Session) Done() <-chan struct{} { return s.done }

// Close 关闭会话（幂等）
func (s *Session) Close() {
	s.closeOnce.Do(func() {
		close(s.closeSig) // 先发信号，阻止新的 Send
		close(s.sendQueue)
		<-s.done
		if err := s.conn.Close(); err != nil {
			log.Printf("[Session] Conn close: id=%d, err=%v", s.id, err)
		}
		if s.onClose != nil {
			s.onClose(s)
		}
	})
}

func SendError(id uint64, msgID uint16, code int) error {
	return &NetError{code: code, sessionID: id, msgID: msgID}
}

type NetError struct {
	code      int
	sessionID uint64
	msgID     uint16
}

func (e *NetError) Error() string {
	switch e.code {
	case ErrorCodeSessionClosed:
		return fmt.Sprintf("session closed: sessionID=%d, msgID=%d", e.sessionID, e.msgID)
	case ErrorCodeSendQueueFull:
		return fmt.Sprintf("send queue full: sessionID=%d, msgID=%d", e.sessionID, e.msgID)
	case ErrorCodeWriteFailed:
		return fmt.Sprintf("write failed: sessionID=%d, msgID=%d", e.sessionID, e.msgID)
	default:
		return fmt.Sprintf("unknown error: sessionID=%d, msgID=%d", e.sessionID, e.msgID)
	}
}

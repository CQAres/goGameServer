package actorcore

import (
	"errors"
	"fmt"
	"sync"
)

// Responder 抽象网络回写能力。
// netlayer.Session 在 Phase 2 中实现此接口，
// 避免 actorcore 反向依赖 netlayer（依赖倒置）。
type Responder interface {
	Send(msgID uint16, payload any) error
}

// ActorMessage Actor 间、Gate→Actor 投递的统一消息格式
type ActorMessage struct {
	MsgID     uint16      // 协议消息 ID
	Payload   []byte      // 原始字节（未解码，handler 自行解码或使用 Data）
	Data      any         // 已解码数据（可选，由 Gate 填充，避免重复解码）
	Source    ActorHandle // 消息来源地址（0 表示来自客户端/Gate）
	Responder Responder   // 网络回写通道（nil 表示无需回写）
}

// HandlerFunc Actor 内部消息处理函数签名
type HandlerFunc func(ctx *ActorContext, msg *ActorMessage) error

// ActorContext 消息处理上下文，在 handler 中使用。
// 提供当前 Actor 地址、消息来源、网络回写通道，以及跨 Actor 通信能力。
type ActorContext struct {
	Self      ActorHandle // 当前 Actor 的地址
	Source    ActorHandle // 消息来源地址
	Responder Responder   // 网络回写通道（可能为 nil）
	mgr       *ActorManager
}

// Send 向目标 Actor 投递消息（跨 Actor 通信）
func (ctx *ActorContext) Send(target ActorHandle, msg *ActorMessage) error {
	if ctx.mgr == nil {
		return fmt.Errorf("actor manager not available")
	}
	msg.Source = ctx.Self
	return ctx.mgr.Send(target, msg)
}

// SendToPlayer 向指定玩家的 Actor 投递消息
func (ctx *ActorContext) SendToPlayer(roleID uint64, msg *ActorMessage) error {
	if ctx.mgr == nil {
		return fmt.Errorf("actor manager not available")
	}
	msg.Source = ctx.Self
	return ctx.mgr.SendToPlayer(roleID, msg)
}

// SendToNamed 向命名服务投递消息
func (ctx *ActorContext) SendToNamed(name string, msg *ActorMessage) error {
	if ctx.mgr == nil {
		return fmt.Errorf("actor manager not available")
	}
	msg.Source = ctx.Self
	return ctx.mgr.SendToNamed(name, msg)
}

// Reply 向客户端回复消息（便捷方法，封装 Responder.Send）
func (ctx *ActorContext) Reply(msgID uint16, payload any) error {
	if ctx.Responder == nil {
		return errors.New("no responder available")
	}
	return ctx.Responder.Send(msgID, payload)
}

// HandlerMap 消息 ID -> Handler 的注册表，并发安全
type HandlerMap struct {
	mu       sync.RWMutex
	handlers map[uint16]HandlerFunc
}

// NewHandlerMap 创建 HandlerMap
func NewHandlerMap() *HandlerMap {
	return &HandlerMap{handlers: make(map[uint16]HandlerFunc)}
}

// Register 注册 handler，重复注册返回错误
func (m *HandlerMap) Register(msgID uint16, handler HandlerFunc) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.handlers[msgID]; exists {
		return fmt.Errorf("%w: msgID=%d", ErrHandlerExists, msgID)
	}
	m.handlers[msgID] = handler
	return nil
}

// MustRegister 注册 handler，重复注册 panic
func (m *HandlerMap) MustRegister(msgID uint16, handler HandlerFunc) {
	if err := m.Register(msgID, handler); err != nil {
		panic(err)
	}
}

// Get 获取 handler
func (m *HandlerMap) Get(msgID uint16) (HandlerFunc, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.handlers[msgID]
	return h, ok
}

// Count 返回已注册 handler 数量
func (m *HandlerMap) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.handlers)
}

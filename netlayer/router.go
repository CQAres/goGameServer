package netlayer

import (
	"fmt"
	"sync"
)

// MessageHandler 消息处理函数
// payload 为 gob 编码后的原始字节，由 handler 自行解码
type MessageHandler func(sess *Session, payload []byte) error

// Router 消息路由表，MsgID -> Handler
type Router struct {
	mu       sync.RWMutex
	handlers map[uint16]MessageHandler
}

// NewRouter 创建路由器
func NewRouter() *Router {
	return &Router{handlers: make(map[uint16]MessageHandler)}
}

// Register 注册消息处理器
func (r *Router) Register(msgID uint16, handler MessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[msgID] = handler
}

// Dispatch 分发消息
func (r *Router) Dispatch(sess *Session, msgID uint16, payload []byte) error {
	r.mu.RLock()
	handler, ok := r.handlers[msgID]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no handler for msgID=%d", msgID)
	}
	return handler(sess, payload)
}

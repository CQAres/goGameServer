package netlayer

import (
	"fmt"
	"sync"

	"goGameServer/actorcore"
)

// RouteTarget 路由目标类型
type RouteTarget int

const (
	TargetPlayer RouteTarget = iota // 路由到玩家 Actor（按 Session.RoleID 查找）
	TargetNamed                     // 路由到命名服务 Actor
)

// DecoderFunc 消息解码函数，将原始字节解码为业务数据
type DecoderFunc func([]byte) (any, error)

// MsgRoute 消息路由条目
type MsgRoute struct {
	Target  RouteTarget // 路由目标
	Name    string      // TargetNamed 时指定命名服务名称
	Decoder DecoderFunc // 消息解码器（nil 表示不解码，传递原始字节）
}

// Gate 网关，负责将网络消息路由到对应的 Actor。
// Gate 是纯转发层，不包含任何业务逻辑。
// 依赖方向：netlayer → actorcore（单向），actorcore 不反向依赖 netlayer。
type Gate struct {
	mgr    *actorcore.ActorManager
	mu     sync.RWMutex
	routes map[uint16]MsgRoute
}

// NewGate 创建网关
func NewGate(mgr *actorcore.ActorManager) *Gate {
	return &Gate{
		mgr:    mgr,
		routes: make(map[uint16]MsgRoute),
	}
}

// RegisterRoute 注册消息路由
func (g *Gate) RegisterRoute(msgID uint16, route MsgRoute) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.routes[msgID]; exists {
		return fmt.Errorf("route already registered: msgID=%d", msgID)
	}
	g.routes[msgID] = route
	return nil
}

// MustRegisterRoute 注册消息路由，重复注册 panic
func (g *Gate) MustRegisterRoute(msgID uint16, route MsgRoute) {
	if err := g.RegisterRoute(msgID, route); err != nil {
		panic(err)
	}
}

// Dispatch 分发消息到对应 Actor。
// Session 隐式实现 actorcore.Responder 接口（Send 方法签名一致），
// 作为 ActorMessage.Responder 传递，使 Actor handler 能通过 ctx.Reply() 回写客户端。
func (g *Gate) Dispatch(sess *Session, msgID uint16, payload []byte) error {
	g.mu.RLock()
	route, ok := g.routes[msgID]
	g.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no route for msgID=%d", msgID)
	}

	// 解码消息（如果配置了解码器）
	var data any
	if route.Decoder != nil {
		var err error
		data, err = route.Decoder(payload)
		if err != nil {
			return fmt.Errorf("decode msgID=%d: %w", msgID, err)
		}
	}

	msg := &actorcore.ActorMessage{
		MsgID:     msgID,
		Payload:   payload,
		Data:      data,
		Responder: sess, // Session implements actorcore.Responder
	}

	switch route.Target {
	case TargetPlayer:
		roleID := sess.RoleID()
		if roleID == 0 {
			return fmt.Errorf("player not logged in: msgID=%d", msgID)
		}
		return g.mgr.SendToPlayer(roleID, msg)
	case TargetNamed:
		if route.Name == "" {
			return fmt.Errorf("named route without name: msgID=%d", msgID)
		}
		return g.mgr.SendToNamed(route.Name, msg)
	default:
		return fmt.Errorf("unknown route target: %d", route.Target)
	}
}

// MakeDecoder 创建基于 gob 的消息解码器。
// factory 返回一个新分配的目标类型指针，例如 func() any { return &LoginMessage{} }
func MakeDecoder(factory func() any) DecoderFunc {
	return func(data []byte) (any, error) {
		v := factory()
		if err := DecodePayload(data, v); err != nil {
			return nil, err
		}
		return v, nil
	}
}

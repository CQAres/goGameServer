package actorcore

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const playerQueueSize = 1024

// PlayerActor 玩家 Actor，每个玩家一个实例。
// 串行处理该玩家的所有消息，避免对玩家数据的并发访问。
// 不同玩家的 Actor 互相独立、并行执行。
type PlayerActor struct {
	handle    ActorHandle
	roleID    uint64
	queue     chan *ActorMessage
	handlers  *HandlerMap // 共享的 handler 注册表（来自 ActorManager）
	mgr       *ActorManager
	responder atomic.Value // Responder，支持重连时更新
	closed    atomic.Bool
	closeSig  chan struct{}
	wg        sync.WaitGroup // 跟踪未处理完的消息
	once      sync.Once
	done      chan struct{}
}

// NewPlayerActor 创建并启动玩家 Actor
func NewPlayerActor(handle ActorHandle, roleID uint64, responder Responder, handlers *HandlerMap, mgr *ActorManager) *PlayerActor {
	a := &PlayerActor{
		handle:   handle,
		roleID:   roleID,
		queue:    make(chan *ActorMessage, playerQueueSize),
		handlers: handlers,
		mgr:      mgr,
		closeSig: make(chan struct{}),
		done:     make(chan struct{}),
	}
	if responder != nil {
		a.responder.Store(responder)
	}
	go a.run()
	return a
}

func (a *PlayerActor) run() {
	defer close(a.done)
	for msg := range a.queue {
		a.dispatch(msg)
		a.wg.Done()
	}
}

func (a *PlayerActor) dispatch(msg *ActorMessage) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[PlayerActor] panic: handle=%s, roleID=%d, msgID=%d, panic=%v",
				a.handle, a.roleID, msg.MsgID, r)
		}
	}()

	handler, ok := a.handlers.Get(msg.MsgID)
	if !ok {
		log.Printf("[PlayerActor] no handler: roleID=%d, msgID=%d", a.roleID, msg.MsgID)
		return
	}

	ctx := &ActorContext{
		Self:      a.handle,
		Source:    msg.Source,
		Responder: a.getResponder(),
		mgr:       a.mgr,
	}

	if err := handler(ctx, msg); err != nil {
		log.Printf("[PlayerActor] handler error: roleID=%d, msgID=%d, err=%v",
			a.roleID, msg.MsgID, err)
	}
}

func (a *PlayerActor) getResponder() Responder {
	if v := a.responder.Load(); v != nil {
		return v.(Responder)
	}
	return nil
}

// SetResponder 更新绑定的网络会话（重连场景）
func (a *PlayerActor) SetResponder(r Responder) {
	a.responder.Store(r)
}

// Handle 返回 Actor 地址
func (a *PlayerActor) Handle() ActorHandle { return a.handle }

// RoleID 返回玩家 ID
func (a *PlayerActor) RoleID() uint64 { return a.roleID }

// Post 投递消息到玩家队列（非阻塞）。
// 队列满或 Actor 已关闭时返回错误，不会 panic。
func (a *PlayerActor) Post(msg *ActorMessage) (err error) {
	if a.closed.Load() {
		return fmt.Errorf("%w: handle=%s", ErrActorClosed, a.handle)
	}
	a.wg.Add(1)
	// recover 兜底：closeSig 检查与 channel 写入之间存在竞态窗口，
	// 已关闭的 channel 写入会 panic，用 recover 转为 error 返回。
	defer func() {
		if r := recover(); r != nil {
			a.wg.Done()
			err = fmt.Errorf("%w: handle=%s", ErrActorClosed, a.handle)
		}
	}()
	select {
	case <-a.closeSig:
		a.wg.Done()
		return fmt.Errorf("%w: handle=%s", ErrActorClosed, a.handle)
	case a.queue <- msg:
		return nil
	default:
		a.wg.Done()
		return fmt.Errorf("%w: handle=%s, msgID=%d", ErrQueueFull, a.handle, msg.MsgID)
	}
}

// Wait 等待所有已投递消息处理完成
func (a *PlayerActor) Wait() { a.wg.Wait() }

// Stop 关闭队列并等待 Actor 退出（幂等）
func (a *PlayerActor) Stop() {
	a.once.Do(func() {
		a.closed.Store(true)
		close(a.closeSig) // 先发信号，阻止新的 Post
		close(a.queue)    // 再关闭队列，让 run 循环退出
	})
	<-a.done
}

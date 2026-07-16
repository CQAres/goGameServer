package actorcore

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const namedQueueSize = 4096

// NamedActor 命名服务 Actor，通过名称注册和查找。
// workers=1 时为串行处理（类似 GlobalActor），适用于需要全局有序性的操作。
// workers>1 时为并发处理（类似 IOWorkerPool），适用于 IO 密集型场景。
type NamedActor struct {
	handle   ActorHandle
	name     string
	queue    chan *ActorMessage
	handlers *HandlerMap // 独立的 handler 注册表
	mgr      *ActorManager
	workers  int
	closed   atomic.Bool
	closeSig chan struct{}
	taskWg   sync.WaitGroup // 跟踪未处理完的消息
	workerWg sync.WaitGroup // 跟踪 worker goroutine
	once     sync.Once
}

// NewNamedActor 创建并启动命名服务 Actor
func NewNamedActor(handle ActorHandle, name string, workers int, mgr *ActorManager) *NamedActor {
	if workers < 1 {
		workers = 1
	}
	a := &NamedActor{
		handle:   handle,
		name:     name,
		queue:    make(chan *ActorMessage, namedQueueSize),
		handlers: NewHandlerMap(),
		mgr:      mgr,
		workers:  workers,
		closeSig: make(chan struct{}),
	}
	for range workers {
		a.workerWg.Add(1)
		go a.run()
	}
	return a
}

func (a *NamedActor) run() {
	defer a.workerWg.Done()
	for msg := range a.queue {
		a.dispatch(msg)
		a.taskWg.Done()
	}
}

func (a *NamedActor) dispatch(msg *ActorMessage) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[NamedActor:%s] panic: handle=%s, msgID=%d, panic=%v",
				a.name, a.handle, msg.MsgID, r)
		}
	}()

	handler, ok := a.handlers.Get(msg.MsgID)
	if !ok {
		log.Printf("[NamedActor:%s] no handler: msgID=%d", a.name, msg.MsgID)
		return
	}

	ctx := &ActorContext{
		Self:      a.handle,
		Source:    msg.Source,
		Responder: msg.Responder, // NamedActor 不绑定固定 Responder，使用消息携带的
		mgr:       a.mgr,
	}

	if err := handler(ctx, msg); err != nil {
		log.Printf("[NamedActor:%s] handler error: msgID=%d, err=%v",
			a.name, msg.MsgID, err)
	}
}

// Handle 返回 Actor 地址
func (a *NamedActor) Handle() ActorHandle { return a.handle }

// Name 返回服务名称
func (a *NamedActor) Name() string { return a.name }

// RegisterHandler 注册消息处理器
func (a *NamedActor) RegisterHandler(msgID uint16, handler HandlerFunc) error {
	return a.handlers.Register(msgID, handler)
}

// MustRegisterHandler 注册消息处理器，重复注册 panic
func (a *NamedActor) MustRegisterHandler(msgID uint16, handler HandlerFunc) {
	if err := a.RegisterHandler(msgID, handler); err != nil {
		panic(err)
	}
}

// Post 投递消息到服务队列（非阻塞）。
// 队列满或 Actor 已关闭时返回错误，不会 panic。
func (a *NamedActor) Post(msg *ActorMessage) (err error) {
	if a.closed.Load() {
		return fmt.Errorf("%w: name=%s", ErrActorClosed, a.name)
	}
	a.taskWg.Add(1)
	defer func() {
		if r := recover(); r != nil {
			a.taskWg.Done()
			err = fmt.Errorf("%w: name=%s", ErrActorClosed, a.name)
		}
	}()
	select {
	case <-a.closeSig:
		a.taskWg.Done()
		return fmt.Errorf("%w: name=%s", ErrActorClosed, a.name)
	case a.queue <- msg:
		return nil
	default:
		a.taskWg.Done()
		return fmt.Errorf("%w: name=%s, msgID=%d", ErrQueueFull, a.name, msg.MsgID)
	}
}

// Wait 等待所有已投递消息处理完成
func (a *NamedActor) Wait() { a.taskWg.Wait() }

// Stop 关闭队列并等待所有 worker 退出（幂等）
func (a *NamedActor) Stop() {
	a.once.Do(func() {
		a.closed.Store(true)
		close(a.closeSig)
		close(a.queue)
	})
	a.workerWg.Wait()
}

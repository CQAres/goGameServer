package actorcore

import (
	"fmt"
	"goGameServer/EventMgr/config"
	"sync"
)

// listenerNode 监听器链表节点
type listenerNode struct {
	ListenerEntry
	next *listenerNode
}

// ActorManager Actor 管理器，统一注册监听器和分发事件
// 内部维护三种 Actor：PlayerActor（按 RoleID 路由）、GlobalActor、IOWorkerPool
type ActorManager struct {
	mu           sync.RWMutex
	listeners    [config.EVENT_MAX]*listenerNode
	playerActors sync.Map // map[uint64]*PlayerActor
	globalActor  *GlobalActor
	ioPool       *IOWorkerPool
}

var (
	instance *ActorManager
	mgrOnce  sync.Once
)

// GetActorManager 获取单例
func GetActorManager() *ActorManager {
	mgrOnce.Do(func() {
		instance = &ActorManager{
			globalActor: NewGlobalActor(),
			ioPool:      NewIOWorkerPool(),
		}
	})
	return instance
}

// RegListener 注册监听器
func (mgr *ActorManager) RegListener(eventType int, name string, listener Listener) error {
	if eventType < config.EVENT_MIN || eventType >= config.EVENT_MAX {
		return fmt.Errorf("eventType is invalid: %d, err=%d", eventType, config.ERR_EVENT_INVALID)
	}
	if name == "" {
		return fmt.Errorf("listener name is empty, err=%d", config.ERR_LISTENER_NIL)
	}
	if listener == nil {
		return fmt.Errorf("listener is nil, err=%d", config.ERR_LISTENER_NIL)
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// 检查重名
	for node := mgr.listeners[eventType]; node != nil; node = node.next {
		if node.Name == name {
			return fmt.Errorf("listener already registered: name=%s, err=%d", name, config.ERR_LISTENER_DUP)
		}
	}

	// 头插法
	mgr.listeners[eventType] = &listenerNode{
		ListenerEntry: ListenerEntry{Name: name, Listener: listener},
		next:          mgr.listeners[eventType],
	}
	return nil
}

// RegHandler 便捷方法：用 ListenerAdapter 包装已有的 EventListenerHandler
// cat 指定该监听器由哪种 Actor 处理
func (mgr *ActorManager) RegHandler(eventType int, name string, handler config.EventListenerHandler, cat Category) error {
	return mgr.RegListener(eventType, name, &ListenerAdapter{Handler: handler, Cat: cat})
}

// Dispatch 分发事件，根据每个监听器的 Category 分别路由到对应 Actor
// 该方法为异步投递（fire-and-forget），不等待监听器执行完成
func (mgr *ActorManager) Dispatch(msg *Message) error {
	if msg.EventType < config.EVENT_MIN || msg.EventType >= config.EVENT_MAX {
		return fmt.Errorf("eventType is invalid: %d, err=%d", msg.EventType, config.ERR_EVENT_INVALID)
	}
	if msg.Data == nil {
		return fmt.Errorf("event data is nil, err=%d", config.ERR_PARAM_NIL)
	}
	if msg.Ctx == nil {
		msg.Ctx = &Context{}
	}

	// 按 Category 分组监听器（在读锁下拷贝，释放锁后路由）
	var playerEntries, globalEntries, ioEntries []ListenerEntry

	mgr.mu.RLock()
	for node := mgr.listeners[msg.EventType]; node != nil; node = node.next {
		switch node.Listener.Category() {
		case CategoryPlayer:
			playerEntries = append(playerEntries, node.ListenerEntry)
		case CategoryGlobal:
			globalEntries = append(globalEntries, node.ListenerEntry)
		case CategoryIO:
			ioEntries = append(ioEntries, node.ListenerEntry)
		}
	}
	mgr.mu.RUnlock()

	var errs []error

	// 路由到 PlayerActor（按 RoleID 分发）
	if len(playerEntries) > 0 {
		actor := mgr.getOrCreatePlayerActor(msg.Ctx.RoleID)
		if err := actor.Post(&Task{Entries: playerEntries, Msg: msg}); err != nil {
			errs = append(errs, err)
		}
	}

	// 路由到 GlobalActor
	if len(globalEntries) > 0 {
		if err := mgr.globalActor.Post(&Task{Entries: globalEntries, Msg: msg}); err != nil {
			errs = append(errs, err)
		}
	}

	// 路由到 IOWorkerPool
	if len(ioEntries) > 0 {
		if err := mgr.ioPool.Post(&Task{Entries: ioEntries, Msg: msg}); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("dispatch event %d failed: %v", msg.EventType, errs)
	}
	return nil
}

// getOrCreatePlayerActor 懒创建玩家 Actor，并发安全
func (mgr *ActorManager) getOrCreatePlayerActor(roleID uint64) *PlayerActor {
	if v, ok := mgr.playerActors.Load(roleID); ok {
		return v.(*PlayerActor)
	}
	actor := NewPlayerActor(roleID)
	actual, loaded := mgr.playerActors.LoadOrStore(roleID, actor)
	if loaded {
		// 已存在同 RoleID 的 Actor，关闭多余的
		actor.Stop()
	}
	return actual.(*PlayerActor)
}

// StopPlayerActor 停止并移除指定玩家的 Actor（玩家登出时调用）
func (mgr *ActorManager) StopPlayerActor(roleID uint64) {
	if v, ok := mgr.playerActors.LoadAndDelete(roleID); ok {
		v.(*PlayerActor).Stop()
	}
}

// Wait 等待所有 Actor 队列处理完成（用于测试和优雅关闭）
func (mgr *ActorManager) Wait() {
	mgr.playerActors.Range(func(_, v any) bool {
		v.(*PlayerActor).Wait()
		return true
	})
	mgr.globalActor.Wait()
	mgr.ioPool.Wait()
}

// Stop 停止所有 Actor
func (mgr *ActorManager) Stop() {
	mgr.playerActors.Range(func(_, v any) bool {
		v.(*PlayerActor).Stop()
		return true
	})
	mgr.globalActor.Stop()
	mgr.ioPool.Stop()
}

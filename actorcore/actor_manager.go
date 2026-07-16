package actorcore

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// 哨兵错误
var (
	ErrActorNotFound      = errors.New("actor not found")
	ErrActorAlreadyExists = errors.New("actor already exists")
	ErrQueueFull          = errors.New("actor queue full")
	ErrHandlerExists      = errors.New("handler already registered")
	ErrNamedActorExists   = errors.New("named actor already exists")
	ErrActorClosed        = errors.New("actor closed")
)

// ActorManager 统一管理所有 Actor 的地址注册表。
// 职责：
//   - Actor 注册与地址分配（ActorHandle）
//   - Actor 间消息路由（Send / SendToPlayer / SendToNamed）
//   - 玩家 Actor 生命周期管理（创建/获取/停止）
//   - 命名服务注册与查找
type ActorManager struct {
	mu             sync.RWMutex
	actors         map[ActorHandle]Actor  // handle -> Actor
	players        map[uint64]ActorHandle // roleID -> handle
	namedActors    map[string]ActorHandle // name -> handle
	playerHandlers *HandlerMap            // 所有 PlayerActor 共享的 handler 注册表
	instIDGen      atomic.Uint64          // InstanceID 自增生成器
}

var (
	mgrInstance *ActorManager
	mgrOnce     sync.Once
)

// GetActorManager 获取单例
func GetActorManager() *ActorManager {
	mgrOnce.Do(func() {
		mgrInstance = &ActorManager{
			actors:         make(map[ActorHandle]Actor),
			players:        make(map[uint64]ActorHandle),
			namedActors:    make(map[string]ActorHandle),
			playerHandlers: NewHandlerMap(),
		}
	})
	return mgrInstance
}

// nextInstID 生成下一个 InstanceID
func (mgr *ActorManager) nextInstID() uint64 {
	return mgr.instIDGen.Add(1)
}

// ---------- 玩家 Actor 管理 ----------

// CreatePlayerActor 创建或获取玩家 Actor。
// 若该 roleID 的 Actor 已存在，则更新 Responder（重连场景）并返回已有 Actor。
func (mgr *ActorManager) CreatePlayerActor(roleID uint64, responder Responder) (ActorHandle, *PlayerActor, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if handle, ok := mgr.players[roleID]; ok {
		if actor, ok := mgr.actors[handle]; ok {
			pa := actor.(*PlayerActor)
			if responder != nil {
				pa.SetResponder(responder)
			}
			return handle, pa, nil
		}
	}

	handle := MakeHandle(CategoryPlayer, mgr.nextInstID())
	pa := NewPlayerActor(handle, roleID, responder, mgr.playerHandlers, mgr)
	mgr.actors[handle] = pa
	mgr.players[roleID] = handle
	return handle, pa, nil
}

// GetPlayerActor 获取玩家 Actor
func (mgr *ActorManager) GetPlayerActor(roleID uint64) (*PlayerActor, bool) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	handle, ok := mgr.players[roleID]
	if !ok {
		return nil, false
	}
	actor, ok := mgr.actors[handle]
	if !ok {
		return nil, false
	}
	return actor.(*PlayerActor), true
}

// StopPlayerActor 停止并移除指定玩家的 Actor（玩家登出时调用）
func (mgr *ActorManager) StopPlayerActor(roleID uint64) {
	mgr.mu.Lock()
	handle, ok := mgr.players[roleID]
	var actor Actor
	if ok {
		actor = mgr.actors[handle]
		delete(mgr.players, roleID)
		delete(mgr.actors, handle)
	}
	mgr.mu.Unlock()

	if actor != nil {
		actor.Stop() // 在锁外执行，避免阻塞其他操作
	}
}

// RegisterPlayerHandler 注册全局玩家消息处理器（所有 PlayerActor 共享）
func (mgr *ActorManager) RegisterPlayerHandler(msgID uint16, handler HandlerFunc) error {
	return mgr.playerHandlers.Register(msgID, handler)
}

// MustRegisterPlayerHandler 注册全局玩家消息处理器，重复注册 panic
func (mgr *ActorManager) MustRegisterPlayerHandler(msgID uint16, handler HandlerFunc) {
	if err := mgr.RegisterPlayerHandler(msgID, handler); err != nil {
		panic(err)
	}
}

// ---------- 命名服务 Actor 管理 ----------

// CreateNamedActor 创建命名服务 Actor
// workers=1 串行处理，workers>1 并发处理
func (mgr *ActorManager) CreateNamedActor(name string, workers int) (ActorHandle, *NamedActor, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if _, ok := mgr.namedActors[name]; ok {
		return 0, nil, fmt.Errorf("%w: name=%s", ErrNamedActorExists, name)
	}

	handle := MakeHandle(CategoryNamed, mgr.nextInstID())
	na := NewNamedActor(handle, name, workers, mgr)
	mgr.actors[handle] = na
	mgr.namedActors[name] = handle
	return handle, na, nil
}

// GetNamedActor 获取命名服务 Actor
func (mgr *ActorManager) GetNamedActor(name string) (*NamedActor, bool) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	handle, ok := mgr.namedActors[name]
	if !ok {
		return nil, false
	}
	actor, ok := mgr.actors[handle]
	if !ok {
		return nil, false
	}
	return actor.(*NamedActor), true
}

// ---------- Actor 间通信 ----------

// Send 向目标 Actor 投递消息
func (mgr *ActorManager) Send(target ActorHandle, msg *ActorMessage) error {
	mgr.mu.RLock()
	actor, ok := mgr.actors[target]
	mgr.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: handle=%s", ErrActorNotFound, target)
	}
	return actor.Post(msg)
}

// SendToPlayer 向指定玩家的 Actor 投递消息
func (mgr *ActorManager) SendToPlayer(roleID uint64, msg *ActorMessage) error {
	mgr.mu.RLock()
	handle, ok := mgr.players[roleID]
	mgr.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: roleID=%d", ErrActorNotFound, roleID)
	}
	return mgr.Send(handle, msg)
}

// SendToNamed 向命名服务投递消息
func (mgr *ActorManager) SendToNamed(name string, msg *ActorMessage) error {
	mgr.mu.RLock()
	handle, ok := mgr.namedActors[name]
	mgr.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: name=%s", ErrActorNotFound, name)
	}
	return mgr.Send(handle, msg)
}

// Query 根据地址查询 Actor
func (mgr *ActorManager) Query(handle ActorHandle) (Actor, bool) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	actor, ok := mgr.actors[handle]
	return actor, ok
}

// ---------- 生命周期管理 ----------

// Wait 等待所有 Actor 处理完成（用于测试和优雅关闭）
func (mgr *ActorManager) Wait() {
	mgr.mu.RLock()
	actors := make([]Actor, 0, len(mgr.actors))
	for _, a := range mgr.actors {
		actors = append(actors, a)
	}
	mgr.mu.RUnlock()

	for _, a := range actors {
		a.Wait()
	}
}

// Stop 停止所有 Actor 并清空注册表
func (mgr *ActorManager) Stop() {
	mgr.mu.Lock()
	actors := make([]Actor, 0, len(mgr.actors))
	for _, a := range mgr.actors {
		actors = append(actors, a)
	}
	mgr.actors = make(map[ActorHandle]Actor)
	mgr.players = make(map[uint64]ActorHandle)
	mgr.namedActors = make(map[string]ActorHandle)
	mgr.mu.Unlock()

	for _, a := range actors {
		a.Stop() // 在锁外执行，避免阻塞
	}
}

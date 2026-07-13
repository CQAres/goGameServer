package actorcore

import (
	"goGameServer/EventMgr/config"
)

// Category 事件分类，决定事件由哪种 Actor 处理
type Category int

const (
	CategoryPlayer Category = iota // 玩家内串行：同一玩家事件按序处理，不同玩家并行
	CategoryGlobal                 // 全局串行：所有此类事件在单一队列顺序处理
	CategoryIO                     // IO 并发：交由 worker pool 并发处理，不保证顺序
)

func (c Category) String() string {
	switch c {
	case CategoryPlayer:
		return "Player"
	case CategoryGlobal:
		return "Global"
	case CategoryIO:
		return "IO"
	default:
		return "Unknown"
	}
}

// Listener 监听器接口，在 EventListenerHandler 基础上增加 Category 方法
type Listener interface {
	config.EventListenerHandler
	Category() Category
}

// ListenerAdapter 将已有的 config.EventListenerHandler 包装为 Listener
// 通过指定 Cat 决定该监听器由哪种 Actor 处理
type ListenerAdapter struct {
	Handler config.EventListenerHandler
	Cat     Category
}

func (a *ListenerAdapter) OnEvent(eventType int, param config.EventData) error {
	return a.Handler.OnEvent(eventType, param)
}

func (a *ListenerAdapter) Category() Category {
	return a.Cat
}

// Context 事件处理上下文，携带路由所需的元信息
type Context struct {
	RoleID uint64 // CategoryPlayer 时用于路由到对应的 PlayerActor
}

// Message 事件消息
type Message struct {
	EventType int
	Data      config.EventData
	Ctx       *Context
}

// ListenerEntry 监听器条目（名称 + 监听器）
type ListenerEntry struct {
	Name     string
	Listener Listener
}

// Task Actor 执行的任务，包含一个事件在某 Category 下的所有监听器
type Task struct {
	Entries []ListenerEntry
	Msg     *Message
}

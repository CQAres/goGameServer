package actorcore

import "fmt"

// ActorHandle 是 Actor 的全局唯一地址。
// 高 8 位为 Category，低 56 位为 InstanceID。
type ActorHandle uint64

const (
	categoryBits   = 8
	instanceIDBits = 64 - categoryBits
	instanceIDMask = (uint64(1) << instanceIDBits) - 1
)

// Category Actor 类型分类，决定 Actor 的路由和处理方式
type Category uint8

const (
	CategoryInvalid Category = 0
	CategoryPlayer  Category = 1 // 玩家 Actor：每玩家一个实例，串行处理
	CategoryNamed   Category = 2 // 命名服务 Actor：全局唯一，按名查找
	CategoryTimer   Category = 3 // 定时器服务（预留）
)

func (c Category) String() string {
	switch c {
	case CategoryPlayer:
		return "Player"
	case CategoryNamed:
		return "Named"
	case CategoryTimer:
		return "Timer"
	default:
		return fmt.Sprintf("Unknown(%d)", uint8(c))
	}
}

// MakeHandle 根据 Category 和 InstanceID 构造 ActorHandle
func MakeHandle(cat Category, instID uint64) ActorHandle {
	return ActorHandle(uint64(cat)<<instanceIDBits) | ActorHandle(instID&instanceIDMask)
}

// Category 返回 Handle 的 Category
func (h ActorHandle) Category() Category {
	return Category(uint64(h) >> instanceIDBits)
}

// InstanceID 返回 Handle 的 InstanceID
func (h ActorHandle) InstanceID() uint64 {
	return uint64(h) & instanceIDMask
}

// IsZero 是否为空 Handle
func (h ActorHandle) IsZero() bool {
	return h == 0
}

func (h ActorHandle) String() string {
	return fmt.Sprintf("{%s:%d}", h.Category(), h.InstanceID())
}

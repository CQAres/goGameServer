package main

import (
	"fmt"
	actorcore2 "goGameServer/EventMgr/ActorEventMgr/actorcore"
	"math/rand"
	"strconv"
	"sync"

	"goGameServer/EventMgr/RoleLevelChange"
	"goGameServer/EventMgr/RoleLogin"
	"goGameServer/EventMgr/RoleLogout"
	"goGameServer/EventMgr/config"
)

// Init 注册监听器，通过 Category 指定每个监听器由哪种 Actor 处理
func Init() {
	mgr := actorcore2.GetActorManager()

	// 登录事件 → Player：玩家状态初始化，串行处理避免竞争
	mgr.RegHandler(config.EVENT_ROLE_LOGIN, "Login", &RoleLogin.LoginListener{}, actorcore2.CategoryPlayer)

	// 登出事件 → Player：玩家状态清理
	mgr.RegHandler(config.EVENT_ROLE_LOGOUT, "Logout", &RoleLogout.LogoutListener{}, actorcore2.CategoryPlayer)

	// 升级事件拆分到三种 Category：
	//   rank  → Global：排行榜是全局共享数据，必须串行
	//   open  → Player：功能开启依赖玩家状态，与玩家其他事件串行
	//   log   → IO：写日志不依赖顺序，交给 worker pool 并发
	mgr.RegHandler(config.EVENT_ROLE_LEVEL_CHANGE, "rank", &RoleLevelChange.RankListener{}, actorcore2.CategoryGlobal)
	mgr.RegHandler(config.EVENT_ROLE_LEVEL_CHANGE, "open", &RoleLevelChange.FuncOpenListener{}, actorcore2.CategoryPlayer)
	mgr.RegHandler(config.EVENT_ROLE_LEVEL_CHANGE, "log", &RoleLevelChange.LevelChangeLog{}, actorcore2.CategoryIO)
}

// ActorTest 并发模拟多玩家登录 → 升级 → 登出
func ActorTest() {
	Init()

	mgr := actorcore2.GetActorManager()
	var wg sync.WaitGroup

	for i := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			role := uint64(i) + 1
			ip := strconv.Itoa(rand.Int())
			server := uint32(rand.Int())

			// 登录
			mgr.Dispatch(&actorcore2.Message{
				EventType: config.EVENT_ROLE_LOGIN,
				Data: &RoleLogin.LoginEvent{
					RoleID:   role,
					IP:       ip,
					ServerID: server,
				},
				Ctx: &actorcore2.Context{RoleID: role},
			})

			// 升级
			mgr.Dispatch(&actorcore2.Message{
				EventType: config.EVENT_ROLE_LEVEL_CHANGE,
				Data: &RoleLevelChange.LevelChangeEvent{
					RoleID:   role,
					OleLevel: 1,
					NewLevel: 2,
				},
				Ctx: &actorcore2.Context{RoleID: role},
			})

			// 登出
			mgr.Dispatch(&actorcore2.Message{
				EventType: config.EVENT_ROLE_LOGOUT,
				Data: &RoleLogout.LogoutEvent{
					RoleID:   role,
					IP:       ip,
					ServerID: server,
				},
				Ctx: &actorcore2.Context{RoleID: role},
			})
		}()
	}

	// 等待所有 Dispatch 调用完成（事件已入队）
	wg.Wait()
	// 等待所有 Actor 队列处理完成
	mgr.Wait()
}

func main() {
	fmt.Println("=== Actor Event Manager Test ===")
	ActorTest()
	fmt.Println("=== All events processed ===")

	// 优雅关闭
	mgr := actorcore2.GetActorManager()
	mgr.Stop()
}

package actorcore

import (
	"fmt"
	"sync"
)

const playerQueueSize = 1024

// PlayerActor 玩家 Actor，每个玩家一个实例
// 串行处理该玩家的事件，避免对玩家数据的并发访问
// 不同玩家的 Actor 互相独立、并行执行
type PlayerActor struct {
	roleID uint64
	queue  chan *Task
	wg     sync.WaitGroup // 跟踪未处理完的 Task
	once   sync.Once
	done   chan struct{}
}

// NewPlayerActor 创建并启动一个玩家 Actor
func NewPlayerActor(roleID uint64) *PlayerActor {
	actor := &PlayerActor{
		roleID: roleID,
		queue:  make(chan *Task, playerQueueSize),
		done:   make(chan struct{}),
	}
	go actor.run()
	return actor
}

func (actor *PlayerActor) run() {
	defer close(actor.done)
	for task := range actor.queue {
		actor.handle(task)
		actor.wg.Done()
	}
}

func (actor *PlayerActor) handle(task *Task) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[PlayerActor] panic: roleID=%d, event=%d, panic=%v\n",
				actor.roleID, task.Msg.EventType, r)
		}
	}()

	for _, entry := range task.Entries {
		if err := entry.Listener.OnEvent(task.Msg.EventType, task.Msg.Data); err != nil {
			fmt.Printf("[PlayerActor] roleID=%d, event=%d, listener=%s, err=%v\n",
				actor.roleID, task.Msg.EventType, entry.Name, err)
		}
	}
}

// Post 投递任务到玩家队列，队列满时返回错误（背压降级）
func (actor *PlayerActor) Post(task *Task) error {
	actor.wg.Add(1)
	select {
	case actor.queue <- task:
		return nil
	default:
		actor.wg.Done()
		return fmt.Errorf("player actor queue full: roleID=%d, event=%d",
			actor.roleID, task.Msg.EventType)
	}
}

// Wait 等待所有已投递的任务处理完成
func (actor *PlayerActor) Wait() {
	actor.wg.Wait()
}

// Stop 关闭队列并等待 Actor 退出
func (actor *PlayerActor) Stop() {
	actor.once.Do(func() {
		close(actor.queue)
	})
	<-actor.done
}

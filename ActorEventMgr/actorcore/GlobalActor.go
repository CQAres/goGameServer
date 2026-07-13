package actorcore

import (
	"fmt"
	"sync"
)

const globalQueueSize = 4096

// GlobalActor 全局 Actor，单一队列串行处理所有全局事件
// 适用于需要全局有序性的操作，如排行榜更新、全局广播等
type GlobalActor struct {
	queue chan *Task
	wg    sync.WaitGroup // 跟踪未处理完的 Task
	once  sync.Once
	done  chan struct{}
}

// NewGlobalActor 创建并启动全局 Actor
func NewGlobalActor() *GlobalActor {
	actor := &GlobalActor{
		queue: make(chan *Task, globalQueueSize),
		done:  make(chan struct{}),
	}
	go actor.run()
	return actor
}

func (actor *GlobalActor) run() {
	defer close(actor.done)
	for task := range actor.queue {
		actor.handle(task)
		actor.wg.Done()
	}
}

func (actor *GlobalActor) handle(task *Task) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[GlobalActor] panic: event=%d, panic=%v\n",
				task.Msg.EventType, r)
		}
	}()

	for _, entry := range task.Entries {
		if err := entry.Listener.OnEvent(task.Msg.EventType, task.Msg.Data); err != nil {
			fmt.Printf("[GlobalActor] event=%d, listener=%s, err=%v\n",
				task.Msg.EventType, entry.Name, err)
		}
	}
}

// Post 投递任务到全局队列
func (actor *GlobalActor) Post(task *Task) error {
	actor.wg.Add(1)
	select {
	case actor.queue <- task:
		return nil
	default:
		actor.wg.Done()
		return fmt.Errorf("global actor queue full: event=%d", task.Msg.EventType)
	}
}

// Wait 等待所有已投递的任务处理完成
func (actor *GlobalActor) Wait() {
	actor.wg.Wait()
}

// Stop 关闭队列并等待 Actor 退出
func (actor *GlobalActor) Stop() {
	actor.once.Do(func() {
		close(actor.queue)
	})
	<-actor.done
}

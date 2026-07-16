package actorcore

import (
	"fmt"
	"sync"
)

const (
	ioQueueSize   = 4096
	ioWorkerCount = 8
)

// IOWorkerPool IO 工作池，多 worker 并发处理 IO 密集型事件
// 适用于数据库写入、网络请求、文件操作等不需要严格顺序的场景
type IOWorkerPool struct {
	queue    chan *Task
	taskWg   sync.WaitGroup // 跟踪未处理完的 Task
	workerWg sync.WaitGroup // 跟踪 worker goroutine
	once     sync.Once
}

// NewIOWorkerPool 创建并启动 IO 工作池
func NewIOWorkerPool() *IOWorkerPool {
	pool := &IOWorkerPool{
		queue: make(chan *Task, ioQueueSize),
	}
	for range ioWorkerCount {
		pool.workerWg.Add(1)
		go pool.run()
	}
	return pool
}

func (pool *IOWorkerPool) run() {
	defer pool.workerWg.Done()
	for task := range pool.queue {
		pool.handle(task)
		pool.taskWg.Done()
	}
}

func (pool *IOWorkerPool) handle(task *Task) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[IOWorkerPool] panic: event=%d, panic=%v\n",
				task.Msg.EventType, r)
		}
	}()

	for _, entry := range task.Entries {
		if err := entry.Listener.OnEvent(task.Msg.EventType, task.Msg.Data); err != nil {
			fmt.Printf("[IOWorkerPool] event=%d, listener=%s, err=%v\n",
				task.Msg.EventType, entry.Name, err)
		}
	}
}

// Post 投递任务到 IO 队列
func (pool *IOWorkerPool) Post(task *Task) error {
	pool.taskWg.Add(1)
	select {
	case pool.queue <- task:
		return nil
	default:
		pool.taskWg.Done()
		return fmt.Errorf("io worker pool queue full: event=%d", task.Msg.EventType)
	}
}

// Wait 等待所有已投递的任务处理完成
func (pool *IOWorkerPool) Wait() {
	pool.taskWg.Wait()
}

// Stop 关闭队列并等待所有 worker 退出
func (pool *IOWorkerPool) Stop() {
	pool.once.Do(func() {
		close(pool.queue)
	})
	pool.workerWg.Wait()
}

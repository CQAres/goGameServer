package actorcore

// Actor 所有 Actor 类型必须实现的统一接口
type Actor interface {
	// Handle 返回 Actor 的全局地址
	Handle() ActorHandle
	// Post 投递消息到 Actor 队列（非阻塞，队列满或已关闭时返回错误）
	Post(msg *ActorMessage) error
	// Stop 停止 Actor（幂等，关闭队列并等待处理完成）
	Stop()
	// Wait 等待所有已投递消息处理完成（用于测试和优雅关闭）
	Wait()
}

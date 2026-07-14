package netlayer

import "time"

// Config 网络层配置
type Config struct {
	TCPAddr           string        // TCP 监听地址
	WSAddr            string        // WebSocket 监听地址
	WSPath            string        // WebSocket 路径
	SendQueueSize     int           // 单连接发送队列大小
	ReadBufferSize    int           // 读缓冲区大小
	WriteBufferSize   int           // 写缓冲区大小
	HeartbeatInterval time.Duration // 心跳检查间隔
	HeartbeatTimeout  time.Duration // 心跳超时时间
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		TCPAddr:           ":9000",
		WSAddr:            ":8080",
		WSPath:            "/ws",
		SendQueueSize:     1024,
		ReadBufferSize:    16 * 1024,
		WriteBufferSize:   16 * 1024,
		HeartbeatInterval: 30 * time.Second,
		HeartbeatTimeout:  90 * time.Second,
	}
}

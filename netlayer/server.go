package netlayer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Server 混合网络服务器，同时支持 TCP 和 WebSocket 两种接入方式。
// 内部统一使用 Connection 抽象，TCP 和 WS 连接走同一套读循环与路由分发逻辑。
type Server struct {
	cfg      *Config
	router   *Router
	mgr      *SessionManager
	upgrader *websocket.Upgrader

	tcpListener net.Listener
	httpServer  *http.Server

	wg       sync.WaitGroup // 跟踪 accept / 读循环 / 心跳等 goroutine
	stopOnce sync.Once
	done     chan struct{} // 关闭信号
}

// NewServer 创建服务器（不启动监听），cfg 为 nil 时使用默认配置，router 为 nil 时创建空路由器。
func NewServer(cfg *Config, router *Router) *Server {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if router == nil {
		router = NewRouter()
	}
	return &Server{
		cfg:    cfg,
		router: router,
		mgr:    NewSessionManager(),
		done:   make(chan struct{}),
		upgrader: &websocket.Upgrader{
			ReadBufferSize:  cfg.ReadBufferSize,
			WriteBufferSize: cfg.WriteBufferSize,
			// 允许所有来源，生产环境应按需限制
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

// SessionManager 返回会话管理器，供业务层在登录 handler 中调用 BindRole 等。
func (s *Server) SessionManager() *SessionManager { return s.mgr }

// Router 返回路由器，供业务层注册消息处理器。
func (s *Server) Router() *Router { return s.router }

// Config 返回当前服务器配置（只读用途）。
func (s *Server) Config() *Config { return s.cfg }

// Start 启动 TCP 和 WebSocket 监听，非阻塞。
// 调用者可通过 Stop() 优雅关闭。
func (s *Server) Start() error {
	// ---------- TCP ----------
	ln, err := net.Listen("tcp", s.cfg.TCPAddr)
	if err != nil {
		return fmt.Errorf("tcp listen %s: %w", s.cfg.TCPAddr, err)
	}
	s.tcpListener = ln
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptTCP(ln)
	}()
	log.Printf("[Server] TCP listening on %s", s.cfg.TCPAddr)

	// ---------- WebSocket ----------
	mux := http.NewServeMux()
	mux.HandleFunc(s.cfg.WSPath, s.handleWSUpgrade)
	s.httpServer = &http.Server{
		Addr:              s.cfg.WSAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[Server] WS http server error: %v", err)
		}
	}()
	log.Printf("[Server] WebSocket listening on %s%s", s.cfg.WSAddr, s.cfg.WSPath)

	// ---------- 心跳检查 ----------
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.heartbeatLoop()
	}()

	return nil
}

// acceptTCP 接受 TCP 连接，每条连接起一个 goroutine 走统一读循环。
func (s *Server) acceptTCP(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.done:
				return // 正常关闭
			default:
				log.Printf("[Server] TCP accept error: %v", err)
				return
			}
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(NewTCPConnection(conn), false)
		}()
	}
}

// handleWSUpgrade 将 HTTP 请求升级为 WebSocket 连接，然后走统一读循环。
func (s *Server) handleWSUpgrade(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[Server] WS upgrade error: %v", err)
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.handleConn(NewWSConnection(conn), true)
	}()
}

// handleConn 连接读循环：创建 Session → 循环 ReadFrame → 更新活跃时间 → 路由分发。
// 心跳消息(MsgIDHeartbeat)在协议层直接消费，不分发到 Router。
func (s *Server) handleConn(conn Connection, isWS bool) {
	sess := NewSession(
		s.mgr.NextID(),
		conn,
		isWS,
		s.cfg.SendQueueSize,
		// onClose 回调：从 SessionManager 中移除
		func(closed *Session) {
			s.mgr.Remove(closed)
			log.Printf("[Server] session closed: id=%d, roleID=%d, remote=%s",
				closed.ID(), closed.RoleID(), closed.RemoteAddr())
		},
	)
	s.mgr.Add(sess)
	log.Printf("[Server] new session: id=%d, ws=%v, remote=%s",
		sess.ID(), isWS, sess.RemoteAddr())

	// defer Close 幂等：无论读循环如何退出都确保连接被清理
	defer sess.Close()

	for {
		msgID, payload, err := conn.ReadFrame()
		if err != nil {
			if !isClosedErr(err) {
				log.Printf("[Server] read frame error: id=%d, err=%v", sess.ID(), err)
			}
			return
		}

		// 任何消息到达都刷新活跃时间
		sess.UpdateActive()

		// 心跳消息在协议层直接消费，不进入业务路由
		if msgID == MsgIDHeartbeat {
			continue
		}

		// 分发到业务 handler
		if err := s.router.Dispatch(sess, msgID, payload); err != nil {
			log.Printf("[Server] dispatch error: id=%d, msgID=%d, err=%v",
				sess.ID(), msgID, err)
		}
	}
}

// heartbeatLoop 定时检查所有 Session 的活跃时间，超时则强制关闭。
func (s *Server) heartbeatLoop() {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.mgr.Range(func(sess *Session) bool {
				if time.Since(sess.LastActive()) > s.cfg.HeartbeatTimeout {
					log.Printf("[Server] heartbeat timeout: id=%d, roleID=%d, lastActive=%s",
						sess.ID(), sess.RoleID(),
						sess.LastActive().Format(time.RFC3339))
					sess.Close()
				}
				return true
			})
		}
	}
}

// Wait 阻塞等待所有 goroutine 退出（不含关闭动作，需先调用 Stop）。
func (s *Server) Wait() { s.wg.Wait() }

// Stop 优雅关闭：停止接受新连接 → 关闭所有在线会话 → 等待 goroutine 退出。幂等。
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.done)

		// 1. 停止接受新连接
		if s.tcpListener != nil {
			s.tcpListener.Close()
		}
		if s.httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s.httpServer.Shutdown(ctx)
		}

		// 2. 关闭所有在线会话（触发各读循环 goroutine 退出）
		s.mgr.Range(func(sess *Session) bool {
			sess.Close()
			return true
		})

		// 3. 等待所有 goroutine 退出
		s.wg.Wait()
		log.Printf("[Server] stopped, remaining sessions=%d", s.mgr.Count())
	})
}

// isClosedErr 判断是否为正常连接关闭错误，用于抑制无关日志。
func isClosedErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
		return true
	}
	if errors.Is(err, websocket.ErrCloseSent) {
		return true
	}
	// gorilla/websocket 正常关闭帧
	var ce *websocket.CloseError
	return errors.As(err, &ce)
}

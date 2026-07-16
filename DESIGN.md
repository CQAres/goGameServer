# goGameServer 游戏服务器

基于 **Skynet 风格 Actor 模型** 的 Go 游戏服务器框架，支持 TCP / WebSocket 双协议接入，通过 Gate 网关将网络消息路由到 Actor 处理，实现业务逻辑与网络层的彻底解耦。

## 目录

- [架构总览](#架构总览)
- [目录结构](#目录结构)
- [核心模块](#核心模块)
  - [actorcore — Actor 核心](#actorcore--actor-核心)
  - [netlayer — 网络层](#netlayer--网络层)
  - [service — 业务服务](#service--业务服务)
- [通信协议](#通信协议)
- [消息流转](#消息流转)
- [启动流程](#启动流程)
- [配置说明](#配置说明)
- [扩展指南](#扩展指南)
- [关键设计决策](#关键设计决策)
- [后续规划](#后续规划)

---

## 架构总览

```
                        ┌─────────────────────────────────────────┐
                        │              main.go (入口)              │
                        └──────┬──────────┬──────────┬─────────────┘
                               │          │          │
                    ┌──────────▼──┐ ┌─────▼─────┐ ┌──▼──────────┐
                    │  netlayer   │ │ actorcore │ │   service    │
                    │  (网络层)    │ │ (Actor核心)│ │  (业务服务)  │
                    └──────┬──────┘ └─────▲─────┘ └──┬──────────┘
                           │              │          │
                           │    依赖方向 ──┘          │
                           │              ▲          │
                           └──────────────┘──────────┘
                              netlayer → actorcore
                              service → netlayer, actorcore
```

**设计原则：**

1. **Actor 模型**：每个玩家拥有独立的 PlayerActor，消息在 Actor 内串行处理，天然避免并发竞争
2. **Gate 网关**：网络层是纯转发层，不含业务逻辑，通过 MsgRoute 路由表将消息分发到对应 Actor
3. **依赖倒置**：actorcore 定义 `Responder` 接口，netlayer.Session 隐式实现，避免 actorcore 反向依赖 netlayer
4. **单向依赖**：`netlayer → actorcore`，`service → netlayer + actorcore`，无循环依赖

---

## 目录结构

```
goGameServer/
├── main.go                     # 程序入口：组装各模块并启动
├── go.mod                      # Go 模块定义
│
├── actorcore/                  # Actor 核心层（Phase 1）
│   ├── handle.go               # ActorHandle 地址系统（Category | InstanceID）
│   ├── message.go              # ActorMessage / Responder / ActorContext / HandlerMap
│   ├── actor.go                # Actor 统一接口
│   ├── player_actor.go         # PlayerActor：每玩家一个实例，串行处理
│   ├── named_actor.go          # NamedActor：命名服务，支持串行/并发模式
│   └── actor_manager.go        # ActorManager：地址注册表 + Actor 间通信
│
├── netlayer/                   # 网络层（Phase 2）
│   ├── codec.go                # 帧编解码：[4字节长度][2字节MsgID][Payload]
│   ├── config.go               # 服务器配置
│   ├── connection.go           # Connection 接口（TCP / WS 统一抽象）
│   ├── gate.go                 # Gate 网关：MsgRoute 路由表，消息分发到 Actor
│   ├── message.go              # 协议消息 ID 和结构体定义
│   ├── server.go               # Server：TCP + WS 双协议监听
│   ├── session.go              # Session：异步发送队列 + 心跳 + Responder 实现
│   └── session_manager.go      # SessionManager：sessionID/roleID 双向索引
│
├── service/                    # 业务服务层（Phase 3）
│   ├── service.go              # Init() 初始化入口
│   ├── login_service.go        # 登录服务（NamedActor）
│   └── echo_service.go         # 回显服务（PlayerActor handler）
│
├── timer/                      # 定时器服务（Phase 4 占位）
├── protocol/                   # 协议定义（预留，消息当前在 netlayer/message.go）
├── client/                     # 测试客户端（Phase 4 占位）
│
├── EventMgr/                   # 独立事件管理模块（保留，未被主架构使用）
└── ActorEventMgr/              # 旧 Actor 事件管理模块（保留，已被 actorcore 取代）
```

---

## 核心模块

### actorcore — Actor 核心

Skynet 风格的 Actor 核心层，提供 Actor 地址系统、消息队列、handler 分发和 Actor 间通信。

#### ActorHandle 地址系统

每个 Actor 拥有全局唯一的 `ActorHandle`（uint64），由高 8 位 Category 和低 56 位 InstanceID 组成：

```
  8 bits          56 bits
┌──────────┬───────────────────────────┐
│ Category │        InstanceID         │
└──────────┴───────────────────────────┘
```

| Category | 说明 | 用途 |
|----------|------|------|
| `CategoryPlayer` (1) | 玩家 Actor | 每个玩家一个实例，按 RoleID 查找 |
| `CategoryNamed` (2) | 命名服务 Actor | 全局唯一，按名称查找（如 "login"） |
| `CategoryTimer` (3) | 定时器服务 | 预留 |

#### Actor 接口

```go
type Actor interface {
    Handle() ActorHandle            // 全局地址
    Post(msg *ActorMessage) error   // 投递消息（非阻塞）
    Stop()                          // 停止（幂等）
    Wait()                          // 等待消息处理完成
}
```

#### Actor 类型

| 类型 | 文件 | 特性 |
|------|------|------|
| `PlayerActor` | `player_actor.go` | 每玩家一个实例，串行处理该玩家所有消息；`atomic.Value` 管理 Responder 支持重连更新 |
| `NamedActor` | `named_actor.go` | 命名服务，`workers=1` 串行（替代 GlobalActor），`workers>1` 并发（替代 IOWorkerPool） |

两者都使用 **closeSig + atomic.Bool + recover** 三重保护防止向已关闭 channel 写入导致 panic。

#### ActorMessage

Actor 间、Gate→Actor 投递的统一消息格式：

```go
type ActorMessage struct {
    MsgID     uint16      // 协议消息 ID
    Payload   []byte      // 原始字节（未解码）
    Data      any         // 已解码数据（由 Gate 填充）
    Source    ActorHandle // 消息来源（0 = 客户端/Gate）
    Responder Responder   // 网络回写通道
}
```

#### Responder 接口（依赖倒置）

```go
type Responder interface {
    Send(msgID uint16, payload any) error
}
```

定义在 actorcore 中，由 `netlayer.Session` 隐式实现（方法签名一致）。Actor handler 通过 `ctx.Reply()` 回写客户端，无需直接依赖 netlayer。

#### ActorContext

handler 执行时的上下文，提供跨 Actor 通信能力：

```go
ctx.Reply(msgID, payload)            // 回复客户端
ctx.Send(targetHandle, msg)          // 发送到指定 Actor
ctx.SendToPlayer(roleID, msg)        // 发送到玩家 Actor
ctx.SendToNamed("rank", msg)         // 发送到命名服务
```

#### ActorManager

单例模式，统一管理所有 Actor：

| 方法 | 说明 |
|------|------|
| `CreatePlayerActor(roleID, responder)` | 创建/获取玩家 Actor（重连时更新 Responder） |
| `StopPlayerActor(roleID)` | 停止并移除玩家 Actor |
| `CreateNamedActor(name, workers)` | 创建命名服务 Actor |
| `Send(handle, msg)` | 按 Handle 发送 |
| `SendToPlayer(roleID, msg)` | 按 RoleID 发送 |
| `SendToNamed(name, msg)` | 按名称发送 |
| `RegisterPlayerHandler(msgID, handler)` | 注册全局 PlayerActor handler（所有玩家共享） |
| `Query(handle)` | 查询 Actor |
| `Wait()` / `Stop()` | 等待/停止所有 Actor |

---

### netlayer — 网络层

纯网络转发层，通过 Gate 网关将消息路由到 Actor，不包含任何业务逻辑。

#### 通信协议

帧格式（TCP 和 WebSocket 统一）：

```
┌──────────────┬────────────┬─────────────────┐
│ 4 字节 Length │ 2 字节 MsgID │    Payload      │
│ (big-endian) │ (big-endian) │  (gob 编码)     │
└──────────────┴────────────┴─────────────────┘
```

- Length = MsgID 字段大小 + Payload 长度
- Payload 使用 Go 标准库 `encoding/gob` 编解码
- 单帧 Payload 最大 16MB

#### Connection 抽象

`Connection` 接口统一 TCP 和 WebSocket：

```go
type Connection interface {
    ReadFrame() (uint16, []byte, error)
    WriteFrame(msgID uint16, payload []byte) error
    Close() error
    RemoteAddr() string
}
```

- `TCPConnection`：自行处理粘包/拆包（基于 Length 前缀）
- `WSConnection`：复用同一帧格式，与 TCP 共用处理逻辑

#### Session

封装 Connection，提供异步发送和生命周期管理：

```
                      Send(msgID, payload)
                             │
                             ▼
                     ┌─────────────┐
                     │  sendQueue  │  (chan sendItem, 带缓冲)
                     └──────┬──────┘
                            │
                     ┌──────▼──────┐
                     │   sendLoop  │  (单独 goroutine, 串行写 socket)
                     └──────┬──────┘
                            │
                     ┌──────▼──────┐
                     │  Connection │
                     └─────────────┘
```

关键特性：
- **非阻塞 Send**：`select` + `default`，队列满时立即返回错误
- **防 panic**：`closeSig` channel + `atomic.Bool` + `recover` 三重保护
- **心跳**：`lastActive` 原子时间戳，由 `heartbeatLoop` 定时检查
- **幂等 Close**：`sync.Once` 保证只关闭一次，关闭顺序：`closeSig → sendQueue → done → conn.Close → onClose`

#### SessionManager

维护 sessionID 和 roleID 双向索引：

| 方法 | 说明 |
|------|------|
| `Add(sess)` | 添加 Session |
| `BindRole(sess, roleID)` | 绑定 RoleID，顶掉同 RoleID 旧连接 |
| `Remove(sess)` | 移除 Session（带指针一致性检查） |
| `GetByRole(roleID)` | 按 RoleID 查找 Session |
| `Range(fn)` | 遍历所有 Session（不持锁） |

`BindRole` 顶号处理：先 `old.SetRoleID(0)` 防止 onClose→Remove 误删新 Session 索引，再异步 `go old.Close()`。

#### Gate 网关

纯转发层，通过路由表将网络消息分发到 Actor：

```go
type MsgRoute struct {
    Target  RouteTarget  // TargetPlayer 或 TargetNamed
    Name    string       // TargetNamed 时的服务名称
    Decoder DecoderFunc  // 消息解码器（gob → 业务结构体）
}
```

`Gate.Dispatch` 流程：
1. 查路由表获取 `MsgRoute`
2. 用 `Decoder` 解码 payload → `msg.Data`
3. 构建 `ActorMessage`，`Responder` 设为 Session
4. 按 `Target` 分发：
   - `TargetPlayer`：检查 `sess.RoleID() != 0`，调用 `mgr.SendToPlayer(roleID, msg)`
   - `TargetNamed`：调用 `mgr.SendToNamed(name, msg)`

#### Server

同时监听 TCP 和 WebSocket，每条连接起一个 goroutine 运行读循环：

```
TCP Accept ──→ handleConn ──→ ReadFrame 循环 ──→ Gate.Dispatch
WS Upgrade ──→ handleConn ──┘
```

心跳检测：`heartbeatLoop` 定时遍历所有 Session，超时则 `go sess.Close()`（非阻塞）。

优雅关闭 `Stop()`：并行关闭所有 Session → `wg.Wait()` 等待所有 goroutine 退出。

---

### service — 业务服务

实现具体的业务逻辑，通过 Actor handler 注册到 ActorManager。

#### LoginService（命名服务）

| 项目 | 说明 |
|------|------|
| Actor 类型 | `NamedActor`（名称 "login"，workers=1 串行） |
| 处理消息 | `MsgIDLogin` |
| 注册方式 | `loginActor.MustRegisterHandler(MsgIDLogin, loginSvc.HandleLogin)` |

HandleLogin 流程：
1. 类型断言 `msg.Data` → `*LoginMessage`
2. 类型断言 `ctx.Responder` → `*Session`
3. `sessMgr.BindRole(sess, roleID)` — 绑定 RoleID
4. `actorMgr.CreatePlayerActor(roleID, sess)` — 创建玩家 Actor
5. `ctx.Reply(MsgIDLoginAck, LoginAckMessage{...})` — 回复成功

#### HandleEcho（PlayerActor handler）

| 项目 | 说明 |
|------|------|
| Actor 类型 | 所有 `PlayerActor` 共享 |
| 处理消息 | `MsgIDEcho` |
| 注册方式 | `mgr.MustRegisterPlayerHandler(MsgIDEcho, HandleEcho)` |

HandleEcho 流程：
1. 类型断言 `msg.Data` → `*EchoMessage`
2. `ctx.Reply(MsgIDEchoAck, EchoAckMessage{Text: msg.Text})` — 回显回复

#### Init 初始化

```go
func Init(mgr *actorcore.ActorManager, sessMgr *netlayer.SessionManager)
```

在服务器启动前调用，创建 login NamedActor 并注册所有 handler。

---

## 通信协议

### 消息 ID 定义

| MsgID | 常量 | 方向 | 结构体 | 说明 |
|-------|------|------|--------|------|
| 0 | `MsgIDHeartbeat` | C→S | `HeartbeatMessage` | 心跳（协议层直接消费，不分发到 Gate） |
| 1 | `MsgIDLogin` | C→S | `LoginMessage` | 登录请求 |
| 2 | `MsgIDLoginAck` | S→C | `LoginAckMessage` | 登录回复 |
| 3 | `MsgIDEcho` | C→S | `EchoMessage` | 回显请求 |
| 4 | `MsgIDEchoAck` | S→C | `EchoAckMessage` | 回显回复 |

### 消息结构体

```go
LoginMessage    { RoleID uint64 }
LoginAckMessage { Success bool; Message string }
EchoMessage     { Text string }
EchoAckMessage  { Text string }
HeartbeatMessage{ Timestamp int64 }
```

---

## 消息流转

### 登录流程

```
客户端                    Gate                    ActorManager              SessionManager
  │                        │                         │                         │
  │── Login(RoleID) ──────►│                         │                         │
  │                        │  解码 LoginMessage       │                         │
  │                        │── SendToNamed("login") ─►│                         │
  │                        │                         │── login NamedActor ────►│
  │                        │                         │   HandleLogin:           │
  │                        │                         │   BindRole(sess, roleID) │
  │                        │                         │   CreatePlayerActor()    │
  │                        │                         │   Reply(LoginAck)        │
  │◄── LoginAck ────────────────────────────────────│                         │
  │                        │                         │                         │
```

### 回显流程

```
客户端                    Gate                    PlayerActor
  │                        │                         │
  │── Echo(Text) ─────────►│                         │
  │                        │  检查 RoleID != 0        │
  │                        │  解码 EchoMessage        │
  │                        │── SendToPlayer(roleID) ─►│
  │                        │                         │  HandleEcho:
  │                        │                         │  Reply(EchoAck)
  │◄── EchoAck ─────────────────────────────────────│
  │                        │                         │
```

### 未登录回显（拒绝）

```
客户端                    Gate
  │── Echo(Text) ─────────►│
  │                        │  检查 RoleID == 0
  │                        │  返回错误 "player not logged in"
  │                        │  (客户端无响应，服务端记录日志)
```

---

## 启动流程

`main.go` 中的初始化顺序：

```
1. actorcore.GetActorManager()        // 获取 ActorManager 单例
2. netlayer.NewGate(mgr)              // 创建 Gate 网关
3. netlayer.NewServer(cfg, gate)      // 创建服务器
4. registerRoutes(gate)               // 注册消息路由表
5. service.Init(mgr, sessMgr)         // 初始化业务服务
6. server.Start()                     // 启动 TCP + WS 监听
7. 等待 SIGINT/SIGTERM                // 阻塞等待中断信号
8. server.Stop() + mgr.Stop()         // 优雅关闭
```

---

## 配置说明

`netlayer.DefaultConfig()` 默认配置：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `TCPAddr` | `:9000` | TCP 监听地址 |
| `WSAddr` | `:8080` | WebSocket 监听地址 |
| `WSPath` | `/ws` | WebSocket 路径 |
| `SendQueueSize` | `1024` | 单连接发送队列大小 |
| `ReadBufferSize` | `16KB` | 读缓冲区大小 |
| `WriteBufferSize` | `16KB` | 写缓冲区大小 |
| `HeartbeatInterval` | `30s` | 心跳检查间隔 |
| `HeartbeatTimeout` | `90s` | 心跳超时时间 |

---

## 扩展指南

### 新增 PlayerActor 消息处理

1. 在 `netlayer/message.go` 定义消息 ID 和结构体
2. 在 `service/` 中编写 handler 函数：

```go
func HandleChat(ctx *actorcore.ActorContext, msg *actorcore.ActorMessage) error {
    chatMsg := msg.Data.(*netlayer.ChatMessage)
    // 业务逻辑...
    return ctx.Reply(netlayer.MsgIDChatAck, netlayer.ChatAckMessage{...})
}
```

3. 在 `service.Init()` 中注册：

```go
mgr.MustRegisterPlayerHandler(netlayer.MsgIDChat, HandleChat)
```

4. 在 `main.go` 的 `registerRoutes` 中添加路由：

```go
gate.MustRegisterRoute(netlayer.MsgIDChat, netlayer.MsgRoute{
    Target:  netlayer.TargetPlayer,
    Decoder: netlayer.MakeDecoder(func() any { return &netlayer.ChatMessage{} }),
})
```

### 新增命名服务

1. 编写服务结构体和 handler：

```go
type RankService struct {
    actorMgr *actorcore.ActorManager
}

func (s *RankService) HandleUpdate(ctx *actorcore.ActorContext, msg *actorcore.ActorMessage) error {
    // 排行榜更新逻辑...
    return nil
}
```

2. 在 `service.Init()` 中创建 NamedActor 并注册：

```go
_, rankActor, _ := mgr.CreateNamedActor("rank", 1) // 串行处理
rankSvc := NewRankService(mgr)
rankActor.MustRegisterHandler(netlayer.MsgIDRankUpdate, rankSvc.HandleUpdate)
```

3. 如果需要从客户端直接访问，在 `registerRoutes` 中添加 `TargetNamed` 路由。

### Actor 间通信

在任意 handler 中通过 `ActorContext` 发送消息：

```go
func HandleSomething(ctx *actorcore.ActorContext, msg *actorcore.ActorMessage) error {
    // 向排行榜服务发送消息
    ctx.SendToNamed("rank", &actorcore.ActorMessage{
        MsgID: MsgIDRankUpdate,
        Data:  &RankUpdateData{...},
    })

    // 向另一个玩家发送消息
    ctx.SendToPlayer(targetRoleID, &actorcore.ActorMessage{
        MsgID: MsgIDNotify,
        Data:  &NotifyData{...},
    })

    return nil
}
```

---

## 关键设计决策

### 1. 为什么用 Actor 模型而不是锁？

游戏服务器中，玩家数据（背包、状态、位置等）需要并发保护。传统方案是对每块数据加锁，容易死锁且难以维护。Actor 模型让每个玩家的消息在独立 goroutine 中串行处理，天然无竞争，代码更简洁。

### 2. 为什么 Gate 不做业务逻辑？

Gate 是纯转发层，只负责解码和路由。业务逻辑在 Actor 中执行。这样网络层可以独立测试和复用，业务变更不影响网络框架。

### 3. 为什么 Responder 定义在 actorcore 而不是 netlayer？

依赖倒置。如果 Responder 在 netlayer，actorcore 需要导入 netlayer，形成 `netlayer → actorcore → netlayer` 循环依赖。将接口定义在 actorcore，netlayer.Session 隐式实现，依赖方向保持单向。

### 4. 为什么 PlayerActor 共享 handler 注册表？

所有玩家处理相同的消息集（登录后可用的功能相同），共享 handler map 避免每个 PlayerActor 重复注册。NamedActor 则各有独立 handler，因为不同服务处理不同消息。

### 5. 为什么用 closeSig + recover 而不是 mutex？

向已关闭 channel 写入会 panic。`closeSig` 提供快速检查路径，`recover` 兜底处理 closeSig 检查与 channel 写入之间的竞态窗口。比 mutex 性能更好（无锁竞争），且不会阻塞 Actor 消息处理。

### 6. 为什么 BindRole 要先 SetRoleID(0) 再 Close？

顶号场景下，旧 Session 关闭时 `onClose` 回调会调用 `Remove`，`Remove` 检查 `byRole[roleID] == sess` 来决定是否删除索引。如果不先清除 RoleID，旧 Session 的 `Remove` 可能误删新 Session 的索引（虽然当前有指针一致性检查 `cur == sess` 可以避免，但 SetRoleID(0) 是更清晰的语义保证）。

---

## 后续规划

| 阶段 | 内容 | 状态 |
|------|------|------|
| Phase 1 | actorcore 核心层 | ✅ 已完成 |
| Phase 2 | netlayer Gate 网关改造 | ✅ 已完成 |
| Phase 3 | service 业务服务（login + echo） | ✅ 已完成 |
| Phase 4 | timer 定时器 + client 测试客户端 | 🔲 待实现 |
| Phase 5 | Actor 间通信、广播、持久化、多节点扩展 | 🔲 待实现 |

### Phase 4 预期

- **Timer**：基于 Actor 模型的定时器，到时向目标 Actor 投递消息（如玩家在线奖励、Buff 过期）
- **Client**：TCP/WebSocket 测试客户端，验证 login → echo 完整流程

### Phase 5 预期

- **广播**：通过 ActorManager 遍历所有 PlayerActor 发送消息
- **持久化**：PlayerActor 状态序列化/反序列化
- **多节点**：Actor 地址扩展节点 ID，跨节点消息转发

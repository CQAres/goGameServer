package netlayer

import "sync"

// SessionManager 管理所有在线 Session，维护 sessionID 和 roleID 双向索引
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[uint64]*Session // sessionID -> Session
	byRole   map[uint64]*Session // roleID -> Session
	nextID   uint64
}

// NewSessionManager 创建 SessionManager
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[uint64]*Session),
		byRole:   make(map[uint64]*Session),
	}
}

// NextID 生成下一个 Session ID
func (m *SessionManager) NextID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	return m.nextID
}

// Add 添加 Session
func (m *SessionManager) Add(sess *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[sess.ID()] = sess
}

// BindRole 绑定 RoleID，若已有同 RoleID 的 Session 则顶掉旧连接
func (m *SessionManager) BindRole(sess *Session, roleID uint64) {
	m.mu.Lock()
	var old *Session
	if o, ok := m.byRole[roleID]; ok && o != sess {
		old = o
		delete(m.sessions, old.ID())
	}
	sess.SetRoleID(roleID)
	m.byRole[roleID] = sess
	m.mu.Unlock()
	if old != nil {
		go old.Close()
	}
}

// Remove 移除 Session（仅当 byRole 中的指针与当前一致时才删除）
func (m *SessionManager) Remove(sess *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sess.ID())
	if sess.RoleID() != 0 {
		if cur, ok := m.byRole[sess.RoleID()]; ok && cur == sess {
			delete(m.byRole, sess.RoleID())
		}
	}
}

// GetByRole 根据 RoleID 获取 Session
func (m *SessionManager) GetByRole(roleID uint64) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.byRole[roleID]
	return s, ok
}

// Get 根据 Session ID 获取
func (m *SessionManager) Get(id uint64) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	return s, ok
}

// Count 返回当前 Session 数量
func (m *SessionManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// Range 遍历所有 Session（遍历期间不持锁，回调内可安全操作 SessionManager）
func (m *SessionManager) Range(fn func(*Session) bool) {
	m.mu.RLock()
	list := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		list = append(list, s)
	}
	m.mu.RUnlock()
	for _, s := range list {
		if !fn(s) {
			return
		}
	}
}

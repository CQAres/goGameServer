package netlayer

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/gorilla/websocket"
)

// Connection 抽象 TCP 和 WebSocket 连接，统一读写帧接口
type Connection interface {
	// ReadFrame 读取一帧消息，返回 (MsgID, Payload)
	ReadFrame() (uint16, []byte, error)
	// WriteFrame 写入一帧消息
	WriteFrame(msgID uint16, payload []byte) error
	// Close 关闭连接
	Close() error
	// RemoteAddr 远端地址
	RemoteAddr() string
}

// ---------------- TCP ----------------

// TCPConnection TCP 连接实现，自行处理粘包/拆包
type TCPConnection struct {
	conn net.Conn
	mu   sync.Mutex
}

func NewTCPConnection(conn net.Conn) *TCPConnection {
	return &TCPConnection{conn: conn}
}

func (tcp *TCPConnection) ReadFrame() (uint16, []byte, error) {
	var lenBuf [lenFieldSize]byte
	if _, err := io.ReadFull(tcp.conn, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length < msgIDFieldSize || length > maxPayloadSize {
		return 0, nil, fmt.Errorf("invalid frame length: %d", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(tcp.conn, data); err != nil {
		return 0, nil, err
	}
	msgID := binary.BigEndian.Uint16(data[0:msgIDFieldSize])
	return msgID, data[msgIDFieldSize:], nil
}

func (tcp *TCPConnection) WriteFrame(msgID uint16, payload []byte) error {
	buf, err := EncodePacket(msgID, payload)
	if err != nil {
		return err
	}
	tcp.mu.Lock()
	defer tcp.mu.Unlock()
	_, err = tcp.conn.Write(buf)
	return err
}

func (tcp *TCPConnection) Close() error       { return tcp.conn.Close() }
func (tcp *TCPConnection) RemoteAddr() string { return tcp.conn.RemoteAddr().String() }

// ---------------- WebSocket ----------------

// WSConnection WebSocket 连接实现，仍按统一帧协议 [长度][MsgID][Payload] 解析
// WS 原生有消息边界，但为与 TCP 共用同一套处理逻辑，写入时仍带长度前缀
type WSConnection struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func NewWSConnection(conn *websocket.Conn) *WSConnection {
	return &WSConnection{conn: conn}
}

func (ws *WSConnection) ReadFrame() (uint16, []byte, error) {
	_, data, err := ws.conn.ReadMessage()
	if err != nil {
		return 0, nil, err
	}
	if len(data) < headerSize {
		return 0, nil, fmt.Errorf("ws frame too short: %d", len(data))
	}
	length := binary.BigEndian.Uint32(data[0:lenFieldSize])
	if length < msgIDFieldSize || length > maxPayloadSize {
		return 0, nil, fmt.Errorf("invalid frame length: %d", length)
	}
	if int(length) != len(data)-lenFieldSize {
		return 0, nil, fmt.Errorf("ws length mismatch: declared=%d, actual=%d",
			length, len(data)-lenFieldSize)
	}
	msgID := binary.BigEndian.Uint16(data[lenFieldSize:headerSize])
	return msgID, data[headerSize:], nil
}

func (ws *WSConnection) WriteFrame(msgID uint16, payload []byte) error {
	buf, err := EncodePacket(msgID, payload)
	if err != nil {
		return err
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.conn.WriteMessage(websocket.BinaryMessage, buf)
}

func (ws *WSConnection) Close() error       { return ws.conn.Close() }
func (ws *WSConnection) RemoteAddr() string { return ws.conn.RemoteAddr().String() }

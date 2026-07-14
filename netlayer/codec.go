package netlayer

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
)

const (
	lenFieldSize   = 4                             // 长度字段 4 字节 (uint32, big-endian)
	msgIDFieldSize = 2                             // MsgID 字段 2 字节 (uint16, big-endian)
	headerSize     = lenFieldSize + msgIDFieldSize // 帧头总大小 6 字节
	maxPayloadSize = 16 * 1024 * 1024              // 单帧 Payload 最大 16MB
)

// EncodePacket 编码完整数据包：[4字节长度][2字节MsgID][Payload]
// 长度字段值 = msgIDFieldSize + len(payload)
func EncodePacket(msgID uint16, payload []byte) ([]byte, error) {
	length := uint32(msgIDFieldSize + len(payload))
	buf := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], length)
	binary.BigEndian.PutUint16(buf[4:6], msgID)
	copy(buf[6:], payload)
	return buf, nil
}

// EncodePayload 用 gob 编码消息体
func EncodePayload(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, fmt.Errorf("gob encode: %w", err)
	}
	return buf.Bytes(), nil
}

// DecodePayload 用 gob 解码消息体
func DecodePayload(data []byte, v any) error {
	if len(data) == 0 {
		return nil
	}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(v); err != nil {
		return fmt.Errorf("gob decode: %w", err)
	}
	return nil
}

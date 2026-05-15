package serializer

import (
	"encoding/binary"
	"errors"
)

const (
	UINT8_SIZE  uint32 = 1
	UINT16_SIZE uint32 = 2
	UINT32_SIZE uint32 = 4
	UINT64_SIZE uint32 = 8
)

var ErrStringTooLong = errors.New("short string exceeds 255 bytes")

func SerializeUint8(value uint8) []byte {
	return []byte{value}
}

func DeserializeUint8(bytes []byte) uint8 {
	return bytes[0]
}

func SerializeUint16(value uint16) []byte {
	buf := make([]byte, UINT16_SIZE)
	binary.BigEndian.PutUint16(buf, value)
	return buf
}

func DeserializeUint16(bytes []byte) uint16 {
	return binary.BigEndian.Uint16(bytes)
}

func SerializeUint32(value uint32) []byte {
	buf := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(buf, value)
	return buf
}

func DeserializeUint32(bytes []byte) uint32 {
	return binary.BigEndian.Uint32(bytes)
}

func SerializeUint64(value uint64) []byte {
	buf := make([]byte, UINT64_SIZE)
	binary.BigEndian.PutUint64(buf, value)
	return buf
}

func DeserializeUint64(bytes []byte) uint64 {
	return binary.BigEndian.Uint64(bytes)
}

// SerializeShortString encodes a string prefixed by a uint8 length (0..255).
func SerializeShortString(value string) ([]byte, error) {
	if len(value) > 255 {
		return nil, ErrStringTooLong
	}
	buf := make([]byte, 1+len(value))
	buf[0] = uint8(len(value))
	copy(buf[1:], value)
	return buf, nil
}

// DeserializeShortString decodes a string of the given length from the buffer.
func DeserializeShortString(bytes []byte) string {
	return string(bytes)
}

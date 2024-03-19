package util

import (
	"bytes"
	"encoding/binary"
)

func IntToBytes(n int) []byte {
	x := int64(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)
	var x int64
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return int(x)
}

// Combine two unsigned 32-bit integers into one unsigned 64-bit integer
func CombineUint32(a, b uint32) uint64 {
	return (uint64(a) << 32) | uint64(b)
}

// Disassemble an unsigned 64-bit integer into two unsigned 32-bit integers
func DisassembleUint64(c uint64) (a, b uint32) {
	a = uint32(c >> 32)
	b = uint32(c << 32 >> 32)
	return
}

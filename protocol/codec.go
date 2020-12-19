package protocol

import "encoding/binary"

func int4ToBytes(input uint32) []byte {
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, input)
	return ret
}

func BytesToInt4(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

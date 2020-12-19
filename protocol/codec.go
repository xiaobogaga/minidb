package protocol

import "encoding/binary"

func int2ToBytes(input uint32) []byte {
	ret := make([]byte, 2)
	ret[0] = byte(input)
	ret[1] = byte(input >> 8)
	return ret
}

func bytes2ToInt(bs []byte) (output uint32) {
	output |= uint32(bs[0])
	output |= uint32(bs[1] << 8)
	return
}

func int3ToBytes(input uint32) []byte {
	ret := make([]byte, 3)
	ret[0] = byte(input)
	ret[1] = byte(input >> 8)
	ret[2] = byte(input >> 16)
	return ret
}

func decodeInt3Bytes(input []byte) uint32 {
	return uint32(input[0]) + uint32(input[1])<<8 + uint32(input[2])<<16
}

func int4ToBytes(input uint32) []byte {
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, input)
	return ret
}

func decodeInt4Bytes(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func int8ToBytes(input uint64) []byte {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint32(ret[:4], uint32(input))
	binary.LittleEndian.PutUint32(ret[4:], uint32(input>>32))
	return ret
}

func lengthEncodedInt(len uint64) []byte {
	if len < uint64(251) {
		bs := make([]byte, 1)
		bs[0] = byte(len)
		return bs
	}
	if len < uint64(65536) {
		bss := int2ToBytes(uint32(len))
		bs := make([]byte, 3)
		bs[0], bs[1], bs[2] = 252, bss[1], bss[2]
		return bs
	}
	if len < uint64(16777216) {
		bss := int3ToBytes(uint32(len))
		bs := make([]byte, 4)
		bs[0], bs[1], bs[2], bs[3] = 253, bss[0], bss[1], bss[2]
		return bs
	}
	bs := make([]byte, 9)
	bss := int8ToBytes(len)
	bs[0] = 254
	bs[1], bs[2], bs[3], bs[4] = bss[0], bss[1], bss[2], bss[3]
	bs[5], bs[6], bs[7], bs[8] = bss[4], bss[5], bss[6], bss[7]
	return bs
}

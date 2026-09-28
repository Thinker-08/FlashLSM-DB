package sstable

import (
	"encoding/binary"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

// Table: data blocks | filter | properties | index | footer. Each block has a
// trailer: compression type (1 byte) | CRC-32C of contents and type (uint32 LE).
// Footer (64 bytes, LE): index, filter, properties handles, each fixed64 offset
// and length (0-47) | version uint32 (48) | CRC-32C of 0-51 (52) | magic (56).
const (
	FooterLen       = 64
	blockTrailerLen = 5
	formatVersion   = 1
	footerMagic     = "LSMKV001"
)

// BlockHandle's Length excludes the trailer.
type BlockHandle struct {
	Offset, Length uint64
}

func (h BlockHandle) appendVarint(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, h.Offset)
	return binary.AppendUvarint(dst, h.Length)
}

func decodeHandleVarint(encoded []byte) (BlockHandle, bool) {
	offset, offsetWidth := binary.Uvarint(encoded)
	if offsetWidth <= 0 {
		return BlockHandle{}, false
	}
	length, lengthWidth := binary.Uvarint(encoded[offsetWidth:])
	if lengthWidth <= 0 || offsetWidth+lengthWidth != len(encoded) {
		return BlockHandle{}, false
	}
	return BlockHandle{Offset: offset, Length: length}, true
}

type footer struct {
	index, filter, properties BlockHandle
	version                   uint32
}

func (ft footer) encode() []byte {
	buf := make([]byte, FooterLen)
	byteOrder := binary.LittleEndian
	byteOrder.PutUint64(buf[0:], ft.index.Offset)
	byteOrder.PutUint64(buf[8:], ft.index.Length)
	byteOrder.PutUint64(buf[16:], ft.filter.Offset)
	byteOrder.PutUint64(buf[24:], ft.filter.Length)
	byteOrder.PutUint64(buf[32:], ft.properties.Offset)
	byteOrder.PutUint64(buf[40:], ft.properties.Length)
	byteOrder.PutUint32(buf[48:], ft.version)
	byteOrder.PutUint32(buf[52:], base.CRC(buf[:52]))
	copy(buf[56:], footerMagic)
	return buf
}

func decodeFooter(encoded []byte, fileSize uint64) (footer, error) {
	if len(encoded) != FooterLen {
		return footer{}, base.CorruptionErrorf("footer is %d bytes", len(encoded))
	}
	if string(encoded[56:]) != footerMagic {
		return footer{}, base.CorruptionErrorf("bad magic number")
	}
	byteOrder := binary.LittleEndian
	if byteOrder.Uint32(encoded[52:]) != base.CRC(encoded[:52]) {
		return footer{}, base.CorruptionErrorf("footer checksum mismatch")
	}
	decoded := footer{
		index:      BlockHandle{byteOrder.Uint64(encoded[0:]), byteOrder.Uint64(encoded[8:])},
		filter:     BlockHandle{byteOrder.Uint64(encoded[16:]), byteOrder.Uint64(encoded[24:])},
		properties: BlockHandle{byteOrder.Uint64(encoded[32:]), byteOrder.Uint64(encoded[40:])},
		version:    byteOrder.Uint32(encoded[48:]),
	}
	if decoded.version != formatVersion {
		return footer{}, base.CorruptionErrorf("unsupported format version %d", decoded.version)
	}
	blocksEnd := fileSize - FooterLen
	for _, handle := range []BlockHandle{decoded.index, decoded.properties} {
		if !handleInBounds(handle, blocksEnd) {
			return footer{}, base.CorruptionErrorf("block handle %+v outside the file", handle)
		}
	}
	if decoded.filter.Length > 0 && !handleInBounds(decoded.filter, blocksEnd) {
		return footer{}, base.CorruptionErrorf("filter handle %+v outside the file", decoded.filter)
	}
	return decoded, nil
}

// handleInBounds avoids additions so hostile handles cannot overflow uint64.
func handleInBounds(handle BlockHandle, limit uint64) bool {
	return handle.Offset <= limit && handle.Length <= limit-handle.Offset && limit-handle.Offset-handle.Length >= blockTrailerLen
}

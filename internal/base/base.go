package base

import (
	"errors"
	"fmt"
	"hash/crc32"
)

var (
	ErrNotFound   = errors.New("lsmkv: not found")
	ErrCorruption = errors.New("lsmkv: corruption")
)

func CorruptionErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorruption, fmt.Sprintf(format, args...))
}

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func CRC(data []byte) uint32 { return crc32.Checksum(data, castagnoliTable) }

func CRCUpdate(crc uint32, data []byte) uint32 { return crc32.Update(crc, castagnoliTable, data) }

// Key and Value are valid until the next positioning call; errors make Valid false.
type InternalIterator interface {
	SeekGE(key InternalKey)
	SeekLT(key InternalKey)
	First()
	Last()
	Next()
	Prev()
	Valid() bool
	Key() InternalKey
	Value() []byte
	Error() error
	Close() error
}

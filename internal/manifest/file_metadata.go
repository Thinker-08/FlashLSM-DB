package manifest

import (
	"fmt"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

type FileMetadata struct {
	FileNum     uint64
	Size        uint64
	Smallest    base.InternalKey
	Largest     base.InternalKey
	SmallestSeq uint64
	LargestSeq  uint64

	// Compacting is guarded by the DB mutex.
	Compacting bool
}

func (fm *FileMetadata) String() string {
	return fmt.Sprintf("%06d:[%s-%s] %d bytes, seq %d-%d", fm.FileNum, fm.Smallest, fm.Largest, fm.Size, fm.SmallestSeq, fm.LargestSeq)
}

func (fm *FileMetadata) ContainsUserKey(compare base.Compare, key []byte) bool {
	return compare(key, fm.Smallest.UserKey) >= 0 && compare(key, fm.Largest.UserKey) <= 0
}

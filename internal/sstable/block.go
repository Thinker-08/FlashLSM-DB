package sstable

import (
	"encoding/binary"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

// Block format (lengths are uvarints, restart fields uint32 LE):
//
//	entry := shared | unshared | valueLen | keySuffix | value
//	block := entry* | restartOffset* | numRestarts
//
// Restart points (every restartInterval entries) must have shared = 0, since
// seeks and Prev start decoding there.

type blockWriter struct {
	restartInterval     int
	buf                 []byte
	restarts            []uint32
	entriesSinceRestart int
	numEntries          int
	lastKey             []byte
}

func (bw *blockWriter) reset() {
	bw.buf = bw.buf[:0]
	bw.restarts = bw.restarts[:0]
	bw.entriesSinceRestart = 0
	bw.numEntries = 0
	bw.lastKey = bw.lastKey[:0]
}

func (bw *blockWriter) empty() bool { return bw.numEntries == 0 }

func (bw *blockWriter) add(key, value []byte) {
	shared := 0
	if bw.numEntries == 0 || bw.entriesSinceRestart >= bw.restartInterval {
		bw.restarts = append(bw.restarts, uint32(len(bw.buf)))
		bw.entriesSinceRestart = 0
	} else {
		shared = base.SharedPrefixLen(bw.lastKey, key)
	}
	bw.buf = binary.AppendUvarint(bw.buf, uint64(shared))
	bw.buf = binary.AppendUvarint(bw.buf, uint64(len(key)-shared))
	bw.buf = binary.AppendUvarint(bw.buf, uint64(len(value)))
	bw.buf = append(bw.buf, key[shared:]...)
	bw.buf = append(bw.buf, value...)
	bw.lastKey = append(bw.lastKey[:0], key...)
	bw.entriesSinceRestart++
	bw.numEntries++
}

func (bw *blockWriter) estimatedSize() int {
	return len(bw.buf) + 4*max(len(bw.restarts), 1) + 4
}

func (bw *blockWriter) finish() []byte {
	if len(bw.restarts) == 0 {
		bw.restarts = append(bw.restarts, 0)
	}
	for _, offset := range bw.restarts {
		bw.buf = binary.LittleEndian.AppendUint32(bw.buf, offset)
	}
	bw.buf = binary.LittleEndian.AppendUint32(bw.buf, uint32(len(bw.restarts)))
	return bw.buf
}

// blockIter must not panic on malformed blocks; it reports corruption instead.
type blockIter struct {
	compare        base.Compare
	data           []byte
	restartsOffset int
	numRestarts    int
	restartIndex   int
	offset         int
	nextOffset     int
	key            []byte
	internalKey    base.InternalKey
	value          []byte
	valid          bool
	err            error
	rawKeys        bool
}

func (bi *blockIter) init(compare base.Compare, block []byte, rawKeys bool) error {
	keyBuf := bi.key[:0]
	*bi = blockIter{compare: compare, key: keyBuf, rawKeys: rawKeys}
	if len(block) < 4 {
		return bi.corrupt("block of %d bytes is too short", len(block))
	}
	numRestarts := binary.LittleEndian.Uint32(block[len(block)-4:])
	if numRestarts == 0 || uint64(numRestarts) > uint64(len(block)-4)/4 {
		return bi.corrupt("bad restart count %d for a %d-byte block", numRestarts, len(block))
	}
	bi.data = block
	bi.numRestarts = int(numRestarts)
	bi.restartsOffset = len(block) - 4 - 4*int(numRestarts)
	return nil
}

func (bi *blockIter) corrupt(format string, args ...any) error {
	bi.err = base.CorruptionErrorf("sstable block: "+format, args...)
	bi.valid = false
	return bi.err
}

func (bi *blockIter) restartOffset(index int) (int, bool) {
	offset := binary.LittleEndian.Uint32(bi.data[bi.restartsOffset+4*index:])
	if uint64(offset) > uint64(bi.restartsOffset) {
		bi.corrupt("restart offset %d beyond entries end %d", offset, bi.restartsOffset)
		return 0, false
	}
	return int(offset), true
}

func (bi *blockIter) decodeEntryHeader(offset int) (shared, unshared, valueLen uint64, keyStart int, ok bool) {
	entry := bi.data[offset:bi.restartsOffset]
	var sharedWidth, unsharedWidth, valueLenWidth int
	shared, sharedWidth = binary.Uvarint(entry)
	if sharedWidth <= 0 {
		bi.corrupt("bad shared length at offset %d", offset)
		return
	}
	unshared, unsharedWidth = binary.Uvarint(entry[sharedWidth:])
	if unsharedWidth <= 0 {
		bi.corrupt("bad key length at offset %d", offset)
		return
	}
	valueLen, valueLenWidth = binary.Uvarint(entry[sharedWidth+unsharedWidth:])
	if valueLenWidth <= 0 {
		bi.corrupt("bad value length at offset %d", offset)
		return
	}
	headerLen := sharedWidth + unsharedWidth + valueLenWidth
	remaining := uint64(len(entry) - headerLen)
	if unshared > remaining || valueLen > remaining-unshared {
		bi.corrupt("entry at offset %d overruns the block", offset)
		return
	}
	return shared, unshared, valueLen, offset + headerLen, true
}

func (bi *blockIter) restartKey(index int) (base.InternalKey, bool) {
	offset, ok := bi.restartOffset(index)
	if !ok {
		return base.InternalKey{}, false
	}
	if offset >= bi.restartsOffset {
		bi.corrupt("restart point %d at the end of the block", index)
		return base.InternalKey{}, false
	}
	shared, unshared, _, keyStart, ok := bi.decodeEntryHeader(offset)
	if !ok {
		return base.InternalKey{}, false
	}
	if shared != 0 {
		bi.corrupt("restart point %d has a shared prefix", index)
		return base.InternalKey{}, false
	}
	return bi.decodeKey(bi.data[keyStart : keyStart+int(unshared)])
}

func (bi *blockIter) decodeKey(encoded []byte) (base.InternalKey, bool) {
	if bi.rawKeys {
		return base.InternalKey{UserKey: encoded}, true
	}
	key, ok := base.DecodeInternalKey(encoded)
	if !ok {
		bi.corrupt("internal key of %d bytes is too short", len(encoded))
	}
	return key, ok
}

func (bi *blockIter) seekToRestart(index int) bool {
	offset, ok := bi.restartOffset(index)
	if !ok {
		return false
	}
	bi.restartIndex = index
	bi.key = bi.key[:0]
	bi.nextOffset = offset
	bi.valid = false
	return true
}

func (bi *blockIter) parseNext() bool {
	offset := bi.nextOffset
	if offset >= bi.restartsOffset {
		bi.valid = false
		bi.offset = bi.restartsOffset
		return false
	}
	shared, unshared, valueLen, keyStart, ok := bi.decodeEntryHeader(offset)
	if !ok {
		return false
	}
	if shared > uint64(len(bi.key)) {
		bi.corrupt("entry at offset %d shares %d bytes of a %d-byte key", offset, shared, len(bi.key))
		return false
	}
	bi.key = append(bi.key[:shared], bi.data[keyStart:keyStart+int(unshared)]...)
	valueStart := keyStart + int(unshared)
	bi.value = bi.data[valueStart : valueStart+int(valueLen) : valueStart+int(valueLen)]
	bi.offset = offset
	bi.nextOffset = valueStart + int(valueLen)
	key, ok := bi.decodeKey(bi.key)
	if !ok {
		return false
	}
	bi.internalKey = key
	for bi.restartIndex+1 < bi.numRestarts {
		nextRestart, ok := bi.restartOffset(bi.restartIndex + 1)
		if !ok {
			return false
		}
		if nextRestart > bi.offset {
			break
		}
		bi.restartIndex++
	}
	bi.valid = true
	return true
}

// searchRestarts returns the last restart point whose key is < target, or 0.
func (bi *blockIter) searchRestarts(target base.InternalKey) (int, bool) {
	left, right := 0, bi.numRestarts-1
	for left < right {
		mid := (left + right + 1) / 2
		key, ok := bi.restartKey(mid)
		if !ok {
			return 0, false
		}
		if bi.compareKeys(key, target) < 0 {
			left = mid
		} else {
			right = mid - 1
		}
	}
	return left, true
}

func (bi *blockIter) compareKeys(a, b base.InternalKey) int {
	if bi.rawKeys {
		return bi.compare(a.UserKey, b.UserKey)
	}
	return base.InternalCompare(bi.compare, a, b)
}

func (bi *blockIter) SeekGE(target base.InternalKey) {
	if bi.err != nil {
		return
	}
	index, ok := bi.searchRestarts(target)
	if !ok || !bi.seekToRestart(index) {
		return
	}
	for bi.parseNext() {
		if bi.compareKeys(bi.internalKey, target) >= 0 {
			return
		}
	}
}

func (bi *blockIter) SeekLT(target base.InternalKey) {
	if bi.err != nil {
		return
	}
	index, ok := bi.searchRestarts(target)
	if !ok || !bi.seekToRestart(index) {
		return
	}
	lastBefore := -1
	for bi.parseNext() {
		if bi.compareKeys(bi.internalKey, target) >= 0 {
			break
		}
		lastBefore = bi.offset
	}
	if bi.err != nil || lastBefore < 0 {
		bi.valid = false
		return
	}
	if !bi.seekToRestart(index) {
		return
	}
	for bi.parseNext() && bi.offset < lastBefore {
	}
}

func (bi *blockIter) First() {
	if bi.err != nil {
		return
	}
	if bi.seekToRestart(0) {
		bi.parseNext()
	}
}

func (bi *blockIter) Last() {
	if bi.err != nil {
		return
	}
	if !bi.seekToRestart(bi.numRestarts - 1) {
		return
	}
	for bi.parseNext() {
		if bi.nextOffset >= bi.restartsOffset {
			return
		}
	}
}

func (bi *blockIter) Next() {
	if !bi.valid {
		return
	}
	bi.parseNext()
}

func (bi *blockIter) Prev() {
	if !bi.valid {
		return
	}
	original := bi.offset
	for {
		offset, ok := bi.restartOffset(bi.restartIndex)
		if !ok {
			return
		}
		if offset < original {
			break
		}
		if bi.restartIndex == 0 {
			bi.valid = false
			return
		}
		bi.restartIndex--
	}
	if !bi.seekToRestart(bi.restartIndex) {
		return
	}
	for bi.parseNext() {
		if bi.nextOffset >= original {
			return
		}
	}
}

func (bi *blockIter) Valid() bool           { return bi.valid }
func (bi *blockIter) Key() base.InternalKey { return bi.internalKey }
func (bi *blockIter) Value() []byte         { return bi.value }
func (bi *blockIter) Error() error          { return bi.err }
func (bi *blockIter) Close() error          { return nil }

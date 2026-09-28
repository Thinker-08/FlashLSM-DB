package lsmkv

import (
	"encoding/binary"
	"time"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/memtable"
)

const (
	MaxKeySize   = 64 << 10
	MaxBatchSize = 64 << 20

	batchHeaderLen = 12
	ttlHeaderLen   = 8
)

var emptyBatchHeader [batchHeaderLen]byte

// Batch copies its arguments and belongs to one goroutine at a time; the zero
// Batch is ready to use.
type Batch struct {
	// data is also the WAL payload; op i gets sequence number seqNum+i:
	// seqNum (8 bytes LE) | count (4 bytes LE) | op*, where
	// op = kind (1 byte) | keyLen (uvarint) | key | [valueLen (uvarint) | value]
	data     []byte
	count    uint32
	hasMerge bool
	err      error
}

func NewBatch() *Batch { return &Batch{} }

func (b *Batch) add(kind base.Kind, key, value []byte) {
	if len(key) > MaxKeySize {
		if b.err == nil {
			b.err = ErrKeyTooLarge
		}
		return
	}
	if len(b.data) < batchHeaderLen {
		b.data = append(b.data[:0], emptyBatchHeader[:]...)
	}
	b.data = append(b.data, byte(kind))
	b.data = binary.AppendUvarint(b.data, uint64(len(key)))
	b.data = append(b.data, key...)
	if kind.HasValue() {
		b.data = binary.AppendUvarint(b.data, uint64(len(value)))
		b.data = append(b.data, value...)
	}
	b.count++
	binary.LittleEndian.PutUint32(b.data[8:], b.count)
}

func (b *Batch) Put(key, value []byte) { b.add(base.KindSet, key, value) }

func (b *Batch) Delete(key []byte) { b.add(base.KindDelete, key, nil) }

func (b *Batch) Merge(key, operand []byte) {
	b.hasMerge = true
	b.add(base.KindMerge, key, operand)
}

func (b *Batch) PutWithExpiry(key, value []byte, expiry time.Time) {
	encoded := make([]byte, ttlHeaderLen+len(value))
	binary.LittleEndian.PutUint64(encoded, uint64(expiry.UnixNano()))
	copy(encoded[ttlHeaderLen:], value)
	b.add(base.KindSetTTL, key, encoded)
}

func (b *Batch) Len() int { return int(b.count) }

func (b *Batch) Size() int { return max(len(b.data), batchHeaderLen) }

func (b *Batch) Reset() {
	b.data = b.data[:0]
	b.count = 0
	b.hasMerge = false
	b.err = nil
}

func (b *Batch) validate(merger *Merger) error {
	switch {
	case b.err != nil:
		return b.err
	case len(b.data) > MaxBatchSize:
		return ErrBatchTooLarge
	case b.hasMerge && merger == nil:
		return ErrNoMerger
	}
	return nil
}

func (b *Batch) setSeqNum(seq uint64) { binary.LittleEndian.PutUint64(b.data, seq) }

func (b *Batch) encoded() []byte { return b.data }

func (b *Batch) appendBatch(src *Batch) {
	if src.count == 0 {
		return
	}
	if len(b.data) < batchHeaderLen {
		b.data = append(b.data[:0], emptyBatchHeader[:]...)
	}
	b.data = append(b.data, src.data[batchHeaderLen:]...)
	b.count += src.count
	binary.LittleEndian.PutUint32(b.data[8:], b.count)
}

// batchReader rejects non-minimal varints, so decoding and re-encoding round-trips.
type batchReader struct {
	remaining []byte
	count     uint32
	decoded   uint32
}

func newBatchReader(encoded []byte) (seq uint64, reader batchReader, err error) {
	if len(encoded) < batchHeaderLen {
		return 0, reader, base.CorruptionErrorf("batch of %d bytes is shorter than its header", len(encoded))
	}
	seq = binary.LittleEndian.Uint64(encoded)
	reader = batchReader{remaining: encoded[batchHeaderLen:], count: binary.LittleEndian.Uint32(encoded[8:])}
	if reader.count > 0 && (seq == 0 || seq > base.SeqNumMax || uint64(reader.count)-1 > base.SeqNumMax-seq) {
		return 0, reader, base.CorruptionErrorf("batch sequence numbers %d+%d are out of range", seq, reader.count)
	}
	return seq, reader, nil
}

func (r *batchReader) next() (kind base.Kind, key, value []byte, ok bool, err error) {
	if len(r.remaining) == 0 {
		if r.decoded != r.count {
			return 0, nil, nil, false, base.CorruptionErrorf("batch holds %d operations, header says %d", r.decoded, r.count)
		}
		return 0, nil, nil, false, nil
	}
	if r.decoded == r.count {
		return 0, nil, nil, false, base.CorruptionErrorf("batch has trailing bytes after %d operations", r.count)
	}
	kind = base.Kind(r.remaining[0])
	if !kind.Valid() {
		return 0, nil, nil, false, base.CorruptionErrorf("batch operation has unknown kind %d", kind)
	}
	r.remaining = r.remaining[1:]
	if key, err = r.lengthPrefixed(); err != nil {
		return 0, nil, nil, false, err
	}
	if kind.HasValue() {
		if value, err = r.lengthPrefixed(); err != nil {
			return 0, nil, nil, false, err
		}
		if kind == base.KindSetTTL && len(value) < ttlHeaderLen {
			return 0, nil, nil, false, base.CorruptionErrorf("TTL value of %d bytes has no expiry", len(value))
		}
	}
	r.decoded++
	return kind, key, value, true, nil
}

func (r *batchReader) lengthPrefixed() ([]byte, error) {
	length, width := binary.Uvarint(r.remaining)
	if width <= 0 || width != uvarintLen(length) {
		return nil, base.CorruptionErrorf("batch has a malformed length")
	}
	if length > uint64(len(r.remaining)-width) {
		return nil, base.CorruptionErrorf("batch length %d overruns the batch", length)
	}
	data := r.remaining[width : width+int(length) : width+int(length)]
	r.remaining = r.remaining[width+int(length):]
	return data, nil
}

func uvarintLen(value uint64) int {
	width := 1
	for value >= 0x80 {
		value >>= 7
		width++
	}
	return width
}

func applyBatch(mem *memtable.Memtable, encoded []byte) error {
	seq, reader, err := newBatchReader(encoded)
	if err != nil {
		return err
	}
	for {
		kind, key, value, ok, err := reader.next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := mem.Add(seq, kind, key, value); err != nil {
			return err
		}
		seq++
	}
}

func validateBatch(encoded []byte) (uint64, error) {
	_, reader, err := newBatchReader(encoded)
	if err != nil {
		return 0, err
	}
	for {
		_, _, _, ok, err := reader.next()
		if err != nil {
			return 0, err
		}
		if !ok {
			return uint64(reader.count), nil
		}
	}
}

func decodeTTL(encoded []byte, now int64) (value []byte, live bool, err error) {
	if len(encoded) < ttlHeaderLen {
		return nil, false, base.CorruptionErrorf("TTL value of %d bytes has no expiry", len(encoded))
	}
	expiry := int64(binary.LittleEndian.Uint64(encoded))
	return encoded[ttlHeaderLen:], now < expiry, nil
}

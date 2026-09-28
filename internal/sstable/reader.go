package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/bloom"
	"github.com/Thinker-08/FlashLSM-DB/internal/cache"
)

type ReadableFile interface {
	io.ReaderAt
	io.Closer
}

type Stats struct {
	BlockReads      atomic.Int64
	BlockBytesRead  atomic.Int64
	BlockCacheHits  atomic.Int64
	FilterChecks    atomic.Int64
	FilterNegatives atomic.Int64
}

type ReaderOptions struct {
	Comparer *base.Comparer
	Cache    *cache.Cache
	FileNum  uint64
	Name     string
	Stats    *Stats
}

// A Reader is safe for concurrent use; all reads go through ReadAt.
type Reader struct {
	file       ReadableFile
	size       uint64
	fileNum    uint64
	name       string
	compare    base.Compare
	blockCache *cache.Cache
	stats      *Stats

	footer     footer
	indexBlock []byte
	filter     bloom.Filter
	props      Properties
}

// On success the Reader owns file.
func Open(file ReadableFile, size uint64, opts ReaderOptions) (*Reader, error) {
	if opts.Comparer == nil {
		opts.Comparer = base.DefaultComparer
	}
	if opts.Stats == nil {
		opts.Stats = new(Stats)
	}
	r := &Reader{
		file: file, size: size, fileNum: opts.FileNum, name: opts.Name,
		compare: opts.Comparer.Compare, blockCache: opts.Cache, stats: opts.Stats,
	}
	if size < FooterLen {
		return nil, r.corrupt("file of %d bytes is too short for a footer", size)
	}
	footerBuf := make([]byte, FooterLen)
	if err := r.readAt(footerBuf, size-FooterLen); err != nil {
		return nil, err
	}
	tableFooter, err := decodeFooter(footerBuf, size)
	if err != nil {
		return nil, r.wrap(err)
	}
	r.footer = tableFooter
	if r.indexBlock, err = r.readBlock(tableFooter.index); err != nil {
		return nil, err
	}
	var indexIter blockIter
	if err := indexIter.init(r.compare, r.indexBlock, false); err != nil {
		return nil, r.wrap(err)
	}
	propsBlock, err := r.readBlock(tableFooter.properties)
	if err != nil {
		return nil, err
	}
	if r.props, err = decodeProperties(propsBlock); err != nil {
		return nil, r.wrap(err)
	}
	if r.props.ComparerName != "" && r.props.ComparerName != opts.Comparer.Name {
		return nil, fmt.Errorf("sstable %s: written with comparer %q, opened with %q", r.name, r.props.ComparerName, opts.Comparer.Name)
	}
	if tableFooter.filter.Length > 0 && r.props.FilterPolicy == bloom.HashName {
		if r.filter, err = r.readBlock(tableFooter.filter); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *Reader) corrupt(format string, args ...any) error {
	return base.CorruptionErrorf("sstable %s: "+format, append([]any{r.name}, args...)...)
}

func (r *Reader) wrap(err error) error {
	if errors.Is(err, base.ErrCorruption) {
		return fmt.Errorf("sstable %s: %w", r.name, err)
	}
	return err
}

func (r *Reader) readAt(buf []byte, offset uint64) error {
	n, err := r.file.ReadAt(buf, int64(offset))
	if n == len(buf) {
		return nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		return r.corrupt("short read of %d bytes at offset %d", len(buf), offset)
	}
	return fmt.Errorf("sstable %s: %w", r.name, err)
}

func (r *Reader) readBlock(handle BlockHandle) ([]byte, error) {
	return r.readBlockInto(handle, nil)
}

// With a non-nil scratch, the result aliases scratch's buffers until its next use.
func (r *Reader) readBlockInto(handle BlockHandle, scratch *blockScratch) ([]byte, error) {
	if !handleInBounds(handle, r.size-FooterLen) {
		return nil, r.corrupt("block handle %+v outside the file", handle)
	}
	blockLen := int(handle.Length + blockTrailerLen)
	var buf []byte
	if scratch != nil && cap(scratch.raw) >= blockLen {
		buf = scratch.raw[:blockLen]
	} else {
		buf = make([]byte, blockLen)
		if scratch != nil {
			scratch.raw = buf
		}
	}
	if err := r.readAt(buf, handle.Offset); err != nil {
		return nil, err
	}
	contents, trailer := buf[:handle.Length], buf[handle.Length:]
	if base.CRCUpdate(base.CRC(contents), trailer[:1]) != binary.LittleEndian.Uint32(trailer[1:]) {
		return nil, r.corrupt("block checksum mismatch at offset %d", handle.Offset)
	}
	var dst []byte
	if scratch != nil {
		dst = scratch.decoded
	}
	block, err := decompressBlock(Compression(trailer[0]), contents, dst)
	if err != nil {
		return nil, r.wrap(err)
	}
	if scratch != nil && Compression(trailer[0]) != NoCompression {
		scratch.decoded = block
	}
	return block, nil
}

type blockScratch struct {
	raw, decoded []byte
}

// release drops large buffers so a pooled Iter does not pin them.
func (s *blockScratch) release() {
	const maxRetained = 1 << 20
	if cap(s.raw) > maxRetained {
		s.raw = nil
	}
	if cap(s.decoded) > maxRetained {
		s.decoded = nil
	}
}

func (r *Reader) readDataBlock(handle BlockHandle, fillCache bool, scratch *blockScratch) ([]byte, error) {
	cacheKey := cache.Key{FileNum: r.fileNum, Offset: handle.Offset}
	if block, ok := r.blockCache.Get(cacheKey); ok {
		r.stats.BlockCacheHits.Add(1)
		return block, nil
	}
	var block []byte
	var err error
	if fillCache || scratch == nil {
		block, err = r.readBlock(handle)
	} else {
		block, err = r.readBlockInto(handle, scratch)
	}
	if err != nil {
		return nil, err
	}
	r.stats.BlockReads.Add(1)
	r.stats.BlockBytesRead.Add(int64(handle.Length))
	if fillCache {
		r.blockCache.Set(cacheKey, block)
	}
	return block, nil
}

func (r *Reader) MayContain(userKey []byte) bool {
	if r.filter == nil {
		return true
	}
	r.stats.FilterChecks.Add(1)
	if r.filter.MayContain(userKey) {
		return true
	}
	r.stats.FilterNegatives.Add(1)
	return false
}

func (r *Reader) Properties() Properties { return r.props }

func (r *Reader) Size() uint64 { return r.size }

func (r *Reader) Close() error { return r.file.Close() }

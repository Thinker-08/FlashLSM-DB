package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/bloom"
	"github.com/Thinker-08/FlashLSM-DB/vfs"
)

type WriterOptions struct {
	Comparer             *base.Comparer
	BlockSize            int
	BlockRestartInterval int
	BloomBitsPerKey      int
	Compression          Compression
}

func (o WriterOptions) withDefaults() WriterOptions {
	if o.Comparer == nil {
		o.Comparer = base.DefaultComparer
	}
	if o.BlockSize <= 0 {
		o.BlockSize = 4 << 10
	}
	if o.BlockRestartInterval <= 0 {
		o.BlockRestartInterval = 16
	}
	return o
}

type Metadata struct {
	Size        uint64
	Smallest    base.InternalKey
	Largest     base.InternalKey
	SmallestSeq uint64
	LargestSeq  uint64
	Properties  Properties
}

type Writer struct {
	file     vfs.File
	buffered *bufio.Writer
	opts     WriterOptions
	compare  base.Compare

	offset     uint64
	dataBlock  blockWriter
	indexBlock blockWriter
	filter     *bloom.Builder

	lastKey           []byte
	keyBuf            []byte
	pendingIndexEntry bool
	pendingHandle     BlockHandle
	compressBuf       []byte
	handleBuf         []byte

	props    Properties
	smallest base.InternalKey
	err      error
	closed   bool
}

func NewWriter(file vfs.File, opts WriterOptions) *Writer {
	opts = opts.withDefaults()
	w := &Writer{
		file:       file,
		buffered:   bufio.NewWriterSize(file, 64<<10),
		opts:       opts,
		compare:    opts.Comparer.Compare,
		dataBlock:  blockWriter{restartInterval: opts.BlockRestartInterval},
		indexBlock: blockWriter{restartInterval: 1},
	}
	if opts.BloomBitsPerKey > 0 {
		w.filter = bloom.NewBuilder(opts.BloomBitsPerKey)
	}
	w.props.ComparerName = opts.Comparer.Name
	w.props.SmallestSeq = base.SeqNumMax
	return w
}

func (w *Writer) Add(key base.InternalKey, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return fmt.Errorf("sstable: add after close")
	}
	isFirst := w.props.NumEntries == 0
	var prevKey base.InternalKey
	if !isFirst {
		prevKey, _ = base.DecodeInternalKey(w.lastKey)
		if base.InternalCompare(w.compare, prevKey, key) >= 0 {
			w.err = fmt.Errorf("sstable: keys out of order: %s added after %s", key, prevKey)
			return w.err
		}
	}
	if w.pendingIndexEntry {
		w.addIndexEntry(prevKey, &key)
	}
	if w.filter != nil && (isFirst || w.compare(prevKey.UserKey, key.UserKey) != 0) {
		w.filter.Add(key.UserKey)
	}

	w.keyBuf = key.Append(w.keyBuf[:0])
	w.dataBlock.add(w.keyBuf, value)
	w.lastKey = append(w.lastKey[:0], w.keyBuf...)
	if isFirst {
		w.smallest = key.Clone()
	}

	w.props.NumEntries++
	switch key.Kind() {
	case base.KindDelete:
		w.props.NumDeletions++
	case base.KindMerge:
		w.props.NumMerges++
	}
	seq := key.SeqNum()
	w.props.SmallestSeq = min(w.props.SmallestSeq, seq)
	w.props.LargestSeq = max(w.props.LargestSeq, seq)
	w.props.RawKeyBytes += uint64(key.Size())
	w.props.RawValueBytes += uint64(len(value))

	if w.dataBlock.estimatedSize() >= w.opts.BlockSize {
		return w.flushDataBlock()
	}
	return nil
}

// addIndexEntry's separator is >= prevKey and, if nextKey is non-nil, < nextKey.
func (w *Writer) addIndexEntry(prevKey base.InternalKey, nextKey *base.InternalKey) {
	separator := prevKey
	var shortened []byte
	comparer := w.opts.Comparer
	if nextKey != nil && comparer.Separator != nil {
		shortened = comparer.Separator(nil, prevKey.UserKey, nextKey.UserKey)
	} else if nextKey == nil && comparer.Successor != nil {
		shortened = comparer.Successor(nil, prevKey.UserKey)
	}
	if shortened != nil && len(shortened) < len(prevKey.UserKey) && w.compare(prevKey.UserKey, shortened) < 0 {
		separator = base.MakeSearchKey(shortened, base.SeqNumMax)
	}
	w.handleBuf = w.pendingHandle.appendVarint(w.handleBuf[:0])
	w.indexBlock.add(separator.Append(nil), w.handleBuf)
	w.pendingIndexEntry = false
}

func (w *Writer) flushDataBlock() error {
	if w.dataBlock.empty() {
		return nil
	}
	handle, err := w.writeBlock(w.dataBlock.finish(), w.opts.Compression)
	if err != nil {
		return err
	}
	w.dataBlock.reset()
	w.pendingHandle = handle
	w.pendingIndexEntry = true
	w.props.NumDataBlocks++
	w.props.DataSize = w.offset
	return nil
}

func (w *Writer) writeBlock(raw []byte, compression Compression) (BlockHandle, error) {
	payload, actualCompression := compressBlock(compression, w.compressBuf, raw)
	if actualCompression != NoCompression {
		w.compressBuf = payload[:0]
	}
	var trailer [blockTrailerLen]byte
	trailer[0] = byte(actualCompression)
	crc := base.CRCUpdate(base.CRC(payload), trailer[:1])
	binary.LittleEndian.PutUint32(trailer[1:], crc)
	handle := BlockHandle{Offset: w.offset, Length: uint64(len(payload))}
	if _, err := w.buffered.Write(payload); err != nil {
		w.err = err
		return handle, err
	}
	if _, err := w.buffered.Write(trailer[:]); err != nil {
		w.err = err
		return handle, err
	}
	w.offset += uint64(len(payload)) + blockTrailerLen
	return handle, nil
}

func (w *Writer) EstimatedSize() uint64 {
	return w.offset + uint64(w.dataBlock.estimatedSize())
}

func (w *Writer) NumEntries() uint64 { return w.props.NumEntries }

func (w *Writer) Close() error {
	if w.closed {
		return fmt.Errorf("sstable: close after close")
	}
	w.closed = true
	err := w.finish()
	if err == nil {
		err = w.file.Sync()
	}
	if closeErr := w.file.Close(); err == nil {
		err = closeErr
	}
	if err != nil && w.err == nil {
		w.err = err
	}
	return err
}

func (w *Writer) Abort() {
	if !w.closed {
		w.closed = true
		w.file.Close()
	}
}

func (w *Writer) finish() error {
	if w.err != nil {
		return w.err
	}
	if err := w.flushDataBlock(); err != nil {
		return err
	}
	if w.pendingIndexEntry {
		lastKey, _ := base.DecodeInternalKey(w.lastKey)
		w.addIndexEntry(lastKey, nil)
	}
	var tableFooter footer
	tableFooter.version = formatVersion
	if w.filter != nil {
		w.props.FilterPolicy = bloom.HashName
		handle, err := w.writeBlock(w.filter.Finish(nil), NoCompression)
		if err != nil {
			return err
		}
		tableFooter.filter = handle
		w.props.FilterSize = handle.Length
	}
	indexContents := w.indexBlock.finish()
	w.props.IndexSize = uint64(len(indexContents))
	w.props.Compression = w.opts.Compression.String()
	if w.props.NumEntries == 0 {
		w.props.SmallestSeq = 0
	}
	handle, err := w.writeBlock(w.props.encode(), NoCompression)
	if err != nil {
		return err
	}
	tableFooter.properties = handle
	if tableFooter.index, err = w.writeBlock(indexContents, NoCompression); err != nil {
		return err
	}
	if _, err := w.buffered.Write(tableFooter.encode()); err != nil {
		w.err = err
		return err
	}
	w.offset += FooterLen
	if err := w.buffered.Flush(); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *Writer) Metadata() Metadata {
	largest, _ := base.DecodeInternalKey(w.lastKey)
	return Metadata{
		Size:        w.offset,
		Smallest:    w.smallest,
		Largest:     largest.Clone(),
		SmallestSeq: w.props.SmallestSeq,
		LargestSeq:  w.props.LargestSeq,
		Properties:  w.props,
	}
}

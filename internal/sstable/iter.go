package sstable

import (
	"sync"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

type Iter struct {
	reader          *Reader
	fillCache       bool
	indexIter       blockIter
	dataIter        blockIter
	dataBlockOffset uint64
	dataLoaded      bool
	err             error
	scratch         blockScratch
}

var iterPool = sync.Pool{New: func() any { return new(Iter) }}

var _ base.InternalIterator = (*Iter)(nil)

func (r *Reader) NewIter(fillCache bool) *Iter {
	it := iterPool.Get().(*Iter)
	indexKeyBuf, dataKeyBuf, scratch := it.indexIter.key, it.dataIter.key, it.scratch
	*it = Iter{reader: r, fillCache: fillCache, scratch: scratch}
	it.indexIter.key, it.dataIter.key = indexKeyBuf[:0], dataKeyBuf[:0]
	if err := it.indexIter.init(r.compare, r.indexBlock, false); err != nil {
		it.err = r.wrap(err)
	}
	return it
}

func (it *Iter) loadDataBlock() bool {
	it.dataIter.valid = false
	if !it.indexIter.Valid() {
		if err := it.indexIter.Error(); err != nil {
			it.err = it.reader.wrap(err)
		}
		return false
	}
	handle, ok := decodeHandleVarint(it.indexIter.Value())
	if !ok {
		it.err = it.reader.corrupt("bad block handle in index")
		return false
	}
	if it.dataLoaded && handle.Offset == it.dataBlockOffset {
		return true
	}
	block, err := it.reader.readDataBlock(handle, it.fillCache, &it.scratch)
	if err != nil {
		it.err = err
		it.dataLoaded = false
		return false
	}
	if err := it.dataIter.init(it.reader.compare, block, false); err != nil {
		it.err = it.reader.wrap(err)
		it.dataLoaded = false
		return false
	}
	it.dataBlockOffset = handle.Offset
	it.dataLoaded = true
	return true
}

func (it *Iter) skipForward() {
	for !it.dataIter.Valid() {
		if err := it.dataIter.Error(); err != nil {
			it.err = it.reader.wrap(err)
			return
		}
		it.indexIter.Next()
		if !it.loadDataBlock() {
			return
		}
		it.dataIter.First()
	}
}

func (it *Iter) skipBackward() {
	for !it.dataIter.Valid() {
		if err := it.dataIter.Error(); err != nil {
			it.err = it.reader.wrap(err)
			return
		}
		it.indexIter.Prev()
		if !it.loadDataBlock() {
			return
		}
		it.dataIter.Last()
	}
}

func (it *Iter) SeekGE(key base.InternalKey) {
	if it.err != nil {
		return
	}
	it.indexIter.SeekGE(key)
	if !it.loadDataBlock() {
		return
	}
	it.dataIter.SeekGE(key)
	it.skipForward()
}

func (it *Iter) SeekLT(key base.InternalKey) {
	if it.err != nil {
		return
	}
	// The first block with separator >= key is the only one that can straddle key.
	it.indexIter.SeekGE(key)
	if !it.indexIter.Valid() {
		if it.indexIter.Error() != nil {
			it.loadDataBlock()
			return
		}
		it.indexIter.Last()
	}
	if !it.loadDataBlock() {
		return
	}
	it.dataIter.SeekLT(key)
	it.skipBackward()
}

func (it *Iter) First() {
	if it.err != nil {
		return
	}
	it.indexIter.First()
	if !it.loadDataBlock() {
		return
	}
	it.dataIter.First()
	it.skipForward()
}

func (it *Iter) Last() {
	if it.err != nil {
		return
	}
	it.indexIter.Last()
	if !it.loadDataBlock() {
		return
	}
	it.dataIter.Last()
	it.skipBackward()
}

func (it *Iter) Next() {
	if it.err != nil || !it.dataIter.Valid() {
		return
	}
	it.dataIter.Next()
	it.skipForward()
}

func (it *Iter) Prev() {
	if it.err != nil || !it.dataIter.Valid() {
		return
	}
	it.dataIter.Prev()
	it.skipBackward()
}

func (it *Iter) Valid() bool           { return it.err == nil && it.dataIter.Valid() }
func (it *Iter) Key() base.InternalKey { return it.dataIter.Key() }
func (it *Iter) Value() []byte         { return it.dataIter.Value() }
func (it *Iter) Error() error          { return it.err }

// Close returns the iterator to a pool, so it must not be used afterwards.
func (it *Iter) Close() error {
	if it.reader == nil {
		return nil
	}
	err := it.err
	indexKeyBuf, dataKeyBuf, scratch := it.indexIter.key, it.dataIter.key, it.scratch
	scratch.release()
	*it = Iter{scratch: scratch}
	it.indexIter.key, it.dataIter.key = indexKeyBuf[:0], dataKeyBuf[:0]
	iterPool.Put(it)
	return err
}

package lsmkv

import (
	"fmt"
	"iter"
	"slices"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/invariants"
	"github.com/Thinker-08/FlashLSM-DB/internal/iterator"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
)

// Iterator sees the database as of its creation and pins its files until Close, so a leaked
// iterator leaks disk space. It belongs to one goroutine at a time.
type Iterator struct {
	db           *DB
	state        *readState
	internalIter base.InternalIterator
	seq          uint64
	lower        []byte
	upper        []byte
	now          int64

	valid   bool
	forward bool
	err     error
	closed  bool

	key, value []byte
	keyBuf     []byte
	valueBuf   []byte
	operands   [][]byte
}

func (db *DB) NewIterator(readOpts *ReadOptions) *Iterator {
	it := &Iterator{db: db}
	if err := db.checkOpen(); err != nil {
		it.err, it.closed = err, true
		return it
	}
	seq, err := db.readSeq(readOpts)
	if err != nil {
		it.err, it.closed = err, true
		return it
	}
	it.seq = seq
	it.state = db.loadReadState()
	it.now = db.now().UnixNano()
	fillCache := true
	if readOpts != nil {
		it.lower, it.upper = slices.Clone(readOpts.LowerBound), slices.Clone(readOpts.UpperBound)
		fillCache = !readOpts.DontFillCache
	}
	it.internalIter, it.err = db.newInternalIter(it.state, it.lower, it.upper, fillCache)
	db.openIterators.Add(1)
	return it
}

func (db *DB) newInternalIter(state *readState, lower, upper []byte, fillCache bool) (base.InternalIterator, error) {
	iters := []base.InternalIterator{state.activeMemtable.NewIter()}
	for i := len(state.immutableMemtables) - 1; i >= 0; i-- {
		iters = append(iters, state.immutableMemtables[i].NewIter())
	}
	outsideBounds := func(file *manifest.FileMetadata) bool {
		return (upper != nil && db.compare(file.Smallest.UserKey, upper) >= 0) ||
			(lower != nil && db.compare(file.Largest.UserKey, lower) < 0)
	}
	for _, file := range state.version.Levels[0] {
		if outsideBounds(file) {
			continue
		}
		it, err := db.tableCache.newIter(file, fillCache)
		if err != nil {
			for _, it := range iters {
				it.Close()
			}
			return nil, err
		}
		iters = append(iters, it)
	}
	openTable := func(file *manifest.FileMetadata) (base.InternalIterator, error) {
		return db.tableCache.newIter(file, fillCache)
	}
	for level := 1; level < len(state.version.Levels); level++ {
		if files := state.version.Levels[level]; len(files) > 0 {
			iters = append(iters, iterator.NewLevel(db.compare, files, openTable, lower, upper))
		}
	}
	return iterator.NewMerging(db.compare, iters...), nil
}

func (it *Iterator) usable() bool {
	it.valid = false
	return !it.closed && it.err == nil && it.internalIter != nil
}

func (it *Iterator) First() bool {
	if !it.usable() {
		return false
	}
	if it.lower != nil {
		return it.SeekGE(it.lower)
	}
	it.forward = true
	it.internalIter.First()
	it.findNext(false)
	return it.valid
}

func (it *Iterator) Last() bool {
	if !it.usable() {
		return false
	}
	if it.upper != nil {
		return it.SeekLT(it.upper)
	}
	it.forward = false
	it.internalIter.Last()
	it.findPrev()
	return it.valid
}

func (it *Iterator) SeekGE(key []byte) bool {
	if !it.usable() {
		return false
	}
	if it.lower != nil && it.db.compare(key, it.lower) < 0 {
		key = it.lower
	}
	it.forward = true
	it.internalIter.SeekGE(base.MakeSearchKey(key, it.seq))
	it.findNext(false)
	return it.valid
}

func (it *Iterator) SeekLT(key []byte) bool {
	if !it.usable() {
		return false
	}
	if it.upper != nil && it.db.compare(key, it.upper) > 0 {
		key = it.upper
	}
	it.forward = false
	it.internalIter.SeekLT(base.MakeSearchKey(key, base.SeqNumMax))
	it.findPrev()
	return it.valid
}

func (it *Iterator) Next() bool {
	if !it.valid || it.closed || it.err != nil {
		it.valid = false
		return false
	}
	var prevKey []byte
	if invariants.Enabled {
		prevKey = slices.Clone(it.key)
	}
	it.keyBuf = append(it.keyBuf[:0], it.key...)
	it.valid = false
	if !it.forward {
		it.forward = true
		it.internalIter.SeekGE(base.InternalKey{UserKey: it.keyBuf, Trailer: 0})
	}
	it.findNext(true)
	if invariants.Enabled && it.valid && it.db.compare(it.key, prevKey) <= 0 {
		panic(fmt.Sprintf("lsmkv: Next moved from %q to %q", prevKey, it.key))
	}
	return it.valid
}

func (it *Iterator) Prev() bool {
	if !it.valid || it.closed || it.err != nil {
		it.valid = false
		return false
	}
	var prevKey []byte
	if invariants.Enabled {
		prevKey = slices.Clone(it.key)
	}
	it.valid = false
	if it.forward {
		it.forward = false
		it.keyBuf = append(it.keyBuf[:0], it.key...)
		it.internalIter.SeekLT(base.MakeSearchKey(it.keyBuf, base.SeqNumMax))
	}
	it.findPrev()
	if invariants.Enabled && it.valid && it.db.compare(it.key, prevKey) >= 0 {
		panic(fmt.Sprintf("lsmkv: Prev moved from %q to %q", prevKey, it.key))
	}
	return it.valid
}

// findNext takes a skipping flag, not a nil keyBuf, because the empty key is a real key.
func (it *Iterator) findNext(skipping bool) {
	compare := it.db.compare
	for it.internalIter.Valid() {
		key := it.internalIter.Key()
		if it.upper != nil && compare(key.UserKey, it.upper) >= 0 {
			break
		}
		if key.SeqNum() > it.seq || (skipping && compare(key.UserKey, it.keyBuf) <= 0) {
			it.internalIter.Next()
			continue
		}
		switch key.Kind() {
		case base.KindSet:
			it.key, it.value, it.valid = key.UserKey, it.internalIter.Value(), true
			return
		case base.KindSetTTL:
			value, live, err := decodeTTL(it.internalIter.Value(), it.now)
			if err != nil {
				it.err = err
				return
			}
			if live {
				it.key, it.value, it.valid = key.UserKey, value, true
				return
			}
		case base.KindDelete:
		case base.KindMerge:
			it.mergeForward()
			return
		default:
			it.err = base.CorruptionErrorf("entry %s has an unknown kind", key)
			return
		}
		it.keyBuf = append(it.keyBuf[:0], key.UserKey...)
		skipping = true
		it.internalIter.Next()
	}
	if err := it.internalIter.Error(); err != nil {
		it.err = err
	}
}

func (it *Iterator) mergeForward() {
	compare := it.db.compare
	it.keyBuf = append(it.keyBuf[:0], it.internalIter.Key().UserKey...)
	it.operands = it.operands[:0]
	var baseValue []byte
	exists := false
scan:
	for ; it.internalIter.Valid() && compare(it.internalIter.Key().UserKey, it.keyBuf) == 0; it.internalIter.Next() {
		key := it.internalIter.Key()
		switch key.Kind() {
		case base.KindMerge:
			it.operands = append(it.operands, slices.Clone(it.internalIter.Value()))
		case base.KindSet:
			baseValue, exists = slices.Clone(it.internalIter.Value()), true
			break scan
		case base.KindSetTTL:
			value, live, err := decodeTTL(it.internalIter.Value(), it.now)
			if err != nil {
				it.err = err
				return
			}
			baseValue, exists = slices.Clone(value), live
			break scan
		case base.KindDelete:
			break scan
		default:
			it.err = base.CorruptionErrorf("entry %s has an unknown kind", key)
			return
		}
	}
	if err := it.internalIter.Error(); err != nil {
		it.err = err
		return
	}
	it.resolveMerge(baseValue, exists)
}

func (it *Iterator) findPrev() {
	compare := it.db.compare
	for it.internalIter.Valid() {
		key := it.internalIter.Key()
		if it.lower != nil && compare(key.UserKey, it.lower) < 0 {
			break
		}
		it.keyBuf = append(it.keyBuf[:0], key.UserKey...)
		it.operands = it.operands[:0]
		var baseValue []byte
		exists, found := false, false
		for ; it.internalIter.Valid() && compare(it.internalIter.Key().UserKey, it.keyBuf) == 0; it.internalIter.Prev() {
			entry := it.internalIter.Key()
			if entry.SeqNum() > it.seq {
				continue
			}
			found = true
			switch entry.Kind() {
			case base.KindSet:
				it.valueBuf = append(it.valueBuf[:0], it.internalIter.Value()...)
				baseValue, exists = it.valueBuf, true
				it.operands = it.operands[:0]
			case base.KindSetTTL:
				value, live, err := decodeTTL(it.internalIter.Value(), it.now)
				if err != nil {
					it.err = err
					return
				}
				it.valueBuf = append(it.valueBuf[:0], value...)
				baseValue, exists = it.valueBuf, live
				it.operands = it.operands[:0]
			case base.KindDelete:
				baseValue, exists = nil, false
				it.operands = it.operands[:0]
			case base.KindMerge:
				it.operands = append(it.operands, slices.Clone(it.internalIter.Value()))
			default:
				it.err = base.CorruptionErrorf("entry %s has an unknown kind", entry)
				return
			}
		}
		if err := it.internalIter.Error(); err != nil {
			it.err = err
			return
		}
		if !found {
			continue
		}
		if len(it.operands) > 0 {
			slices.Reverse(it.operands)
			it.resolveMerge(slices.Clone(baseValue), exists)
			return
		}
		if exists {
			it.key, it.value, it.valid = it.keyBuf, baseValue, true
			return
		}
	}
	if err := it.internalIter.Error(); err != nil {
		it.err = err
	}
}

// resolveMerge expects it.operands newest first.
func (it *Iterator) resolveMerge(baseValue []byte, exists bool) {
	merged, err := it.db.fullMerge(it.keyBuf, baseValue, exists, it.operands, true)
	if err != nil {
		it.err = err
		return
	}
	it.valueBuf = append(it.valueBuf[:0], merged...)
	it.key, it.value, it.valid = it.keyBuf, it.valueBuf, true
}

func (it *Iterator) Valid() bool { return it.valid }

// Key returns the current key, borrowed until the next positioning call.
func (it *Iterator) Key() []byte { return it.key }

// Value returns the current value, borrowed until the next positioning call.
func (it *Iterator) Value() []byte { return it.value }

func (it *Iterator) Error() error { return it.err }

// All iterates from First; afterwards check Error, and Close the iterator yourself.
func (it *Iterator) All() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		for ok := it.First(); ok; ok = it.Next() {
			if !yield(it.Key(), it.Value()) {
				return
			}
		}
	}
}

// Close returns the iterator's error, if any; closing twice is harmless.
func (it *Iterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed, it.valid = true, false
	if it.internalIter != nil {
		if err := it.internalIter.Close(); err != nil && it.err == nil {
			it.err = err
		}
	}
	it.state.unref()
	it.db.openIterators.Add(-1)
	return it.err
}

package memtable

import (
	"fmt"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

type Memtable struct {
	list   *skiplist
	logNum uint64
}

func New(compare base.Compare, logNum uint64) *Memtable {
	return &Memtable{list: newSkiplist(compare), logNum: logNum}
}

// Add copies key and value. Only one goroutine may call Add at a time.
func (mt *Memtable) Add(seq uint64, kind base.Kind, key, value []byte) error {
	if !mt.list.add(base.MakeInternalKey(key, seq, kind), value) {
		return fmt.Errorf("memtable: duplicate internal key %q#%d,%s", key, seq, kind)
	}
	return nil
}

func (mt *Memtable) Get(key []byte, seq uint64) (value []byte, kind base.Kind, found bool) {
	entry := mt.list.findGreaterOrEqual(base.MakeSearchKey(key, seq), nil)
	if entry == nil || mt.list.compare(entry.userKey(), key) != 0 {
		return nil, 0, false
	}
	return entry.value(), base.Kind(entry.trailer), true
}

func (mt *Memtable) LogNum() uint64 { return mt.logNum }

func (mt *Memtable) ApproximateSize() int64 { return mt.list.size.Load() }

func (mt *Memtable) Len() int64 { return mt.list.count.Load() }

func (mt *Memtable) Empty() bool { return mt.list.count.Load() == 0 }

func (mt *Memtable) NewIter() *Iter {
	return &Iter{list: mt.list}
}

func (mt *Memtable) InitIter(it *Iter) {
	*it = Iter{list: mt.list}
}

type Iter struct {
	list    *skiplist
	current *node
}

var _ base.InternalIterator = (*Iter)(nil)

func (it *Iter) SeekGE(key base.InternalKey) { it.current = it.list.findGreaterOrEqual(key, nil) }

func (it *Iter) SeekLT(key base.InternalKey) {
	it.current = it.list.findLessThan(key)
	if it.current == it.list.head {
		it.current = nil
	}
}

func (it *Iter) First() { it.current = it.list.head.next[0].Load() }

func (it *Iter) Last() {
	it.current = it.list.findLast()
	if it.current == it.list.head {
		it.current = nil
	}
}

func (it *Iter) Next() {
	if it.current != nil {
		it.current = it.current.next[0].Load()
	}
}

func (it *Iter) Prev() {
	if it.current == nil {
		return
	}
	it.SeekLT(it.Key())
}

func (it *Iter) Valid() bool { return it.current != nil }

func (it *Iter) Key() base.InternalKey {
	return base.InternalKey{UserKey: it.current.userKey(), Trailer: it.current.trailer}
}

func (it *Iter) Value() []byte { return it.current.value() }
func (it *Iter) Error() error  { return nil }
func (it *Iter) Close() error  { it.current = nil; return nil }

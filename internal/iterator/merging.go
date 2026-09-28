package iterator

import (
	"errors"
	"fmt"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/invariants"
)

// Merging assumes internal keys are unique across children, so its heap never breaks ties.
type Merging struct {
	compare  base.Compare
	children []base.InternalIterator
	heap     []int
	forward  bool
	err      error
	savedKey []byte
}

var _ base.InternalIterator = (*Merging)(nil)

func NewMerging(compare base.Compare, children ...base.InternalIterator) *Merging {
	return &Merging{compare: compare, children: children, forward: true}
}

func (mi *Merging) less(a, b int) bool {
	order := base.InternalCompare(mi.compare, mi.children[a].Key(), mi.children[b].Key())
	if mi.forward {
		return order < 0
	}
	return order > 0
}

func (mi *Merging) siftDown(i int) {
	size := len(mi.heap)
	for {
		left := 2*i + 1
		if left >= size {
			return
		}
		smaller := left
		if right := left + 1; right < size && mi.less(mi.heap[right], mi.heap[left]) {
			smaller = right
		}
		if !mi.less(mi.heap[smaller], mi.heap[i]) {
			return
		}
		mi.heap[i], mi.heap[smaller] = mi.heap[smaller], mi.heap[i]
		i = smaller
	}
}

func (mi *Merging) childValid(child base.InternalIterator) bool {
	if child.Valid() {
		return true
	}
	if err := child.Error(); err != nil && mi.err == nil {
		mi.err = err
	}
	return false
}

func (mi *Merging) rebuildHeap() {
	mi.heap = mi.heap[:0]
	for i, child := range mi.children {
		if mi.childValid(child) {
			mi.heap = append(mi.heap, i)
		}
	}
	for i := len(mi.heap)/2 - 1; i >= 0; i-- {
		mi.siftDown(i)
	}
}

func (mi *Merging) fixHeapTop() {
	if mi.childValid(mi.children[mi.heap[0]]) {
		mi.siftDown(0)
		return
	}
	last := len(mi.heap) - 1
	mi.heap[0] = mi.heap[last]
	mi.heap = mi.heap[:last]
	if len(mi.heap) > 0 {
		mi.siftDown(0)
	}
}

func (mi *Merging) resetDirection(forward bool) {
	mi.err = nil
	mi.forward = forward
}

func (mi *Merging) SeekGE(key base.InternalKey) {
	mi.resetDirection(true)
	for _, child := range mi.children {
		child.SeekGE(key)
	}
	mi.rebuildHeap()
}

func (mi *Merging) SeekLT(key base.InternalKey) {
	mi.resetDirection(false)
	for _, child := range mi.children {
		child.SeekLT(key)
	}
	mi.rebuildHeap()
}

func (mi *Merging) First() {
	mi.resetDirection(true)
	for _, child := range mi.children {
		child.First()
	}
	mi.rebuildHeap()
}

func (mi *Merging) Last() {
	mi.resetDirection(false)
	for _, child := range mi.children {
		child.Last()
	}
	mi.rebuildHeap()
}

func (mi *Merging) Next() {
	if !mi.Valid() {
		return
	}
	// After moving backward, the other children sit before the current key; re-seek them past it.
	if !mi.forward {
		top := mi.heap[0]
		key := mi.saveTopKey()
		for i, child := range mi.children {
			if i == top {
				continue
			}
			child.SeekGE(key)
			if child.Valid() && base.InternalCompare(mi.compare, child.Key(), key) == 0 {
				child.Next()
			}
		}
		mi.forward = true
		mi.children[top].Next()
		mi.rebuildHeap()
		return
	}
	var prevKey base.InternalKey
	if invariants.Enabled {
		prevKey = mi.Key().Clone()
	}
	mi.children[mi.heap[0]].Next()
	mi.fixHeapTop()
	if invariants.Enabled && mi.Valid() && base.InternalCompare(mi.compare, mi.Key(), prevKey) <= 0 {
		panic(fmt.Sprintf("iterator: Next moved from %s to %s", prevKey, mi.Key()))
	}
}

func (mi *Merging) Prev() {
	if !mi.Valid() {
		return
	}
	// After moving forward, the other children sit after the current key; re-seek them before it.
	if mi.forward {
		top := mi.heap[0]
		key := mi.saveTopKey()
		for i, child := range mi.children {
			if i != top {
				child.SeekLT(key)
			}
		}
		mi.forward = false
		mi.children[top].Prev()
		mi.rebuildHeap()
		return
	}
	var prevKey base.InternalKey
	if invariants.Enabled {
		prevKey = mi.Key().Clone()
	}
	mi.children[mi.heap[0]].Prev()
	mi.fixHeapTop()
	if invariants.Enabled && mi.Valid() && base.InternalCompare(mi.compare, mi.Key(), prevKey) >= 0 {
		panic(fmt.Sprintf("iterator: Prev moved from %s to %s", prevKey, mi.Key()))
	}
}

func (mi *Merging) saveTopKey() base.InternalKey {
	key := mi.children[mi.heap[0]].Key()
	mi.savedKey = append(mi.savedKey[:0], key.UserKey...)
	return base.InternalKey{UserKey: mi.savedKey, Trailer: key.Trailer}
}

func (mi *Merging) Valid() bool { return mi.err == nil && len(mi.heap) > 0 }

func (mi *Merging) Key() base.InternalKey { return mi.children[mi.heap[0]].Key() }

func (mi *Merging) Value() []byte { return mi.children[mi.heap[0]].Value() }

func (mi *Merging) Error() error { return mi.err }

func (mi *Merging) Close() error {
	var errs []error
	for _, child := range mi.children {
		if err := child.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	mi.children, mi.heap = nil, nil
	return errors.Join(errs...)
}

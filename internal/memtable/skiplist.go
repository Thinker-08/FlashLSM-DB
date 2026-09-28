package memtable

import (
	"sync/atomic"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

const (
	maxHeight       = 12
	branchingFactor = 4
)

const (
	nodeSlabLen       = 256
	pointerSlabLen    = 1024
	byteSlabSize      = 64 << 10
	largeEntrySize    = byteSlabSize / 4
	nodeOverheadBytes = 64
)

type node struct {
	data    []byte
	keyLen  uint32
	trailer uint64
	next    []atomic.Pointer[node]
}

func (n *node) userKey() []byte { return n.data[:n.keyLen:n.keyLen] }
func (n *node) value() []byte   { return n.data[n.keyLen:len(n.data):len(n.data)] }

type skiplist struct {
	compare     base.Compare
	head        *node
	height      atomic.Int32 // a stale read is harmless
	randomState uint64
	size        atomic.Int64
	count       atomic.Int64

	nodeSlab    []node
	pointerSlab []atomic.Pointer[node]
	byteSlab    []byte
}

func newSkiplist(compare base.Compare) *skiplist {
	sl := &skiplist{compare: compare, randomState: 0x2545f4914f6cdd1d}
	sl.head = &node{next: make([]atomic.Pointer[node], maxHeight)}
	sl.height.Store(1)
	return sl
}

func (sl *skiplist) compareNode(n *node, key base.InternalKey) int {
	if order := sl.compare(n.userKey(), key.UserKey); order != 0 {
		return order
	}
	switch {
	case n.trailer > key.Trailer:
		return -1
	case n.trailer < key.Trailer:
		return 1
	}
	return 0
}

func (sl *skiplist) randomHeight() int {
	height := 1
	for height < maxHeight {
		sl.randomState ^= sl.randomState >> 12
		sl.randomState ^= sl.randomState << 25
		sl.randomState ^= sl.randomState >> 27
		if ((sl.randomState*0x2545f4914f6cdd1d)>>32)%branchingFactor != 0 {
			break
		}
		height++
	}
	return height
}

func (sl *skiplist) allocNode(height, dataLen int) *node {
	if len(sl.nodeSlab) == 0 {
		sl.nodeSlab = make([]node, nodeSlabLen)
	}
	newNode := &sl.nodeSlab[0]
	sl.nodeSlab = sl.nodeSlab[1:]
	if len(sl.pointerSlab) < height {
		sl.pointerSlab = make([]atomic.Pointer[node], pointerSlabLen)
	}
	newNode.next = sl.pointerSlab[:height:height]
	sl.pointerSlab = sl.pointerSlab[height:]
	if dataLen > largeEntrySize {
		newNode.data = make([]byte, dataLen)
	} else {
		if len(sl.byteSlab) < dataLen {
			sl.byteSlab = make([]byte, byteSlabSize)
		}
		newNode.data = sl.byteSlab[:dataLen:dataLen]
		sl.byteSlab = sl.byteSlab[dataLen:]
	}
	return newNode
}

func (sl *skiplist) findGreaterOrEqual(key base.InternalKey, prev *[maxHeight]*node) *node {
	current := sl.head
	level := int(sl.height.Load()) - 1
	for {
		next := current.next[level].Load()
		if next != nil && sl.compareNode(next, key) < 0 {
			current = next
			continue
		}
		if prev != nil {
			prev[level] = current
		}
		if level == 0 {
			return next
		}
		level--
	}
}

func (sl *skiplist) findLessThan(key base.InternalKey) *node {
	current := sl.head
	level := int(sl.height.Load()) - 1
	for {
		next := current.next[level].Load()
		if next != nil && sl.compareNode(next, key) < 0 {
			current = next
			continue
		}
		if level == 0 {
			return current
		}
		level--
	}
}

func (sl *skiplist) findLast() *node {
	current := sl.head
	level := int(sl.height.Load()) - 1
	for {
		next := current.next[level].Load()
		if next != nil {
			current = next
			continue
		}
		if level == 0 {
			return current
		}
		level--
	}
}

func (sl *skiplist) add(key base.InternalKey, value []byte) bool {
	var prev [maxHeight]*node
	if existing := sl.findGreaterOrEqual(key, &prev); existing != nil && sl.compareNode(existing, key) == 0 {
		return false
	}
	height := sl.randomHeight()
	if currentHeight := int(sl.height.Load()); height > currentHeight {
		for i := currentHeight; i < height; i++ {
			prev[i] = sl.head
		}
		sl.height.Store(int32(height))
	}
	newNode := sl.allocNode(height, len(key.UserKey)+len(value))
	copy(newNode.data, key.UserKey)
	copy(newNode.data[len(key.UserKey):], value)
	newNode.keyLen = uint32(len(key.UserKey))
	newNode.trailer = key.Trailer
	// Link n bottom-up only once it is complete, so lock-free readers never see it half-built.
	for i := 0; i < height; i++ {
		newNode.next[i].Store(prev[i].next[i].Load())
		prev[i].next[i].Store(newNode)
	}
	sl.size.Add(int64(len(key.UserKey) + base.TrailerLen + len(value) + nodeOverheadBytes + 8*height))
	sl.count.Add(1)
	return true
}

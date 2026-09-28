package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// File numbers are never reused, so blocks of deleted files need no invalidation.
type Key struct {
	FileNum uint64
	Offset  uint64
}

const (
	maxShards          = 16
	minShardBytes      = 256 << 10
	entryOverheadBytes = 96
)

// A nil *Cache is valid and caches nothing.
type Cache struct {
	shards    []cacheShard
	shardMask uint64
	capacity  int64
	hits      atomic.Int64
	misses    atomic.Int64
}

type entry struct {
	key   Key
	value []byte
}

type cacheShard struct {
	mu        sync.Mutex
	capacity  int64
	usedBytes int64
	entries   map[Key]*list.Element
	lru       list.List
}

func New(capacity int64) *Cache {
	if capacity <= 0 {
		return nil
	}
	numShards := maxShards
	for numShards > 1 && capacity/int64(numShards) < minShardBytes {
		numShards /= 2
	}
	c := &Cache{shards: make([]cacheShard, numShards), shardMask: uint64(numShards - 1), capacity: capacity}
	for i := range c.shards {
		c.shards[i].capacity = capacity / int64(numShards)
		c.shards[i].entries = make(map[Key]*list.Element)
	}
	return c
}

func (c *Cache) shardFor(key Key) *cacheShard {
	hash := key.FileNum*0x9e3779b97f4a7c15 ^ key.Offset*0xc2b2ae3d27d4eb4f
	hash ^= hash >> 29
	return &c.shards[hash&c.shardMask]
}

func (c *Cache) Get(key Key) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	shard := c.shardFor(key)
	shard.mu.Lock()
	var value []byte
	element, ok := shard.entries[key]
	if ok {
		shard.lru.MoveToFront(element)
		// Read under the lock: Set replaces values in place.
		value = element.Value.(*entry).value
	}
	shard.mu.Unlock()
	if !ok {
		c.misses.Add(1)
		return nil, false
	}
	c.hits.Add(1)
	return value, true
}

func (c *Cache) Set(key Key, value []byte) {
	if c == nil {
		return
	}
	shard := c.shardFor(key)
	size := int64(len(value)) + entryOverheadBytes
	if size > shard.capacity {
		return
	}
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if element, ok := shard.entries[key]; ok {
		existing := element.Value.(*entry)
		shard.usedBytes += size - (int64(len(existing.value)) + entryOverheadBytes)
		existing.value = value
		shard.lru.MoveToFront(element)
	} else {
		shard.entries[key] = shard.lru.PushFront(&entry{key: key, value: value})
		shard.usedBytes += size
	}
	for shard.usedBytes > shard.capacity {
		oldest := shard.lru.Back()
		evicted := oldest.Value.(*entry)
		shard.lru.Remove(oldest)
		delete(shard.entries, evicted.key)
		shard.usedBytes -= int64(len(evicted.value)) + entryOverheadBytes
	}
}

type Metrics struct {
	Capacity int64
	Size     int64
	Count    int64
	Hits     int64
	Misses   int64
}

func (c *Cache) Metrics() Metrics {
	if c == nil {
		return Metrics{}
	}
	metrics := Metrics{Capacity: c.capacity, Hits: c.hits.Load(), Misses: c.misses.Load()}
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		metrics.Size += shard.usedBytes
		metrics.Count += int64(len(shard.entries))
		shard.mu.Unlock()
	}
	return metrics
}

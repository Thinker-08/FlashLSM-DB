package lsmkv

import (
	"container/list"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/cache"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
	"github.com/Thinker-08/FlashLSM-DB/internal/sstable"
	"github.com/Thinker-08/FlashLSM-DB/vfs"
)

type tableCache struct {
	dirname    string
	fs         vfs.FS
	comparer   *base.Comparer
	blockCache *cache.Cache
	stats      *sstable.Stats
	capacity   int

	mu      sync.Mutex
	entries map[uint64]*tableEntry
	lru     list.List
	closed  bool

	hits, misses atomic.Int64
}

type tableEntry struct {
	fileNum    uint64
	reader     *sstable.Reader
	err        error
	loaded     chan struct{}
	refs       int // guarded by tableCache.mu; the cache holds one
	lruElement *list.Element
}

func newTableCache(dirname string, fs vfs.FS, comparer *base.Comparer, blockCache *cache.Cache, stats *sstable.Stats, capacity int) *tableCache {
	return &tableCache{
		dirname: dirname, fs: fs, comparer: comparer, blockCache: blockCache, stats: stats,
		capacity: capacity, entries: make(map[uint64]*tableEntry),
	}
}

func (tc *tableCache) acquire(file *manifest.FileMetadata) (*tableEntry, error) {
	tc.mu.Lock()
	if tc.closed {
		tc.mu.Unlock()
		return nil, ErrClosed
	}
	if entry, ok := tc.entries[file.FileNum]; ok {
		entry.refs++
		tc.lru.MoveToFront(entry.lruElement)
		tc.mu.Unlock()
		<-entry.loaded
		if entry.err != nil {
			tc.release(entry)
			return nil, entry.err
		}
		tc.hits.Add(1)
		return entry, nil
	}
	entry := &tableEntry{fileNum: file.FileNum, loaded: make(chan struct{}), refs: 2}
	entry.lruElement = tc.lru.PushFront(entry)
	tc.entries[file.FileNum] = entry
	evicted := tc.evictLocked()
	tc.mu.Unlock()
	tc.closeReaders(evicted)
	tc.misses.Add(1)

	entry.reader, entry.err = tc.openReader(file)
	close(entry.loaded)
	if entry.err != nil {
		tc.mu.Lock()
		if entry.lruElement != nil {
			tc.lru.Remove(entry.lruElement)
			entry.lruElement = nil
			delete(tc.entries, file.FileNum)
			entry.refs--
		}
		tc.mu.Unlock()
		tc.release(entry)
		return nil, entry.err
	}
	return entry, nil
}

func (tc *tableCache) openReader(file *manifest.FileMetadata) (*sstable.Reader, error) {
	path := base.MakeFilepath(tc.dirname, base.FileTypeTable, file.FileNum)
	tableFile, err := tc.fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("lsmkv: open table %s: %w", filepath.Base(path), err)
	}
	reader, err := sstable.Open(tableFile, file.Size, sstable.ReaderOptions{
		Comparer: tc.comparer, Cache: tc.blockCache, FileNum: file.FileNum,
		Name: filepath.Base(path), Stats: tc.stats,
	})
	if err != nil {
		tableFile.Close()
		return nil, err
	}
	return reader, nil
}

func (tc *tableCache) evictLocked() []*sstable.Reader {
	var toClose []*sstable.Reader
	for tc.lru.Len() > tc.capacity {
		entry := tc.lru.Back().Value.(*tableEntry)
		if reader := tc.removeLocked(entry); reader != nil {
			toClose = append(toClose, reader)
		}
	}
	return toClose
}

func (tc *tableCache) removeLocked(entry *tableEntry) *sstable.Reader {
	if entry.lruElement == nil {
		return nil
	}
	tc.lru.Remove(entry.lruElement)
	entry.lruElement = nil
	delete(tc.entries, entry.fileNum)
	entry.refs--
	if entry.refs == 0 {
		return entry.reader
	}
	return nil
}

func (tc *tableCache) closeReaders(readers []*sstable.Reader) {
	for _, reader := range readers {
		if reader != nil {
			reader.Close()
		}
	}
}

func (tc *tableCache) release(entry *tableEntry) {
	tc.mu.Lock()
	entry.refs--
	unused := entry.refs == 0
	if entry.refs < 0 {
		tc.mu.Unlock()
		panic("lsmkv: table cache entry released too many times")
	}
	tc.mu.Unlock()
	if unused && entry.reader != nil {
		entry.reader.Close()
	}
}

func (tc *tableCache) evict(fileNum uint64) {
	tc.mu.Lock()
	var reader *sstable.Reader
	if entry, ok := tc.entries[fileNum]; ok {
		reader = tc.removeLocked(entry)
	}
	tc.mu.Unlock()
	tc.closeReaders([]*sstable.Reader{reader})
}

func (tc *tableCache) close() {
	tc.mu.Lock()
	tc.closed = true
	var readers []*sstable.Reader
	for _, entry := range tc.entries {
		readers = append(readers, tc.removeLocked(entry))
	}
	tc.mu.Unlock()
	tc.closeReaders(readers)
}

func (tc *tableCache) openCount() int {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.lru.Len()
}

func (tc *tableCache) newIter(file *manifest.FileMetadata, fillCache bool) (base.InternalIterator, error) {
	entry, err := tc.acquire(file)
	if err != nil {
		return nil, err
	}
	return &tableIter{Iter: entry.reader.NewIter(fillCache), tableCache: tc, entry: entry}, nil
}

type tableIter struct {
	*sstable.Iter
	tableCache *tableCache
	entry      *tableEntry
}

func (ti *tableIter) Close() error {
	if ti.entry == nil {
		return nil
	}
	err := ti.Iter.Close()
	ti.tableCache.release(ti.entry)
	ti.entry, ti.Iter = nil, nil
	return err
}

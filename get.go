package lsmkv

import (
	"slices"
	"sort"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
	"github.com/Thinker-08/FlashLSM-DB/internal/memtable"
)

// Get returns a copy of the value of key, or ErrNotFound.
func (db *DB) Get(key []byte, readOpts *ReadOptions) ([]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}
	seq, err := db.readSeq(readOpts)
	if err != nil {
		return nil, err
	}
	// Load seq before the read state, which then holds every write at or below seq.
	state := db.loadReadState()
	defer state.unref()
	lookup := getter{db: db, key: key, seq: seq, fillCache: readOpts == nil || !readOpts.DontFillCache, now: db.now().UnixNano()}
	return lookup.run(state)
}

func (db *DB) readSeq(readOpts *ReadOptions) (uint64, error) {
	if readOpts != nil && readOpts.Snapshot != nil {
		if readOpts.Snapshot.db != db {
			return 0, errForeignSnapshot
		}
		return readOpts.Snapshot.seq, nil
	}
	return db.visibleSeq.Load(), nil
}

type getter struct {
	db        *DB
	key       []byte
	seq       uint64
	fillCache bool
	now       int64
	operands  [][]byte
	value     []byte
	found     bool
	err       error
}

func (g *getter) run(state *readState) ([]byte, error) {
	var memIter memtable.Iter
	state.activeMemtable.InitIter(&memIter)
	if g.search(&memIter) {
		return g.result()
	}
	for i := len(state.immutableMemtables) - 1; i >= 0; i-- {
		state.immutableMemtables[i].InitIter(&memIter)
		if g.search(&memIter) {
			return g.result()
		}
	}
	version := state.version
	for _, file := range version.Levels[0] {
		if file.ContainsUserKey(g.db.compare, g.key) && g.searchTable(file) {
			return g.result()
		}
	}
	for level := 1; level < len(version.Levels); level++ {
		files := version.Levels[level]
		i := sort.Search(len(files), func(i int) bool { return g.db.compare(files[i].Largest.UserKey, g.key) >= 0 })
		if i < len(files) && g.db.compare(files[i].Smallest.UserKey, g.key) <= 0 && g.searchTable(files[i]) {
			return g.result()
		}
	}
	if len(g.operands) > 0 {
		g.finish(nil, false)
	}
	return g.result()
}

func (g *getter) searchTable(file *manifest.FileMetadata) bool {
	entry, err := g.db.tableCache.acquire(file)
	if err != nil {
		g.err = err
		return true
	}
	defer g.db.tableCache.release(entry)
	if !entry.reader.MayContain(g.key) {
		return false
	}
	it := entry.reader.NewIter(g.fillCache)
	defer it.Close()
	return g.search(it)
}

func (g *getter) search(it base.InternalIterator) bool {
	for it.SeekGE(base.MakeSearchKey(g.key, g.seq)); it.Valid(); it.Next() {
		key := it.Key()
		if g.db.compare(key.UserKey, g.key) != 0 {
			return false
		}
		if key.SeqNum() > g.seq {
			continue
		}
		switch key.Kind() {
		case base.KindSet:
			g.finish(it.Value(), true)
		case base.KindSetTTL:
			value, live, err := decodeTTL(it.Value(), g.now)
			if err != nil {
				g.err = err
			} else {
				g.finish(value, live)
			}
		case base.KindDelete:
			g.finish(nil, false)
		case base.KindMerge:
			g.operands = append(g.operands, slices.Clone(it.Value()))
			continue
		default:
			g.err = base.CorruptionErrorf("entry %s has an unknown kind", key)
		}
		return true
	}
	if err := it.Error(); err != nil {
		g.err = err
		return true
	}
	return false
}

func (g *getter) finish(value []byte, exists bool) {
	if len(g.operands) == 0 {
		if exists {
			g.value, g.found = slices.Clone(value), true
			if g.value == nil {
				g.value = []byte{}
			}
		}
		return
	}
	merged, err := g.db.fullMerge(g.key, slices.Clone(value), exists, g.operands, true)
	if err != nil {
		g.err = err
		return
	}
	g.value, g.found = merged, true
}

func (g *getter) result() ([]byte, error) {
	switch {
	case g.err != nil:
		return nil, g.err
	case !g.found:
		return nil, ErrNotFound
	}
	return g.value, nil
}

func (db *DB) fullMerge(key, existing []byte, exists bool, operands [][]byte, newestFirst bool) ([]byte, error) {
	if db.opts.Merger == nil {
		return nil, ErrNoMerger
	}
	if newestFirst {
		operands = slices.Clone(operands)
		slices.Reverse(operands)
	}
	merged, err := db.opts.Merger.FullMerge(key, existing, exists, operands)
	if err != nil {
		return nil, err
	}
	if merged == nil {
		merged = []byte{}
	}
	return merged, nil
}

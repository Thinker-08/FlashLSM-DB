package lsmkv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/cache"
	"github.com/Thinker-08/FlashLSM-DB/internal/compaction"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
	"github.com/Thinker-08/FlashLSM-DB/internal/memtable"
	"github.com/Thinker-08/FlashLSM-DB/internal/record"
	"github.com/Thinker-08/FlashLSM-DB/internal/sstable"
	"github.com/Thinker-08/FlashLSM-DB/vfs"
)

type DB struct {
	dirname string
	opts    *Options
	fs      vfs.FS
	compare base.Compare
	logger  *slog.Logger
	dirLock io.Closer

	// Lock order: compactionMu, the manifest lock, then mu; readStateMu and the
	// version-list mutex are leaves. No I/O happens while mu is held.
	mu           sync.Mutex
	stateChanged sync.Cond

	// Guarded by mu.
	activeMemtable     *memtable.Memtable
	immutableMemtables []*memtable.Memtable // oldest first
	writeQueue         []*writeRequest
	groupBatch         Batch
	snapshots          snapshotList
	backgroundErr      error // sticky: fails every later write
	closing            bool
	pendingOutputs     map[uint64]struct{}
	runningCompaction  *compaction.Compaction
	lastSeq            uint64

	// Owned by the commit leader. A new WAL's directory entry is synced lazily: by its
	// first synced write, or by the previous memtable's flush before the MANIFEST names it.
	wal          *record.Writer
	walFile      vfs.File
	walNum       uint64
	walDirSynced bool

	visibleSeq atomic.Uint64
	closed     atomic.Bool

	readStateMu sync.Mutex
	readState   *readState

	versions         *manifest.VersionSet
	compactionOpts   compaction.Options
	compactionPicker compaction.Picker
	tableCache       *tableCache
	blockCache       *cache.Cache
	tableStats       sstable.Stats

	compactionMu       sync.Mutex
	obsoleteFilesMu    sync.Mutex
	flushRequests      chan struct{}
	compactionRequests chan struct{}
	shutdown           chan struct{}
	backgroundWorkers  sync.WaitGroup

	openIterators   atomic.Int64
	versionInstalls atomic.Int64
	counters        metricCounters
}

func (db *DB) logf(level slog.Level, msg string, args ...any) {
	if db.logger != nil {
		db.logger.Log(context.Background(), level, msg, args...)
	}
}

func (db *DB) checkOpen() error {
	if db.closed.Load() {
		return ErrClosed
	}
	return nil
}

func (db *DB) setBackgroundErrLocked(err error) {
	if db.backgroundErr == nil {
		db.backgroundErr = err
		db.logf(slog.LevelError, "lsmkv: background error; writes now fail until reopen", "err", err)
	}
	db.stateChanged.Broadcast()
}

func (db *DB) hasBackgroundErr() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.backgroundErr != nil
}

func requestWork(requests chan struct{}) {
	select {
	case requests <- struct{}{}:
	default:
	}
}

func (db *DB) maybeScheduleFlush()      { requestWork(db.flushRequests) }
func (db *DB) maybeScheduleCompaction() { requestWork(db.compactionRequests) }

func (db *DB) shuttingDown() bool {
	select {
	case <-db.shutdown:
		return true
	default:
		return false
	}
}

func (db *DB) now() time.Time { return db.opts.Clock() }

func (db *DB) Put(key, value []byte, writeOpts *WriteOptions) error {
	batch := batchPool.Get().(*Batch)
	defer batchPool.Put(batch)
	batch.Reset()
	batch.Put(key, value)
	return db.Write(batch, writeOpts)
}

// Delete removes key. Deleting a missing key is not an error.
func (db *DB) Delete(key []byte, writeOpts *WriteOptions) error {
	batch := batchPool.Get().(*Batch)
	defer batchPool.Put(batch)
	batch.Reset()
	batch.Delete(key)
	return db.Write(batch, writeOpts)
}

// Merge adds an operand for key; Options.Merger combines operands on read and compaction.
func (db *DB) Merge(key, operand []byte, writeOpts *WriteOptions) error {
	batch := batchPool.Get().(*Batch)
	defer batchPool.Put(batch)
	batch.Reset()
	batch.Merge(key, operand)
	return db.Write(batch, writeOpts)
}

// PutWithTTL is Put, except the key reads as deleted once ttl passes on Options.Clock.
func (db *DB) PutWithTTL(key, value []byte, ttl time.Duration, writeOpts *WriteOptions) error {
	batch := batchPool.Get().(*Batch)
	defer batchPool.Put(batch)
	batch.Reset()
	batch.PutWithExpiry(key, value, db.now().Add(ttl))
	return db.Write(batch, writeOpts)
}

var batchPool = sync.Pool{New: func() any { return new(Batch) }}

// Write applies batch atomically; a nil or empty batch with Sync makes every earlier write durable.
func (db *DB) Write(batch *Batch, writeOpts *WriteOptions) error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	if batch == nil {
		batch = &Batch{}
	}
	if err := batch.validate(db.opts.Merger); err != nil {
		return err
	}
	return db.commit(batch, writeOpts != nil && writeOpts.Sync)
}

// Flush waits until the current memtable and every older one are written to SSTables.
func (db *DB) Flush() error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	if err := db.commit(nil, false); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.immutableMemtables) == 0 {
		return db.backgroundErr
	}
	target := db.immutableMemtables[len(db.immutableMemtables)-1]
	for slices.Contains(db.immutableMemtables, target) && db.backgroundErr == nil && !db.closing {
		db.stateChanged.Wait()
	}
	if db.backgroundErr != nil {
		return db.backgroundErr
	}
	if db.closing {
		return ErrClosed
	}
	return nil
}

// Close syncs the WAL; writes not yet flushed are replayed by the next Open.
func (db *DB) Close() error {
	if db.closed.Swap(true) {
		return ErrClosed
	}
	db.mu.Lock()
	db.closing = true
	db.stateChanged.Broadcast()
	for len(db.writeQueue) > 0 {
		db.stateChanged.Wait()
	}
	openSnapshots := db.snapshots.count
	backgroundErr := db.backgroundErr
	db.mu.Unlock()

	close(db.shutdown)
	db.backgroundWorkers.Wait()

	var errs []error
	if db.wal != nil {
		if backgroundErr == nil {
			if err := db.syncWAL(db.wal); err != nil {
				errs = append(errs, fmt.Errorf("lsmkv: sync WAL: %w", err))
			}
		}
		if err := db.walFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	db.mu.Lock()
	state := db.readState
	db.readState = nil
	db.mu.Unlock()
	if state != nil {
		state.unref()
	}
	openIterators := db.openIterators.Load()
	if db.opts.ParanoidChecks {
		if err := db.checkLevelInvariants(); err != nil {
			errs = append(errs, fmt.Errorf("lsmkv: invariant violated: %w", err))
		}
	}
	db.tableCache.close()
	if err := db.versions.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := db.dirLock.Close(); err != nil {
		errs = append(errs, err)
	}
	if db.opts.ParanoidChecks && (openIterators > 0 || openSnapshots > 0) {
		errs = append(errs, fmt.Errorf("lsmkv: closed with %d open iterators and %d unreleased snapshots", openIterators, openSnapshots))
	}
	if backgroundErr != nil {
		errs = append(errs, backgroundErr)
	}
	return errors.Join(errs...)
}

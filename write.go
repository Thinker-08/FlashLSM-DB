package lsmkv

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/memtable"
	"github.com/Thinker-08/FlashLSM-DB/internal/record"
	"github.com/Thinker-08/FlashLSM-DB/vfs"
)

type writeRequest struct {
	batch  *Batch // nil for a Flush request
	sync   bool
	done   bool
	err    error
	wakeup sync.Cond
}

var writeRequestPool = sync.Pool{New: func() any { return new(writeRequest) }}

const (
	maxGroupBytes   = 1 << 20
	smallBatchBytes = 128 << 10
)

func (db *DB) commit(batch *Batch, wantSync bool) error {
	request := writeRequestPool.Get().(*writeRequest)
	*request = writeRequest{batch: batch, sync: wantSync}
	request.wakeup.L = &db.mu
	defer writeRequestPool.Put(request)

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closing {
		return ErrClosed
	}
	db.writeQueue = append(db.writeQueue, request)
	for !request.done && db.writeQueue[0] != request {
		request.wakeup.Wait()
	}
	if request.done {
		return request.err
	}

	lastInGroup := request
	err := db.makeRoomForWriteLocked(batch == nil)
	if err == nil && batch != nil {
		var group *Batch
		group, lastInGroup = db.buildGroupLocked()
		err = db.commitGroupLocked(group, wantSync)
	}
	for {
		finished := db.writeQueue[0]
		db.writeQueue[0] = nil
		db.writeQueue = db.writeQueue[1:]
		if finished != request {
			finished.err, finished.done = err, true
			finished.wakeup.Signal()
		}
		if finished == lastInGroup {
			break
		}
	}
	if len(db.writeQueue) > 0 {
		db.writeQueue[0].wakeup.Signal()
	} else if db.closing {
		db.stateChanged.Broadcast()
	}
	return err
}

func (db *DB) buildGroupLocked() (*Batch, *writeRequest) {
	leader := db.writeQueue[0]
	group := leader.batch
	size := group.Size()
	limit := maxGroupBytes
	if size <= smallBatchBytes {
		limit = size + smallBatchBytes
	}
	lastInGroup := leader
	for _, request := range db.writeQueue[1:] {
		if request.batch == nil || (request.sync && !leader.sync) {
			break
		}
		if size+request.batch.Size()-batchHeaderLen > limit {
			break
		}
		if group == leader.batch {
			group = &db.groupBatch
			group.Reset()
			group.appendBatch(leader.batch)
		}
		group.appendBatch(request.batch)
		size += request.batch.Size() - batchHeaderLen
		lastInGroup = request
	}
	return group, lastInGroup
}

func (db *DB) commitGroupLocked(group *Batch, wantSync bool) error {
	count := uint64(group.count)
	firstSeq := db.lastSeq + 1
	if count > 0 {
		group.setSeqNum(firstSeq)
	}
	mem, wal := db.activeMemtable, db.wal
	// Only the leader writes the WAL and memtable, and readers skip entries above visibleSeq.
	db.mu.Unlock()

	var err error
	if count > 0 {
		err = wal.WriteRecord(group.encoded())
		if err == nil {
			db.counters.walBytesWritten.Add(int64(len(group.encoded())) + record.HeaderLen)
			db.counters.userBytesWritten.Add(int64(len(group.encoded()) - batchHeaderLen))
		}
	}
	if err == nil && wantSync {
		err = db.syncWAL(wal)
	}
	if err == nil && count > 0 {
		err = applyBatch(mem, group.encoded())
	}

	db.mu.Lock()
	if err != nil {
		// The WAL's contents are now unknown, so the failure is sticky.
		err = fmt.Errorf("lsmkv: commit: %w", err)
		db.setBackgroundErrLocked(err)
		return err
	}
	db.lastSeq += count
	db.visibleSeq.Store(db.lastSeq)
	return nil
}

func (db *DB) makeRoomForWriteLocked(force bool) error {
	allowDelay := !force
	stalled := false
	var stallStart time.Time
	defer func() {
		if stalled {
			db.counters.stallDuration.Add(int64(time.Since(stallStart)))
		}
	}()
	startStall := func() {
		if !stalled {
			stalled = true
			stallStart = time.Now()
			db.counters.stalls.Add(1)
		}
	}
	numL0Files := func() int { return len(db.versions.Current().Levels[0]) }
	autoCompactions := !db.opts.DisableAutomaticCompactions
	for {
		switch {
		case db.backgroundErr != nil:
			return db.backgroundErr
		case db.closing:
			return ErrClosed
		case allowDelay && autoCompactions && numL0Files() >= db.opts.L0SlowdownWritesTrigger:
			startStall()
			db.mu.Unlock()
			time.Sleep(time.Millisecond)
			db.mu.Lock()
			allowDelay = false
		case !force && db.activeMemtable.ApproximateSize() < db.opts.MemtableSize:
			return nil
		case force && db.activeMemtable.Empty():
			return nil
		case len(db.immutableMemtables) >= db.opts.MaxImmutableMemtables:
			startStall()
			db.maybeScheduleFlush()
			db.stateChanged.Wait()
		case autoCompactions && numL0Files() >= db.opts.L0StopWritesTrigger:
			startStall()
			db.maybeScheduleCompaction()
			db.stateChanged.Wait()
		default:
			if err := db.rotateMemtableLocked(); err != nil {
				return err
			}
			force = false
		}
	}
}

func (db *DB) rotateMemtableLocked() error {
	walNum := db.versions.NewFileNum()
	oldWAL, oldFile := db.wal, db.walFile
	db.mu.Unlock()
	walFile, err := db.rotateWAL(oldWAL, oldFile, walNum)
	db.mu.Lock()
	if err != nil {
		err = fmt.Errorf("lsmkv: rotate WAL: %w", err)
		db.setBackgroundErrLocked(err)
		return err
	}
	db.wal, db.walFile, db.walNum, db.walDirSynced = record.NewWriter(walFile), walFile, walNum, false
	db.immutableMemtables = append(db.immutableMemtables, db.activeMemtable)
	db.activeMemtable = memtable.New(db.compare, walNum)
	db.updateReadStateLocked()
	db.maybeScheduleFlush()
	return nil
}

func (db *DB) syncWAL(wal *record.Writer) error {
	if err := wal.Sync(); err != nil {
		return err
	}
	db.counters.walSyncs.Add(1)
	if !db.walDirSynced {
		if err := db.fs.SyncDir(db.dirname); err != nil {
			return err
		}
		db.walDirSynced = true
	}
	return nil
}

func (db *DB) rotateWAL(oldWAL *record.Writer, oldFile vfs.File, walNum uint64) (vfs.File, error) {
	// Only the newest WAL may have a torn tail; recovery treats one elsewhere as corruption.
	if err := oldWAL.Sync(); err != nil {
		return nil, err
	}
	db.counters.walSyncs.Add(1)
	walFile, err := db.createWAL(walNum)
	if err != nil {
		return nil, err
	}
	if err := oldFile.Close(); err != nil {
		db.logf(slog.LevelWarn, "lsmkv: closing old WAL", "err", err)
	}
	return walFile, nil
}

func (db *DB) createWAL(walNum uint64) (vfs.File, error) {
	walFile, err := db.fs.Create(base.MakeFilepath(db.dirname, base.FileTypeLog, walNum))
	if err != nil {
		return nil, err
	}
	db.counters.walFilesCreated.Add(1)
	return walFile, nil
}

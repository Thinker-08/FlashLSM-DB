package lsmkv

import (
	"fmt"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
	"github.com/Thinker-08/FlashLSM-DB/internal/memtable"
	"github.com/Thinker-08/FlashLSM-DB/internal/sstable"
)

const maxFlushLevel = 2

func (db *DB) flushLoop() {
	defer db.backgroundWorkers.Done()
	for {
		select {
		case <-db.shutdown:
			return
		case <-db.flushRequests:
		}
		db.flushPending()
	}
}

func (db *DB) flushPending() {
	for !db.shuttingDown() {
		db.mu.Lock()
		if db.backgroundErr != nil || len(db.immutableMemtables) == 0 {
			db.mu.Unlock()
			return
		}
		oldest := db.immutableMemtables[0]
		// Once oldest is flushed, the oldest WAL still needed is its successor's, which rotations cannot change.
		nextLogNum := db.activeMemtable.LogNum()
		if len(db.immutableMemtables) > 1 {
			nextLogNum = db.immutableMemtables[1].LogNum()
		}
		oldestSnapshotSeq := db.oldestSnapshotSeqLocked()
		db.mu.Unlock()

		if err := db.flushMemtable(oldest, nextLogNum, oldestSnapshotSeq); err != nil {
			db.mu.Lock()
			db.setBackgroundErrLocked(fmt.Errorf("lsmkv: flush: %w", err))
			db.mu.Unlock()
			return
		}
	}
}

func (db *DB) flushMemtable(mem *memtable.Memtable, nextLogNum, oldestSnapshotSeq uint64) error {
	var file *manifest.FileMetadata
	if !mem.Empty() {
		var err error
		if file, err = db.writeMemtable(mem, oldestSnapshotSeq); err != nil {
			return err
		}
	}

	db.versions.Lock()
	edit := &manifest.VersionEdit{}
	edit.SetLogNumber(nextLogNum)
	edit.SetLastSeq(db.visibleSeq.Load())
	level := 0
	if file != nil {
		level = db.pickFlushLevel(file)
		edit.AddFile(level, file)
	}
	err := db.versions.LogAndApply(edit, func(*manifest.Version) {
		db.immutableMemtables = db.immutableMemtables[1:]
		if file != nil {
			delete(db.pendingOutputs, file.FileNum)
			db.counters.flushes.Add(1)
			db.counters.flushBytesWritten.Add(int64(file.Size))
			db.counters.levelBytesIn[level].Add(int64(file.Size))
		}
		db.updateReadStateLocked()
		db.stateChanged.Broadcast()
	})
	db.versions.Unlock()
	if err != nil {
		// The edit may still be durable, so keep the table; Open deletes it if unreferenced.
		return err
	}
	db.maybeScheduleCompaction()
	db.deleteObsoleteFiles()
	db.maybeCheckInvariants()
	return nil
}

func (db *DB) writeMemtable(mem *memtable.Memtable, oldestSnapshotSeq uint64) (*manifest.FileMetadata, error) {
	tableWriter, file, err := db.newTable()
	if err != nil {
		return nil, err
	}
	it := mem.NewIter()
	var currentKey []byte
	shadowed, isFirst := false, true
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		if isFirst || db.compare(key.UserKey, currentKey) != 0 {
			currentKey = append(currentKey[:0], key.UserKey...)
			shadowed, isFirst = false, false
		} else if shadowed {
			continue
		}
		if err := tableWriter.Add(key, it.Value()); err != nil {
			tableWriter.Abort()
			db.abandonOutputs([]*manifest.FileMetadata{file})
			return nil, err
		}
		if key.SeqNum() <= oldestSnapshotSeq && key.Kind() != base.KindMerge {
			shadowed = true
		}
	}
	if err := db.finishTable(tableWriter, file); err != nil {
		db.abandonOutputs([]*manifest.FileMetadata{file})
		return nil, err
	}
	if err := db.fs.SyncDir(db.dirname); err != nil {
		db.abandonOutputs([]*manifest.FileMetadata{file})
		return nil, err
	}
	return file, nil
}

func (db *DB) newTable() (*sstable.Writer, *manifest.FileMetadata, error) {
	db.mu.Lock()
	fileNum := db.versions.NewFileNum()
	db.pendingOutputs[fileNum] = struct{}{}
	db.mu.Unlock()
	file := &manifest.FileMetadata{FileNum: fileNum}
	tableFile, err := db.fs.Create(base.MakeFilepath(db.dirname, base.FileTypeTable, fileNum))
	if err != nil {
		db.abandonOutputs([]*manifest.FileMetadata{file})
		return nil, nil, err
	}
	tableWriter := sstable.NewWriter(tableFile, sstable.WriterOptions{
		Comparer:             db.opts.Comparer,
		BlockSize:            db.opts.BlockSize,
		BlockRestartInterval: db.opts.BlockRestartInterval,
		BloomBitsPerKey:      db.opts.BloomBitsPerKey,
		Compression:          db.opts.Compression,
	})
	return tableWriter, file, nil
}

func (db *DB) finishTable(tableWriter *sstable.Writer, file *manifest.FileMetadata) error {
	if err := tableWriter.Close(); err != nil {
		return err
	}
	written := tableWriter.Metadata()
	file.Size = written.Size
	file.Smallest, file.Largest = written.Smallest, written.Largest
	file.SmallestSeq, file.LargestSeq = written.SmallestSeq, written.LargestSeq
	return nil
}

func (db *DB) abandonOutputs(files []*manifest.FileMetadata) {
	for _, file := range files {
		db.fs.Remove(base.MakeFilepath(db.dirname, base.FileTypeTable, file.FileNum))
	}
	db.mu.Lock()
	for _, file := range files {
		delete(db.pendingOutputs, file.FileNum)
	}
	db.mu.Unlock()
}

func (db *DB) pickFlushLevel(file *manifest.FileMetadata) int {
	if db.opts.CompactionStrategy != LeveledCompaction {
		return 0
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	version := db.versions.Current()
	smallest, largest := file.Smallest.UserKey, file.Largest.UserKey
	if len(version.Overlaps(0, db.compare, smallest, largest)) > 0 {
		return 0
	}
	level := 0
	for level < maxFlushLevel && level+1 < db.opts.NumLevels {
		nextLevel := level + 1
		if len(version.Overlaps(nextLevel, db.compare, smallest, largest)) > 0 {
			break
		}
		if nextLevel+1 < db.opts.NumLevels &&
			manifest.TotalSize(version.Overlaps(nextLevel+1, db.compare, smallest, largest)) > db.compactionOpts.MaxGrandparentOverlap() {
			break
		}
		// A running compaction overlapping the table must not land older data above or beside it.
		if job := db.runningCompaction; job != nil && job.OutputLevel <= nextLevel &&
			db.compare(job.Smallest, largest) <= 0 && db.compare(smallest, job.Largest) <= 0 {
			break
		}
		level = nextLevel
	}
	return level
}

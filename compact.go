package lsmkv

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/compaction"
	"github.com/Thinker-08/FlashLSM-DB/internal/iterator"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
	"github.com/Thinker-08/FlashLSM-DB/internal/sstable"
)

var errCompactionAborted = errors.New("lsmkv: compaction aborted by Close")

func (db *DB) compactLoop() {
	defer db.backgroundWorkers.Done()
	for {
		select {
		case <-db.shutdown:
			return
		case <-db.compactionRequests:
		}
		db.compactPending()
	}
}

func (db *DB) compactPending() {
	for !db.shuttingDown() {
		db.compactionMu.Lock()
		job := db.pickCompaction()
		if job == nil {
			db.compactionMu.Unlock()
			return
		}
		err := db.runCompaction(job)
		db.compactionMu.Unlock()
		if err != nil {
			if !errors.Is(err, errCompactionAborted) {
				db.mu.Lock()
				db.setBackgroundErrLocked(fmt.Errorf("lsmkv: compaction: %w", err))
				db.mu.Unlock()
			}
			return
		}
	}
}

// pickCompaction holds the manifest lock so it never races a flush choosing its level.
func (db *DB) pickCompaction() *compaction.Compaction {
	db.versions.Lock()
	defer db.versions.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.backgroundErr != nil || db.opts.DisableAutomaticCompactions {
		return nil
	}
	var env compaction.Env
	for level := range env.CompactPointers {
		env.CompactPointers[level] = db.versions.CompactPointer(level)
	}
	job := db.compactionPicker.Pick(db.versions.Current(), env)
	if job != nil {
		db.beginCompactionLocked(job)
	}
	return job
}

func (db *DB) beginCompactionLocked(job *compaction.Compaction) {
	job.Version.Ref()
	for _, file := range job.InputFiles() {
		file.Compacting = true
	}
	db.runningCompaction = job
}

func (db *DB) endCompaction(job *compaction.Compaction) {
	db.mu.Lock()
	for _, file := range job.InputFiles() {
		file.Compacting = false
	}
	db.runningCompaction = nil
	job.Version.Unref()
	db.stateChanged.Broadcast()
	db.mu.Unlock()
}

func (db *DB) runCompaction(job *compaction.Compaction) error {
	err := db.compact(job)
	// job.Version still lists the inputs, so release it before collecting them.
	db.endCompaction(job)
	if err == nil && !job.TrivialMove {
		db.deleteObsoleteFiles()
		db.maybeCheckInvariants()
	}
	return err
}

func (db *DB) compact(job *compaction.Compaction) error {
	start := time.Now()
	if job.TrivialMove {
		file := job.Inputs[0].Files[0]
		edit := &manifest.VersionEdit{}
		edit.DeleteFile(job.StartLevel(), file.FileNum)
		edit.AddFile(job.OutputLevel, file)
		if job.CompactPointer != nil {
			edit.SetCompactPointer(job.CompactPointer.Level, job.CompactPointer.Key)
		}
		edit.SetLastSeq(db.visibleSeq.Load())
		db.versions.Lock()
		err := db.versions.LogAndApply(edit, func(*manifest.Version) {
			db.counters.trivialMoves.Add(1)
			db.updateReadStateLocked()
		})
		db.versions.Unlock()
		if err == nil {
			db.logf(slog.LevelDebug, "lsmkv: trivial move", "file", file.FileNum, "from", job.StartLevel(), "to", job.OutputLevel)
		}
		return err
	}

	db.mu.Lock()
	oldestSnapshotSeq := db.oldestSnapshotSeqLocked()
	noSnapshots := db.snapshots.count == 0
	db.mu.Unlock()

	outputs, stats, err := db.writeCompactionOutputs(job, oldestSnapshotSeq, noSnapshots)
	if err != nil {
		db.abandonOutputs(outputs)
		return err
	}
	edit := &manifest.VersionEdit{}
	for _, input := range job.Inputs {
		for _, file := range input.Files {
			edit.DeleteFile(input.Level, file.FileNum)
		}
	}
	var bytesWritten uint64
	for _, file := range outputs {
		edit.AddFile(job.OutputLevel, file)
		bytesWritten += file.Size
	}
	if job.CompactPointer != nil {
		edit.SetCompactPointer(job.CompactPointer.Level, job.CompactPointer.Key)
	}
	edit.SetLastSeq(db.visibleSeq.Load())
	db.versions.Lock()
	err = db.versions.LogAndApply(edit, func(*manifest.Version) {
		for _, file := range outputs {
			delete(db.pendingOutputs, file.FileNum)
		}
		db.counters.compactions.Add(1)
		db.counters.compactBytesRead.Add(int64(job.InputBytes()))
		db.counters.compactBytesWritten.Add(int64(bytesWritten))
		db.counters.levelBytesIn[job.OutputLevel].Add(int64(bytesWritten))
		db.updateReadStateLocked()
	})
	db.versions.Unlock()
	if err != nil {
		// The edit may still be durable, so keep the outputs; Open deletes them if unreferenced.
		return err
	}
	db.logf(slog.LevelDebug, "lsmkv: compacted",
		"reason", job.Reason, "from", job.StartLevel(), "to", job.OutputLevel,
		"inputs", len(job.InputFiles()), "outputs", len(outputs),
		"read", job.InputBytes(), "written", bytesWritten,
		"dropped", stats.dropped, "took", time.Since(start))
	return nil
}

func (db *DB) compactionIter(job *compaction.Compaction) (base.InternalIterator, error) {
	var iters []base.InternalIterator
	fail := func(err error) (base.InternalIterator, error) {
		for _, it := range iters {
			it.Close()
		}
		return nil, err
	}
	openTable := func(file *manifest.FileMetadata) (base.InternalIterator, error) {
		return db.tableCache.newIter(file, false)
	}
	for _, input := range job.Inputs {
		if input.Level == 0 {
			for _, file := range input.Files {
				it, err := openTable(file)
				if err != nil {
					return fail(err)
				}
				iters = append(iters, it)
			}
			continue
		}
		iters = append(iters, iterator.NewLevel(db.compare, input.Files, openTable, nil, nil))
	}
	return iterator.NewMerging(db.compare, iters...), nil
}

type compactionStats struct {
	dropped int64
}

type compactionWriter struct {
	db          *DB
	job         *compaction.Compaction
	tableWriter *sstable.Writer
	currentFile *manifest.FileMetadata
	outputs     []*manifest.FileMetadata
}

func (cw *compactionWriter) add(key base.InternalKey, value []byte) error {
	if cw.tableWriter == nil {
		tableWriter, file, err := cw.db.newTable()
		if err != nil {
			return err
		}
		cw.tableWriter, cw.currentFile = tableWriter, file
		cw.outputs = append(cw.outputs, file)
	}
	return cw.tableWriter.Add(key, value)
}

func (cw *compactionWriter) finish() error {
	if cw.tableWriter == nil {
		return nil
	}
	err := cw.db.finishTable(cw.tableWriter, cw.currentFile)
	cw.tableWriter, cw.currentFile = nil, nil
	return err
}

func (cw *compactionWriter) abort() {
	if cw.tableWriter != nil {
		cw.tableWriter.Abort()
		cw.tableWriter, cw.currentFile = nil, nil
	}
}

func (db *DB) writeCompactionOutputs(job *compaction.Compaction, oldestSnapshotSeq uint64, noSnapshots bool) ([]*manifest.FileMetadata, compactionStats, error) {
	var stats compactionStats
	it, err := db.compactionIter(job)
	if err != nil {
		return nil, stats, err
	}
	defer it.Close()
	output := &compactionWriter{db: db, job: job}
	fail := func(err error) ([]*manifest.FileMetadata, compactionStats, error) {
		output.abort()
		return output.outputs, stats, err
	}
	processor := keyProcessor{
		db: db, job: job, output: output, it: it, stats: &stats,
		oldestSnapshotSeq: oldestSnapshotSeq,
		// Filtering would change what snapshots read.
		applyFilter: noSnapshots && db.opts.CompactionFilter != nil,
		now:         db.now().UnixNano(),
	}
	pendingSplit := false
	keysSeen := 0
	for it.First(); it.Valid(); {
		if keysSeen++; keysSeen%1024 == 0 && db.shuttingDown() {
			return fail(errCompactionAborted)
		}
		key := it.Key()
		if job.ShouldStopBefore(key) {
			pendingSplit = true
		}
		// Split only between user keys: no user key may span two files of a level.
		if output.tableWriter != nil && (pendingSplit || (job.MaxOutputFileSize > 0 && output.tableWriter.EstimatedSize() >= job.MaxOutputFileSize)) {
			if err := output.finish(); err != nil {
				return fail(err)
			}
			pendingSplit = false
			if db.shuttingDown() {
				return fail(errCompactionAborted)
			}
		}
		if err := processor.processKey(); err != nil {
			return fail(err)
		}
	}
	if err := it.Error(); err != nil {
		return fail(err)
	}
	if err := output.finish(); err != nil {
		return fail(err)
	}
	if len(output.outputs) > 0 {
		if err := db.fs.SyncDir(db.dirname); err != nil {
			return output.outputs, stats, err
		}
	}
	return output.outputs, stats, nil
}

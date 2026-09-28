package lsmkv

import (
	"errors"
	"fmt"
	"slices"

	"github.com/Thinker-08/FlashLSM-DB/internal/compaction"
)

// CompactRange compacts [start, end] (nil is unbounded), dropping deleted and expired entries.
func (db *DB) CompactRange(start, end []byte) error {
	if err := db.Flush(); err != nil {
		return err
	}
	db.compactionMu.Lock()
	defer db.compactionMu.Unlock()

	db.mu.Lock()
	version := db.versions.Current()
	deepestLevel := 0
	for level := 1; level < db.opts.NumLevels; level++ {
		if len(version.Overlaps(level, db.compare, start, end)) > 0 {
			deepestLevel = level
		}
	}
	db.mu.Unlock()

	if deepestLevel == 0 {
		return db.manualCompact(0, start, end, false)
	}
	for level := 0; level < deepestLevel; level++ {
		if err := db.manualCompact(level, start, end, false); err != nil {
			return err
		}
	}
	return db.manualCompact(deepestLevel, start, end, true)
}

func (db *DB) manualCompact(level int, start, end []byte, inPlace bool) error {
	var resumeAfter []byte
	for {
		if err := db.checkOpen(); err != nil {
			return err
		}
		if db.shuttingDown() {
			return ErrClosed
		}
		db.versions.Lock()
		db.mu.Lock()
		if db.backgroundErr != nil {
			err := db.backgroundErr
			db.mu.Unlock()
			db.versions.Unlock()
			return err
		}
		job := compaction.PickManual(db.compare, &db.compactionOpts, db.versions.Current(), level, start, end, inPlace, resumeAfter)
		if job != nil {
			db.beginCompactionLocked(job)
		}
		db.mu.Unlock()
		db.versions.Unlock()
		if job == nil {
			return nil
		}
		if err := db.runCompaction(job); err != nil {
			if errors.Is(err, errCompactionAborted) {
				return ErrClosed
			}
			db.mu.Lock()
			db.setBackgroundErrLocked(fmt.Errorf("lsmkv: manual compaction: %w", err))
			db.mu.Unlock()
			return err
		}
		if inPlace {
			resumeAfter = slices.Clone(job.Largest)
		}
	}
}

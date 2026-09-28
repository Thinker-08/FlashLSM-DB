package lsmkv

import (
	"errors"
	"fmt"

	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
)

const paranoidCheckEvery = 8

func (db *DB) maybeCheckInvariants() {
	if !db.opts.ParanoidChecks || db.versionInstalls.Add(1)%paranoidCheckEvery != 0 {
		return
	}
	if err := db.checkLevelInvariants(); err != nil && !errors.Is(err, ErrClosed) {
		panic(fmt.Sprintf("lsmkv: invariant violated: %v", err))
	}
}

func (db *DB) checkLevelInvariants() error {
	db.mu.Lock()
	version := db.versions.Current()
	version.Ref()
	db.mu.Unlock()
	defer version.Unref()
	if err := version.CheckOrdering(db.compare); err != nil {
		return err
	}
	oldestSeqAbove := make(map[string]uint64)
	scanTables := func(files []*manifest.FileMetadata, levelName string) error {
		oldestSeqHere := make(map[string]uint64)
		for _, file := range files {
			it, err := db.tableCache.newIter(file, false)
			if err != nil {
				return err
			}
			for it.First(); it.Valid(); it.Next() {
				internalKey := it.Key()
				userKey, seq := string(internalKey.UserKey), internalKey.SeqNum()
				if aboveSeq, ok := oldestSeqAbove[userKey]; ok && seq >= aboveSeq {
					it.Close()
					return fmt.Errorf("key %q: seq %d in %s table %06d is not older than seq %d above it", userKey, seq, levelName, file.FileNum, aboveSeq)
				}
				if hereSeq, ok := oldestSeqHere[userKey]; !ok || seq < hereSeq {
					oldestSeqHere[userKey] = seq
				}
			}
			err = it.Error()
			it.Close()
			if err != nil {
				return err
			}
		}
		for userKey, seq := range oldestSeqHere {
			if aboveSeq, ok := oldestSeqAbove[userKey]; !ok || seq < aboveSeq {
				oldestSeqAbove[userKey] = seq
			}
		}
		return nil
	}
	for _, file := range version.Levels[0] {
		if err := scanTables([]*manifest.FileMetadata{file}, "L0"); err != nil {
			return err
		}
	}
	for level := 1; level < len(version.Levels); level++ {
		if err := scanTables(version.Levels[level], fmt.Sprintf("L%d", level)); err != nil {
			return err
		}
	}
	return nil
}

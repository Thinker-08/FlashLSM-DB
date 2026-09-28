package lsmkv

import (
	"log/slog"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

func (db *DB) deleteObsoleteFiles() {
	db.obsoleteFilesMu.Lock()
	defer db.obsoleteFilesMu.Unlock()
	if db.hasBackgroundErr() {
		return
	}
	// List first: any listed file that a job is still writing is then already in pendingOutputs.
	names, err := db.fs.List(db.dirname)
	if err != nil {
		db.logf(slog.LevelWarn, "lsmkv: listing obsolete files", "err", err)
		return
	}
	live := make(map[uint64]struct{})
	db.versions.Lock()
	db.mu.Lock()
	if db.backgroundErr != nil {
		// A failed MANIFEST write may still have persisted, so delete nothing until reopen.
		db.mu.Unlock()
		db.versions.Unlock()
		return
	}
	db.versions.AddLiveFiles(live)
	for fileNum := range db.pendingOutputs {
		live[fileNum] = struct{}{}
	}
	logNum := db.versions.LogNumber()
	manifestNum := db.versions.ManifestFileNum()
	db.mu.Unlock()
	db.versions.Unlock()

	for _, name := range names {
		fileType, fileNum, ok := base.ParseFilename(name)
		if !ok {
			continue
		}
		var obsolete bool
		switch fileType {
		case base.FileTypeTable:
			_, isLive := live[fileNum]
			obsolete = !isLive
		case base.FileTypeLog:
			obsolete = fileNum < logNum
		case base.FileTypeManifest:
			obsolete = fileNum != manifestNum
		}
		if !obsolete {
			continue
		}
		if fileType == base.FileTypeTable {
			db.tableCache.evict(fileNum)
		}
		if err := db.fs.Remove(base.MakeFilepath(db.dirname, fileType, fileNum)); err != nil {
			db.logf(slog.LevelWarn, "lsmkv: deleting obsolete file", "file", name, "err", err)
			continue
		}
		db.counters.filesDeleted.Add(1)
	}
}

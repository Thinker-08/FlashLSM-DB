package lsmkv

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/cache"
	"github.com/Thinker-08/FlashLSM-DB/internal/compaction"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
	"github.com/Thinker-08/FlashLSM-DB/internal/memtable"
	"github.com/Thinker-08/FlashLSM-DB/internal/record"
	"github.com/Thinker-08/FlashLSM-DB/vfs"
)

// Open opens the database in dirname; nil opts means DefaultOptions. A torn tail on the newest
// WAL is dropped; other damage is ErrCorruption unless Options.BestEffortRecovery is set.
func Open(dirname string, opts *Options) (*DB, error) {
	options, err := opts.validate()
	if err != nil {
		return nil, err
	}
	if options.CreateIfMissing {
		if err := options.FS.MkdirAll(dirname, 0o755); err != nil {
			return nil, err
		}
	}
	dirLock, err := options.FS.Lock(filepath.Join(dirname, base.LockFilename))
	if err != nil {
		if vfs.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrDoesNotExist, dirname)
		}
		return nil, fmt.Errorf("lsmkv: open %s: %w", dirname, err)
	}
	db := &DB{
		dirname:            dirname,
		opts:               options,
		fs:                 options.FS,
		compare:            options.Comparer.Compare,
		logger:             options.Logger,
		dirLock:            dirLock,
		pendingOutputs:     make(map[uint64]struct{}),
		flushRequests:      make(chan struct{}, 1),
		compactionRequests: make(chan struct{}, 1),
		shutdown:           make(chan struct{}),
	}
	db.stateChanged.L = &db.mu
	db.snapshots.init()
	db.compactionOpts = compaction.Options{
		NumLevels:               options.NumLevels,
		L0CompactionTrigger:     options.L0CompactionTrigger,
		L0SlowdownWritesTrigger: options.L0SlowdownWritesTrigger,
		L1MaxBytes:              uint64(options.L1MaxBytes),
		LevelMultiplier:         uint64(options.LevelMultiplier),
		TargetFileSize:          uint64(options.TargetFileSize),
		TieredMinMergeWidth:     options.SizeTiered.MinMergeWidth,
		TieredSizeRatio:         options.SizeTiered.SizeRatio,
		TieredMaxSizeAmpPercent: uint64(options.SizeTiered.MaxSizeAmplificationPercent),
	}
	if options.CompactionStrategy == SizeTieredCompaction {
		db.compactionPicker = compaction.NewTiered(db.compare, &db.compactionOpts)
	} else {
		db.compactionPicker = compaction.NewLeveled(db.compare, &db.compactionOpts)
	}
	db.blockCache = cache.New(options.BlockCacheSize)
	db.tableCache = newTableCache(dirname, options.FS, options.Comparer, db.blockCache, &db.tableStats, options.MaxOpenFiles)
	db.versions = manifest.New(manifest.Options{
		Dirname: dirname, FS: options.FS, Comparer: options.Comparer, MaxManifestSize: options.MaxManifestFileSize,
		Logf: func(format string, args ...any) { db.logf(slog.LevelWarn, fmt.Sprintf(format, args...)) },
	}, &db.mu)

	if err := db.recoverFromDisk(); err != nil {
		db.tableCache.close()
		db.versions.Close()
		if db.walFile != nil {
			db.walFile.Close()
		}
		dirLock.Close()
		return nil, err
	}
	db.backgroundWorkers.Add(2)
	go db.flushLoop()
	go db.compactLoop()
	db.maybeScheduleCompaction()
	return db, nil
}

func (db *DB) recoverFromDisk() error {
	names, err := db.fs.List(db.dirname)
	if err != nil {
		return err
	}
	type numberedFile struct {
		fileType base.FileType
		fileNum  uint64
	}
	var entries []numberedFile
	dbExists := false
	for _, name := range names {
		fileType, fileNum, ok := base.ParseFilename(name)
		if !ok {
			continue
		}
		switch fileType {
		case base.FileTypeCurrent:
			dbExists = true
		case base.FileTypeLog, base.FileTypeTable, base.FileTypeManifest:
			entries = append(entries, numberedFile{fileType, fileNum})
		}
	}
	switch {
	case dbExists && db.opts.ErrorIfExists:
		return fmt.Errorf("%w: %s", ErrExists, db.dirname)
	case !dbExists && !db.opts.CreateIfMissing:
		return fmt.Errorf("%w: %s", ErrDoesNotExist, db.dirname)
	case !dbExists:
		for _, entry := range entries {
			if entry.fileType != base.FileTypeManifest {
				return base.CorruptionErrorf("%s holds %s files but no CURRENT; refusing to overwrite them", db.dirname, entry.fileType)
			}
		}
	}
	if dbExists {
		if err := db.versions.Load(); err != nil {
			return err
		}
	}
	for _, entry := range entries {
		db.versions.MarkFileNumUsed(entry.fileNum)
	}

	// A MANIFEST that stopped early at damage misses later edits; missing files expose that.
	version := db.versions.Current()
	presentTables := make(map[uint64]bool)
	var walNums []uint64
	logNum := db.versions.LogNumber()
	for _, entry := range entries {
		switch {
		case entry.fileType == base.FileTypeTable:
			presentTables[entry.fileNum] = true
		case entry.fileType == base.FileTypeLog && entry.fileNum >= logNum:
			walNums = append(walNums, entry.fileNum)
		}
	}
	slices.Sort(walNums)
	for level, files := range version.Levels {
		if level >= db.opts.NumLevels && len(files) > 0 {
			return fmt.Errorf("%w: the database has files at L%d, beyond NumLevels %d", ErrInvalidOptions, level, db.opts.NumLevels)
		}
		for _, file := range files {
			if !presentTables[file.FileNum] {
				return base.CorruptionErrorf("table %s is missing", base.MakeFilename(base.FileTypeTable, file.FileNum))
			}
		}
	}
	if dbExists && len(walNums) > 0 && walNums[0] != logNum {
		return base.CorruptionErrorf("WAL %s is missing, but newer WALs exist", base.MakeFilename(base.FileTypeLog, logNum))
	}

	db.lastSeq = db.versions.LastSeq()
	edit := &manifest.VersionEdit{}
	var mem *memtable.Memtable
	for i, walFileNum := range walNums {
		stop, err := db.replayWAL(walFileNum, i == len(walNums)-1, edit, &mem)
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	if mem != nil && !mem.Empty() {
		file, err := db.writeMemtable(mem, base.SeqNumMax)
		if err != nil {
			return err
		}
		edit.AddFile(0, file)
	}

	// Record the new log number before creating its WAL: a new WAL next to an old torn one
	// would make that torn tail look like corruption on the next Open.
	newWALNum := db.versions.NewFileNum()
	edit.SetLogNumber(newWALNum)
	edit.SetLastSeq(db.lastSeq)
	db.versions.ForceNewManifest()
	db.versions.Lock()
	err = db.versions.LogAndApply(edit, nil)
	db.versions.Unlock()
	if err != nil {
		return err
	}
	walFile, err := db.createWAL(newWALNum)
	if err != nil {
		return err
	}
	db.wal, db.walFile, db.walNum, db.walDirSynced = record.NewWriter(walFile), walFile, newWALNum, false
	db.visibleSeq.Store(db.lastSeq)

	db.mu.Lock()
	db.activeMemtable = memtable.New(db.compare, newWALNum)
	clear(db.pendingOutputs)
	db.updateReadStateLocked()
	db.mu.Unlock()

	db.deleteObsoleteFiles()
	if err := db.fs.Remove(filepath.Join(db.dirname, base.CurrentTmpFilename)); err != nil && !vfs.IsNotExist(err) {
		db.logf(slog.LevelWarn, "lsmkv: removing stale CURRENT.tmp", "err", err)
	}
	return nil
}

func (db *DB) replayWAL(walNum uint64, newest bool, edit *manifest.VersionEdit, mem **memtable.Memtable) (stop bool, err error) {
	name := base.MakeFilename(base.FileTypeLog, walNum)
	walFile, err := db.fs.Open(filepath.Join(db.dirname, name))
	if err != nil {
		return false, err
	}
	defer walFile.Close()
	info, err := walFile.Stat()
	if err != nil {
		return false, err
	}
	// Older WALs were synced before rotation, so only the newest can have a torn tail.
	handleDamagedRecord := func(err error, offset int64) (bool, error) {
		if newest {
			db.logf(slog.LevelWarn, "lsmkv: dropping torn tail of the newest WAL", "wal", name, "offset", offset, "err", err)
			return true, nil
		}
		return db.handleCorruptWAL(name, err, offset)
	}

	records := record.NewReader(walFile, info.Size())
	for {
		payload, err := records.Next()
		if err == io.EOF {
			return false, nil
		}
		var recordErr *record.Error
		if errors.As(err, &recordErr) {
			return handleDamagedRecord(err, records.Offset())
		}
		if err != nil {
			return false, err
		}
		// A checksummed record that fails to decode is corruption, not a torn write.
		// Validate it whole so recovery never applies half a batch.
		count, err := validateBatch(payload)
		if err != nil {
			return db.handleCorruptWAL(name, err, records.Offset())
		}
		if *mem == nil {
			*mem = memtable.New(db.compare, walNum)
		}
		if err := applyBatch(*mem, payload); err != nil {
			return db.handleCorruptWAL(name, err, records.Offset())
		}
		if count > 0 {
			seq, _, _ := newBatchReader(payload)
			db.lastSeq = max(db.lastSeq, seq+count-1)
		}
		if (*mem).ApproximateSize() >= db.opts.MemtableSize {
			file, err := db.writeMemtable(*mem, base.SeqNumMax)
			if err != nil {
				return false, err
			}
			edit.AddFile(0, file)
			*mem = nil
		}
	}
}

func (db *DB) handleCorruptWAL(name string, err error, offset int64) (bool, error) {
	if db.opts.BestEffortRecovery {
		db.logf(slog.LevelError, "lsmkv: best-effort recovery stops at a damaged WAL", "wal", name, "offset", offset, "err", err)
		return true, nil
	}
	return false, fmt.Errorf("lsmkv: WAL %s at offset %d: %w", name, offset, err)
}

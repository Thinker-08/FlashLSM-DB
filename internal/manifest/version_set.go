package manifest

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/record"
	"github.com/Thinker-08/FlashLSM-DB/vfs"
)

type Options struct {
	Dirname         string
	FS              vfs.FS
	Comparer        *base.Comparer
	MaxManifestSize int64
	Logf            func(format string, args ...any)
}

// Lock order: mu (the manifest lock), then dbMu (the DB mutex), never the reverse.
type VersionSet struct {
	mu   sync.Mutex
	opts Options
	dbMu *sync.Mutex

	current      *Version // written under mu and dbMu, read under either
	liveVersions versionList

	nextFileNum atomic.Uint64
	logNumber   atomic.Uint64
	lastSeq     uint64 // guarded by mu
	// compactPointers are guarded by dbMu.
	compactPointers [NumLevels]base.InternalKey

	manifestFileNum atomic.Uint64
	manifestFile    vfs.File
	manifestWriter  *record.Writer
	needNewManifest bool
	manifestFailed  bool
}

func New(opts Options, dbMu *sync.Mutex) *VersionSet {
	vs := &VersionSet{opts: opts, dbMu: dbMu, needNewManifest: true}
	vs.liveVersions.init()
	vs.nextFileNum.Store(1)
	vs.install(&Version{})
	return vs
}

func (vs *VersionSet) logf(format string, args ...any) {
	if vs.opts.Logf != nil {
		vs.opts.Logf(format, args...)
	}
}

func (vs *VersionSet) Lock() { vs.mu.Lock() }

func (vs *VersionSet) Unlock() { vs.mu.Unlock() }

func (vs *VersionSet) Current() *Version { return vs.current }

func (vs *VersionSet) NewFileNum() uint64 { return vs.nextFileNum.Add(1) - 1 }

func (vs *VersionSet) MarkFileNumUsed(fileNum uint64) {
	for {
		next := vs.nextFileNum.Load()
		if next > fileNum || vs.nextFileNum.CompareAndSwap(next, fileNum+1) {
			return
		}
	}
}

func (vs *VersionSet) NextFileNum() uint64 { return vs.nextFileNum.Load() }

// LogNumber returns the oldest WAL still needed.
func (vs *VersionSet) LogNumber() uint64 { return vs.logNumber.Load() }

func (vs *VersionSet) LastSeq() uint64 { return vs.lastSeq }

func (vs *VersionSet) ManifestFileNum() uint64 { return vs.manifestFileNum.Load() }

func (vs *VersionSet) CompactPointer(level int) base.InternalKey { return vs.compactPointers[level] }

func (vs *VersionSet) AddLiveFiles(live map[uint64]struct{}) {
	vs.liveVersions.mu.Lock()
	defer vs.liveVersions.mu.Unlock()
	for version := vs.liveVersions.root.next; version != &vs.liveVersions.root; version = version.next {
		for _, files := range version.Levels {
			for _, file := range files {
				live[file.FileNum] = struct{}{}
			}
		}
	}
}

func (vs *VersionSet) LiveVersions() int {
	vs.liveVersions.mu.Lock()
	defer vs.liveVersions.mu.Unlock()
	count := 0
	for version := vs.liveVersions.root.next; version != &vs.liveVersions.root; version = version.next {
		count++
	}
	return count
}

func (vs *VersionSet) install(version *Version) {
	version.Ref()
	vs.liveVersions.mu.Lock()
	vs.liveVersions.pushBack(version)
	vs.liveVersions.mu.Unlock()
	previous := vs.current
	vs.current = version
	if previous != nil {
		previous.Unref()
	}
}

// LogAndApply needs the manifest lock but not the DB mutex. Callers treat an
// error as sticky, since the MANIFEST's tail is then unknown.
func (vs *VersionSet) LogAndApply(edit *VersionEdit, onInstall func(version *Version)) error {
	if !edit.HasLogNumber || edit.LogNumber < vs.logNumber.Load() {
		edit.SetLogNumber(vs.logNumber.Load())
	}
	if !edit.HasLastSeq || edit.LastSeq < vs.lastSeq {
		edit.SetLastSeq(vs.lastSeq)
	}
	for _, added := range edit.NewFiles {
		vs.MarkFileNumUsed(added.File.FileNum)
	}
	edit.SetNextFileNum(vs.nextFileNum.Load())

	builder := newVersionBuilder(vs.opts.Comparer.Compare, vs.current)
	if err := builder.apply(edit); err != nil {
		return err
	}
	version, err := builder.save()
	if err != nil {
		return err
	}

	if vs.manifestWriter == nil || vs.needNewManifest || vs.manifestFailed ||
		(vs.opts.MaxManifestSize > 0 && vs.manifestWriter.Size() >= vs.opts.MaxManifestSize) {
		err = vs.writeNewManifest(version, edit)
	} else {
		err = vs.manifestWriter.WriteRecord(edit.Encode(nil))
		if err == nil {
			err = vs.manifestWriter.Sync()
		}
	}
	if err != nil {
		vs.manifestFailed = true
		return err
	}

	vs.dbMu.Lock()
	vs.install(version)
	vs.logNumber.Store(edit.LogNumber)
	vs.lastSeq = edit.LastSeq
	for _, pointer := range edit.CompactPointers {
		vs.compactPointers[pointer.Level] = pointer.Key
	}
	if onInstall != nil {
		onInstall(version)
	}
	vs.dbMu.Unlock()
	return nil
}

func (vs *VersionSet) ForceNewManifest() { vs.needNewManifest = true }

// Sequence: sync the new MANIFEST and the dir, write and sync CURRENT.tmp, rename
// it over CURRENT, sync the dir. CURRENT thus never names an incomplete MANIFEST.
func (vs *VersionSet) writeNewManifest(version *Version, edit *VersionEdit) error {
	fs, dir := vs.opts.FS, vs.opts.Dirname
	manifestNum := vs.NewFileNum()
	path := base.MakeFilepath(dir, base.FileTypeManifest, manifestNum)
	manifestFile, err := fs.Create(path)
	if err != nil {
		return err
	}
	writer := record.NewWriter(manifestFile)
	snapshot := VersionEdit{ComparerName: vs.opts.Comparer.Name}
	snapshot.SetLogNumber(edit.LogNumber)
	snapshot.SetNextFileNum(vs.nextFileNum.Load())
	snapshot.SetLastSeq(edit.LastSeq)
	pointers := vs.compactPointers
	for _, pointer := range edit.CompactPointers {
		pointers[pointer.Level] = pointer.Key
	}
	for level, key := range pointers {
		if key.UserKey != nil {
			snapshot.SetCompactPointer(level, key)
		}
	}
	for level, files := range version.Levels {
		for _, file := range files {
			snapshot.AddFile(level, file)
		}
	}
	err = writer.WriteRecord(snapshot.Encode(nil))
	if err == nil {
		err = writer.Sync()
	}
	if err == nil {
		err = fs.SyncDir(dir)
	}
	renamed := false
	if err == nil {
		renamed, err = setCurrentFile(fs, dir, manifestNum)
	}
	if err != nil {
		manifestFile.Close()
		// Once CURRENT may name the new MANIFEST, removing it would leave CURRENT dangling.
		if !renamed {
			fs.Remove(path)
		}
		return err
	}
	if vs.manifestFile != nil {
		vs.manifestFile.Close()
	}
	vs.manifestFile, vs.manifestWriter = manifestFile, writer
	vs.manifestFileNum.Store(manifestNum)
	vs.needNewManifest, vs.manifestFailed = false, false
	return nil
}

func setCurrentFile(fs vfs.FS, dir string, manifestNum uint64) (renamed bool, err error) {
	tmpPath := filepath.Join(dir, base.CurrentTmpFilename)
	file, err := fs.Create(tmpPath)
	if err != nil {
		return false, err
	}
	_, err = file.Write([]byte(base.MakeFilename(base.FileTypeManifest, manifestNum) + "\n"))
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = fs.Rename(tmpPath, filepath.Join(dir, base.CurrentFilename))
	}
	if err != nil {
		return false, err
	}
	return true, fs.SyncDir(dir)
}

func ReadCurrent(fs vfs.FS, dir string) (uint64, error) {
	file, err := fs.Open(filepath.Join(dir, base.CurrentFilename))
	if err != nil {
		return 0, err
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, 256))
	if err != nil {
		return 0, err
	}
	name, hasNewline := strings.CutSuffix(string(contents), "\n")
	fileType, manifestNum, parsed := base.ParseFilename(name)
	if !hasNewline || !parsed || fileType != base.FileTypeManifest {
		return 0, base.CorruptionErrorf("CURRENT holds %q, not a MANIFEST name", contents)
	}
	return manifestNum, nil
}

// Replay stops at a damaged record; a crash can only tear the last, which nothing
// depends on until synced. Deeper damage loses edits and surfaces as missing files.
func (vs *VersionSet) Load() error {
	fs, dir := vs.opts.FS, vs.opts.Dirname
	manifestNum, err := ReadCurrent(fs, dir)
	if err != nil {
		return err
	}
	path := base.MakeFilepath(dir, base.FileTypeManifest, manifestNum)
	file, err := fs.Open(path)
	if err != nil {
		if vfs.IsNotExist(err) {
			return base.CorruptionErrorf("CURRENT names %s, which does not exist", filepath.Base(path))
		}
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}

	var (
		builder                      = newVersionBuilder(vs.opts.Comparer.Compare, nil)
		logNum, nextFileNum, lastSeq uint64
		haveLogNum, haveNextFileNum  bool
		haveLastSeq, haveComparer    bool
		pointers                     [NumLevels]base.InternalKey
	)
	records := record.NewReader(file, info.Size())
	for {
		payload, err := records.Next()
		if err == io.EOF {
			break
		}
		var recordErr *record.Error
		if errors.As(err, &recordErr) {
			vs.logf("manifest %s: ignoring damaged tail: %v", filepath.Base(path), recordErr)
			break
		}
		if err != nil {
			return err
		}
		var edit VersionEdit
		if err := edit.Decode(payload); err != nil {
			return fmt.Errorf("manifest %s: record at offset %d: %w", filepath.Base(path), records.Offset(), err)
		}
		if edit.ComparerName != "" {
			if edit.ComparerName != vs.opts.Comparer.Name {
				return fmt.Errorf("lsmkv: database uses comparer %q, but the options name %q", edit.ComparerName, vs.opts.Comparer.Name)
			}
			haveComparer = true
		}
		if err := builder.apply(&edit); err != nil {
			return fmt.Errorf("manifest %s: %w", filepath.Base(path), err)
		}
		if edit.HasLogNumber {
			logNum, haveLogNum = edit.LogNumber, true
		}
		if edit.HasNextFileNum {
			nextFileNum, haveNextFileNum = edit.NextFileNum, true
		}
		if edit.HasLastSeq {
			lastSeq, haveLastSeq = edit.LastSeq, true
		}
		for _, pointer := range edit.CompactPointers {
			pointers[pointer.Level] = pointer.Key
		}
	}
	if !haveComparer || !haveLogNum || !haveNextFileNum || !haveLastSeq {
		return base.CorruptionErrorf("manifest %s lacks a comparer, log number, next file number or last sequence number", filepath.Base(path))
	}
	version, err := builder.save()
	if err != nil {
		return err
	}
	vs.install(version)
	vs.nextFileNum.Store(nextFileNum)
	vs.MarkFileNumUsed(manifestNum)
	vs.logNumber.Store(logNum)
	vs.lastSeq = lastSeq
	vs.compactPointers = pointers
	vs.manifestFileNum.Store(manifestNum)
	vs.needNewManifest = true
	return nil
}

func (vs *VersionSet) Close() error {
	if vs.manifestFile == nil {
		return nil
	}
	err := vs.manifestFile.Close()
	vs.manifestFile, vs.manifestWriter = nil, nil
	return err
}

func WriteSnapshot(fs vfs.FS, dir string, comparer *base.Comparer, version *Version, manifestNum, logNum, nextFileNum, lastSeq uint64) error {
	path := base.MakeFilepath(dir, base.FileTypeManifest, manifestNum)
	manifestFile, err := fs.Create(path)
	if err != nil {
		return err
	}
	snapshot := VersionEdit{ComparerName: comparer.Name}
	snapshot.SetLogNumber(logNum)
	snapshot.SetNextFileNum(nextFileNum)
	snapshot.SetLastSeq(lastSeq)
	for level, files := range version.Levels {
		for _, file := range files {
			snapshot.AddFile(level, file)
		}
	}
	writer := record.NewWriter(manifestFile)
	err = writer.WriteRecord(snapshot.Encode(nil))
	if err == nil {
		err = writer.Sync()
	}
	if closeErr := manifestFile.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = fs.SyncDir(dir)
	}
	if err == nil {
		_, err = setCurrentFile(fs, dir, manifestNum)
	}
	return err
}

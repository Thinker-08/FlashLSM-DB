package vfs

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MemFS is an in-memory FS, safe for concurrent use. A crash image keeps every
// directory, but only synced bytes of files under names a SyncDir made durable.
type MemFS struct {
	// File writes hold mu shared, so crash images (taken exclusively) are atomic.
	mu    sync.RWMutex
	dirs  map[string]*memDir
	locks map[string]bool

	mutatingOps atomic.Int64
	crashAt     atomic.Int64
	crashMu     sync.Mutex
	crashRand   *rand.Rand
	crashImage  *MemFS
}

type memDir struct {
	files   map[string]*memNode
	durable map[string]*memNode
}

type memNode struct {
	mu      sync.Mutex
	data    []byte
	synced  int
	modTime time.Time
}

var _ FS = (*MemFS)(nil)

func NewMem() *MemFS {
	fs := &MemFS{dirs: make(map[string]*memDir), locks: make(map[string]bool)}
	fs.dirs["."] = newMemDir()
	fs.dirs["/"] = newMemDir()
	return fs
}

func newMemDir() *memDir {
	return &memDir{files: make(map[string]*memNode), durable: make(map[string]*memNode)}
}

func cleanPath(name string) string { return filepath.Clean(name) }

func splitPath(name string) (string, string) {
	name = cleanPath(name)
	return filepath.Dir(name), filepath.Base(name)
}

func pathError(op, path string, err error) error {
	return &os.PathError{Op: op, Path: path, Err: err}
}

// beforeMutate must be called without fs.mu held. It publishes the image before
// releasing fs.mu so no op blocked behind it can finish while CrashImage is nil.
func (fs *MemFS) beforeMutate() {
	op := fs.mutatingOps.Add(1)
	if crashAt := fs.crashAt.Load(); crashAt != 0 && op == crashAt {
		fs.mu.Lock()
		fs.crashMu.Lock()
		fs.crashImage = fs.imageLocked(fs.crashRand)
		fs.crashMu.Unlock()
		fs.mu.Unlock()
	}
}

// MutatingOps counts every create, remove, rename, link, mkdir, SyncDir, Write and Sync.
func (fs *MemFS) MutatingOps() int64 { return fs.mutatingOps.Load() }

// SetCrashPoint takes a crash image before mutating op number nthOp from now, torn if random != nil.
func (fs *MemFS) SetCrashPoint(nthOp int64, random *rand.Rand) {
	if nthOp < 1 {
		panic("vfs: crash point must be at least 1")
	}
	fs.crashMu.Lock()
	fs.crashRand = random
	fs.crashImage = nil
	fs.crashMu.Unlock()
	fs.crashAt.Store(fs.mutatingOps.Load() + nthOp)
}

// CrashImage returns the SetCrashPoint image, or nil until the point is reached.
func (fs *MemFS) CrashImage() *MemFS {
	fs.crashMu.Lock()
	defer fs.crashMu.Unlock()
	return fs.crashImage
}

// Crash returns a copy holding only durable state; the receiver keeps working.
func (fs *MemFS) Crash() *MemFS {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.imageLocked(nil)
}

// CrashTorn is Crash plus a random, possibly garbled, prefix of each unsynced tail.
func (fs *MemFS) CrashTorn(random *rand.Rand) *MemFS {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.imageLocked(random)
}

func (fs *MemFS) imageLocked(random *rand.Rand) *MemFS {
	image := NewMem()
	copied := make(map[*memNode]*memNode)
	for path, dir := range fs.dirs {
		imageDir := newMemDir()
		for name, node := range dir.durable {
			nodeCopy, ok := copied[node]
			if !ok {
				nodeCopy = node.crashCopy(random)
				copied[node] = nodeCopy
			}
			imageDir.files[name] = nodeCopy
			imageDir.durable[name] = nodeCopy
		}
		image.dirs[path] = imageDir
	}
	return image
}

func (n *memNode) crashCopy(random *rand.Rand) *memNode {
	n.mu.Lock()
	defer n.mu.Unlock()
	keep := n.synced
	if random != nil && len(n.data) > n.synced {
		keep += random.IntN(len(n.data) - n.synced + 1)
	}
	data := make([]byte, keep)
	copy(data, n.data[:keep])
	if tail := data[n.synced:]; random != nil && len(tail) > 0 {
		switch random.IntN(3) {
		case 1:
			clear(tail[random.IntN(len(tail)):])
		case 2:
			for i := random.IntN(3); i >= 0; i-- {
				tail[random.IntN(len(tail))] ^= 1 << random.IntN(8)
			}
		}
	}
	return &memNode{data: data, synced: len(data), modTime: n.modTime}
}

func (fs *MemFS) lookupDir(dirPath string) (*memDir, bool) {
	dir, ok := fs.dirs[dirPath]
	return dir, ok
}

func (fs *MemFS) Create(name string) (File, error) {
	fs.beforeMutate()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	dirPath, fileName := splitPath(name)
	dir, ok := fs.lookupDir(dirPath)
	if !ok {
		return nil, pathError("create", name, os.ErrNotExist)
	}
	if _, isDir := fs.dirs[cleanPath(name)]; isDir {
		return nil, pathError("create", name, errors.New("is a directory"))
	}
	node := &memNode{modTime: time.Now()}
	dir.files[fileName] = node
	return &memFile{fs: fs, node: node, name: name}, nil
}

func (fs *MemFS) Open(name string) (File, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	dirPath, fileName := splitPath(name)
	dir, ok := fs.lookupDir(dirPath)
	if !ok {
		return nil, pathError("open", name, os.ErrNotExist)
	}
	node, ok := dir.files[fileName]
	if !ok {
		return nil, pathError("open", name, os.ErrNotExist)
	}
	return &memFile{fs: fs, node: node, name: name, readOnly: true}, nil
}

func (fs *MemFS) Remove(name string) error {
	fs.beforeMutate()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	dirPath, fileName := splitPath(name)
	dir, ok := fs.lookupDir(dirPath)
	if !ok {
		return pathError("remove", name, os.ErrNotExist)
	}
	if _, ok := dir.files[fileName]; !ok {
		return pathError("remove", name, os.ErrNotExist)
	}
	delete(dir.files, fileName)
	return nil
}

func (fs *MemFS) Rename(oldname, newname string) error {
	fs.beforeMutate()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	oldDirPath, oldFileName := splitPath(oldname)
	newDirPath, newFileName := splitPath(newname)
	oldDir, ok := fs.lookupDir(oldDirPath)
	if !ok {
		return pathError("rename", oldname, os.ErrNotExist)
	}
	newDir, ok := fs.lookupDir(newDirPath)
	if !ok {
		return pathError("rename", newname, os.ErrNotExist)
	}
	node, ok := oldDir.files[oldFileName]
	if !ok {
		return pathError("rename", oldname, os.ErrNotExist)
	}
	delete(oldDir.files, oldFileName)
	newDir.files[newFileName] = node
	return nil
}

func (fs *MemFS) Link(oldname, newname string) error {
	fs.beforeMutate()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	oldDirPath, oldFileName := splitPath(oldname)
	newDirPath, newFileName := splitPath(newname)
	oldDir, ok := fs.lookupDir(oldDirPath)
	if !ok {
		return pathError("link", oldname, os.ErrNotExist)
	}
	newDir, ok := fs.lookupDir(newDirPath)
	if !ok {
		return pathError("link", newname, os.ErrNotExist)
	}
	node, ok := oldDir.files[oldFileName]
	if !ok {
		return pathError("link", oldname, os.ErrNotExist)
	}
	if _, exists := newDir.files[newFileName]; exists {
		return pathError("link", newname, os.ErrExist)
	}
	newDir.files[newFileName] = node
	return nil
}

func (fs *MemFS) MkdirAll(dirPath string, _ os.FileMode) error {
	fs.beforeMutate()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for path := cleanPath(dirPath); ; path = filepath.Dir(path) {
		if _, ok := fs.dirs[path]; ok {
			break
		}
		parentPath, name := filepath.Dir(path), filepath.Base(path)
		if parentDir, ok := fs.dirs[parentPath]; ok {
			if _, isFile := parentDir.files[name]; isFile {
				return pathError("mkdir", path, errors.New("not a directory"))
			}
		}
		fs.dirs[path] = newMemDir()
		if path == parentPath {
			break
		}
	}
	return nil
}

func (fs *MemFS) List(dirPath string) ([]string, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	dirPath = cleanPath(dirPath)
	dir, ok := fs.lookupDir(dirPath)
	if !ok {
		return nil, pathError("readdir", dirPath, os.ErrNotExist)
	}
	names := make([]string, 0, len(dir.files))
	for name := range dir.files {
		names = append(names, name)
	}
	for path := range fs.dirs {
		if path != dirPath && filepath.Dir(path) == dirPath {
			names = append(names, filepath.Base(path))
		}
	}
	sort.Strings(names)
	return names, nil
}

func (fs *MemFS) Stat(name string) (os.FileInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	cleanName := cleanPath(name)
	if _, ok := fs.dirs[cleanName]; ok {
		return &memFileInfo{name: filepath.Base(cleanName), isDir: true}, nil
	}
	dirPath, fileName := splitPath(cleanName)
	dir, ok := fs.lookupDir(dirPath)
	if !ok {
		return nil, pathError("stat", name, os.ErrNotExist)
	}
	node, ok := dir.files[fileName]
	if !ok {
		return nil, pathError("stat", name, os.ErrNotExist)
	}
	return node.info(fileName), nil
}

func (fs *MemFS) Lock(name string) (io.Closer, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cleanName := cleanPath(name)
	if fs.locks[cleanName] {
		return nil, fmt.Errorf("lock %s: %w", name, ErrLocked)
	}
	dirPath, fileName := splitPath(cleanName)
	dir, ok := fs.lookupDir(dirPath)
	if !ok {
		return nil, pathError("lock", name, os.ErrNotExist)
	}
	if _, ok := dir.files[fileName]; !ok {
		dir.files[fileName] = &memNode{modTime: time.Now()}
	}
	fs.locks[cleanName] = true
	return &memLock{fs: fs, path: cleanName}, nil
}

type memLock struct {
	fs   *MemFS
	path string
	once sync.Once
}

func (ml *memLock) Close() error {
	ml.once.Do(func() {
		ml.fs.mu.Lock()
		delete(ml.fs.locks, ml.path)
		ml.fs.mu.Unlock()
	})
	return nil
}

func (fs *MemFS) SyncDir(dirPath string) error {
	fs.beforeMutate()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	dir, ok := fs.lookupDir(cleanPath(dirPath))
	if !ok {
		return pathError("syncdir", dirPath, os.ErrNotExist)
	}
	dir.durable = maps.Clone(dir.files)
	return nil
}

// Mutate replaces a file's contents, durable bytes included, with transform(contents).
func (fs *MemFS) Mutate(name string, transform func(data []byte) []byte) error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	dirPath, fileName := splitPath(name)
	dir, ok := fs.lookupDir(dirPath)
	if !ok {
		return pathError("mutate", name, os.ErrNotExist)
	}
	node, ok := dir.files[fileName]
	if !ok {
		return pathError("mutate", name, os.ErrNotExist)
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	node.data = transform(node.data)
	node.synced = min(node.synced, len(node.data))
	return nil
}

func (fs *MemFS) String() string {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	paths := make([]string, 0, len(fs.dirs))
	for path := range fs.dirs {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var out strings.Builder
	for _, path := range paths {
		dir := fs.dirs[path]
		names := make([]string, 0, len(dir.files))
		for name := range dir.files {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			node := dir.files[name]
			node.mu.Lock()
			note := ""
			if dir.durable[name] != node {
				note = " (name not durable)"
			}
			fmt.Fprintf(&out, "%s: %d bytes, %d synced%s\n", filepath.Join(path, name), len(node.data), node.synced, note)
			node.mu.Unlock()
		}
	}
	return out.String()
}

func (n *memNode) info(name string) *memFileInfo {
	n.mu.Lock()
	defer n.mu.Unlock()
	return &memFileInfo{name: name, size: int64(len(n.data)), modTime: n.modTime}
}

type memFile struct {
	fs       *MemFS
	node     *memNode
	name     string
	readOnly bool
	position int64
	closed   atomic.Bool
}

func (f *memFile) Read(p []byte) (int, error) {
	if f.closed.Load() {
		return 0, pathError("read", f.name, os.ErrClosed)
	}
	n, err := f.ReadAt(p, f.position)
	f.position += int64(n)
	if err == io.EOF && n > 0 {
		err = nil
	}
	return n, err
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	if f.closed.Load() {
		return 0, pathError("read", f.name, os.ErrClosed)
	}
	if off < 0 {
		return 0, pathError("read", f.name, errors.New("negative offset"))
	}
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	if off >= int64(len(f.node.data)) {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n := copy(p, f.node.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	if f.closed.Load() {
		return 0, pathError("write", f.name, os.ErrClosed)
	}
	if f.readOnly {
		return 0, pathError("write", f.name, errors.New("file is read-only"))
	}
	f.fs.beforeMutate()
	f.fs.mu.RLock()
	defer f.fs.mu.RUnlock()
	f.node.mu.Lock()
	f.node.data = append(f.node.data, p...)
	f.node.modTime = time.Now()
	f.node.mu.Unlock()
	return len(p), nil
}

func (f *memFile) Sync() error {
	if f.closed.Load() {
		return pathError("sync", f.name, os.ErrClosed)
	}
	f.fs.beforeMutate()
	f.fs.mu.RLock()
	defer f.fs.mu.RUnlock()
	f.node.mu.Lock()
	f.node.synced = len(f.node.data)
	f.node.mu.Unlock()
	return nil
}

func (f *memFile) Stat() (os.FileInfo, error) {
	if f.closed.Load() {
		return nil, pathError("stat", f.name, os.ErrClosed)
	}
	return f.node.info(filepath.Base(f.name)), nil
}

func (f *memFile) Close() error {
	if f.closed.Swap(true) {
		return pathError("close", f.name, os.ErrClosed)
	}
	return nil
}

type memFileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (fi *memFileInfo) Name() string       { return fi.name }
func (fi *memFileInfo) Size() int64        { return fi.size }
func (fi *memFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *memFileInfo) IsDir() bool        { return fi.isDir }
func (fi *memFileInfo) Sys() any           { return nil }
func (fi *memFileInfo) Mode() os.FileMode {
	if fi.isDir {
		return os.ModeDir | 0o755
	}
	return 0o644
}

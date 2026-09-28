package vfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var Default FS = osFS{}

type osFS struct{}

func (osFS) Create(name string) (File, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (osFS) Open(name string) (File, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (osFS) Remove(name string) error                    { return os.Remove(name) }
func (osFS) Rename(oldname, newname string) error        { return os.Rename(oldname, newname) }
func (osFS) Link(oldname, newname string) error          { return os.Link(oldname, newname) }
func (osFS) MkdirAll(dir string, perm os.FileMode) error { return os.MkdirAll(dir, perm) }
func (osFS) Stat(name string) (os.FileInfo, error)       { return os.Stat(name) }

func (osFS) List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	sort.Strings(names)
	return names, nil
}

func (osFS) SyncDir(dir string) error {
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = syncDirHandle(dirFile)
	if closeErr := dirFile.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (osFS) Lock(name string) (io.Closer, error) {
	absPath, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	// The in-process set makes a second Lock fail even where file locks would not.
	if !processLocks.acquire(absPath) {
		return nil, fmt.Errorf("lock %s: %w (held by this process)", name, ErrLocked)
	}
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		processLocks.release(absPath)
		return nil, err
	}
	if err := lockFile(file); err != nil {
		file.Close()
		processLocks.release(absPath)
		return nil, fmt.Errorf("lock %s: %w: %v", name, ErrLocked, err)
	}
	return &osLock{file: file, path: absPath}, nil
}

type osLock struct {
	once sync.Once
	file *os.File
	path string
}

func (ol *osLock) Close() error {
	var err error
	ol.once.Do(func() {
		err = unlockFile(ol.file)
		if closeErr := ol.file.Close(); err == nil {
			err = closeErr
		}
		processLocks.release(ol.path)
	})
	return err
}

type lockSet struct {
	mu    sync.Mutex
	paths map[string]bool
}

var processLocks = &lockSet{paths: make(map[string]bool)}

func (s *lockSet) acquire(path string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.paths[path] {
		return false
	}
	s.paths[path] = true
	return true
}

func (s *lockSet) release(path string) {
	s.mu.Lock()
	delete(s.paths, path)
	s.mu.Unlock()
}

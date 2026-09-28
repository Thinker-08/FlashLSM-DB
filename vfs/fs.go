package vfs

import (
	"errors"
	"io"
	"os"
)

type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Closer
	Sync() error
	Stat() (os.FileInfo, error)
}

type FS interface {
	Create(name string) (File, error)
	Open(name string) (File, error)
	Remove(name string) error
	Rename(oldname, newname string) error
	Link(oldname, newname string) error
	MkdirAll(dir string, perm os.FileMode) error
	List(dir string) ([]string, error)
	Stat(name string) (os.FileInfo, error)
	// Lock creates and locks name. A second Lock fails with ErrLocked, even in-process.
	Lock(name string) (io.Closer, error)
	// SyncDir makes the creates, renames and removes in dir durable.
	SyncDir(dir string) error
}

var ErrLocked = errors.New("vfs: lock is held by another user")

func IsNotExist(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}

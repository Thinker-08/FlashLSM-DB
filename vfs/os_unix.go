//go:build unix

package vfs

import (
	"os"
	"syscall"
)

func lockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func unlockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

// On macOS, File.Sync issues F_FULLFSYNC, which APFS supports on directories.
func syncDirHandle(dir *os.File) error {
	return dir.Sync()
}

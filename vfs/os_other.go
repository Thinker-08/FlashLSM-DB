//go:build !unix

package vfs

import "os"

// Unsupported platforms get no cross-process locking, only the in-process lock set.
func lockFile(*os.File) error   { return nil }
func unlockFile(*os.File) error { return nil }

func syncDirHandle(dir *os.File) error {
	// Windows cannot sync directory handles; renames there are durable on return.
	return nil
}

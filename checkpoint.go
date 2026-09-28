package lsmkv

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
	"github.com/Thinker-08/FlashLSM-DB/vfs"
)

// Checkpoint writes a copy holding every write made before the call into destDir, which must be
// empty or not exist. Tables are hard-linked when possible.
func (db *DB) Checkpoint(destDir string) error {
	if err := db.Flush(); err != nil {
		return err
	}
	db.mu.Lock()
	version := db.versions.Current()
	version.Ref()
	seq := db.visibleSeq.Load()
	db.mu.Unlock()
	defer version.Unref()

	if err := db.fs.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	if names, err := db.fs.List(destDir); err != nil {
		return err
	} else if len(names) > 0 {
		return fmt.Errorf("lsmkv: checkpoint directory %s is not empty", destDir)
	}
	var maxFileNum uint64
	for _, files := range version.Levels {
		for _, file := range files {
			name := base.MakeFilename(base.FileTypeTable, file.FileNum)
			srcPath, dstPath := filepath.Join(db.dirname, name), filepath.Join(destDir, name)
			if err := db.fs.Link(srcPath, dstPath); err != nil {
				if err := copyFile(db.fs, srcPath, dstPath); err != nil {
					return fmt.Errorf("lsmkv: checkpoint %s: %w", name, err)
				}
			}
			maxFileNum = max(maxFileNum, file.FileNum)
		}
	}
	// The copy starts with no WAL; its log number names the next one.
	return manifest.WriteSnapshot(db.fs, destDir, db.opts.Comparer, version, maxFileNum+1, maxFileNum+2, maxFileNum+3, seq)
}

func copyFile(fs vfs.FS, srcPath, dstPath string) error {
	srcFile, err := fs.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	dstFile, err := fs.Create(dstPath)
	if err != nil {
		return err
	}
	_, err = io.Copy(dstFile, srcFile)
	if err == nil {
		err = dstFile.Sync()
	}
	if closeErr := dstFile.Close(); err == nil {
		err = closeErr
	}
	return err
}

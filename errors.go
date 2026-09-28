package lsmkv

import (
	"errors"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/vfs"
)

var (
	ErrNotFound = base.ErrNotFound
	// ErrCorruption is wrapped by errors that name the damaged file and offset.
	ErrCorruption = base.ErrCorruption
	ErrClosed     = errors.New("lsmkv: database is closed")
	// ErrLocked means another DB, in this process or another, has the directory open.
	ErrLocked        = vfs.ErrLocked
	ErrExists        = errors.New("lsmkv: database already exists")
	ErrDoesNotExist  = errors.New("lsmkv: database does not exist")
	ErrKeyTooLarge   = errors.New("lsmkv: key exceeds the 64 KiB limit")
	ErrBatchTooLarge = errors.New("lsmkv: batch exceeds the 64 MiB limit")
	// ErrNoMerger means a merge operand was written or read without Options.Merger.
	ErrNoMerger = errors.New("lsmkv: merge operands need Options.Merger")
	// ErrInvalidOptions wraps every option validation failure.
	ErrInvalidOptions = errors.New("lsmkv: invalid options")
)

package lsmkv

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
	"github.com/Thinker-08/FlashLSM-DB/internal/sstable"
	"github.com/Thinker-08/FlashLSM-DB/vfs"
)

// Comparer orders keys; its Name is recorded, and reopening with a different Name fails.
type Comparer = base.Comparer

// DefaultComparer orders keys like bytes.Compare.
var DefaultComparer = base.DefaultComparer

type Compression = sstable.Compression

const (
	NoCompression     = sstable.NoCompression
	SnappyCompression = sstable.SnappyCompression
	ZstdCompression   = sstable.ZstdCompression
)

type CompactionStrategy int

const (
	LeveledCompaction CompactionStrategy = iota
	SizeTieredCompaction
)

func (s CompactionStrategy) String() string {
	switch s {
	case LeveledCompaction:
		return "leveled"
	case SizeTieredCompaction:
		return "size-tiered"
	}
	return fmt.Sprintf("strategy(%d)", int(s))
}

type Merger struct {
	Name string
	// FullMerge applies operands, oldest first, to existing; it must not retain or modify its
	// arguments. Compaction merges partial histories, so it must be deterministic and associative.
	FullMerge func(key, existing []byte, exists bool, operands [][]byte) ([]byte, error)
}

type SizeTieredOptions struct {
	MinMergeWidth int
	// SizeRatio caps a merge's largest run at SizeRatio times its smallest.
	SizeRatio float64
	// MaxSizeAmplificationPercent merges all runs once newer runs total this % of the oldest.
	MaxSizeAmplificationPercent int
}

// Options should start from DefaultOptions, since zero numeric fields are taken literally.
type Options struct {
	FS              vfs.FS
	CreateIfMissing bool
	ErrorIfExists   bool
	Comparer        *Comparer

	MemtableSize          int64
	MaxImmutableMemtables int

	BlockSize            int
	BlockRestartInterval int
	// BloomBitsPerKey of 0 disables bloom filters.
	BloomBitsPerKey int
	Compression     Compression

	TargetFileSize      int64
	L0CompactionTrigger int
	// L0SlowdownWritesTrigger is the L0 file count at which each write is delayed once by 1 ms.
	L0SlowdownWritesTrigger int
	L0StopWritesTrigger     int
	// NumLevels is at most 7.
	NumLevels          int
	L1MaxBytes         int64
	LevelMultiplier    int
	CompactionStrategy CompactionStrategy
	SizeTiered         SizeTieredOptions
	// DisableAutomaticCompactions keeps flushes and CompactRange; writes then never stall on L0.
	DisableAutomaticCompactions bool

	// BlockCacheSize of 0 disables the block cache.
	BlockCacheSize      int64
	MaxOpenFiles        int
	MaxManifestFileSize int64

	Merger *Merger
	// CompactionFilter must be deterministic. It runs on each key's newest version, and
	// only while no snapshot is open.
	CompactionFilter func(level int, key, value []byte) (remove bool)
	// Clock is used to expire TTL entries; nil means time.Now.
	Clock func() time.Time

	// BestEffortRecovery lets Open keep the history before damage in an older WAL.
	BestEffortRecovery bool
	// ParanoidChecks periodically verifies tree invariants and makes Close fail while an
	// iterator or snapshot is still open.
	ParanoidChecks bool
	// Logger of nil disables logging.
	Logger *slog.Logger
}

func DefaultOptions() *Options {
	return &Options{
		FS:                      vfs.Default,
		CreateIfMissing:         true,
		Comparer:                DefaultComparer,
		MemtableSize:            4 << 20,
		MaxImmutableMemtables:   1,
		BlockSize:               4 << 10,
		BlockRestartInterval:    16,
		BloomBitsPerKey:         10,
		Compression:             NoCompression,
		TargetFileSize:          2 << 20,
		L0CompactionTrigger:     4,
		L0SlowdownWritesTrigger: 8,
		L0StopWritesTrigger:     12,
		NumLevels:               manifest.NumLevels,
		L1MaxBytes:              10 << 20,
		LevelMultiplier:         10,
		CompactionStrategy:      LeveledCompaction,
		SizeTiered: SizeTieredOptions{
			MinMergeWidth:               4,
			SizeRatio:                   1.5,
			MaxSizeAmplificationPercent: 200,
		},
		BlockCacheSize:      8 << 20,
		MaxOpenFiles:        1000,
		MaxManifestFileSize: 64 << 20,
	}
}

type ReadOptions struct {
	Snapshot *Snapshot
	// LowerBound is an inclusive iterator bound.
	LowerBound []byte
	// UpperBound is an exclusive iterator bound.
	UpperBound    []byte
	DontFillCache bool
}

type WriteOptions struct {
	// Sync makes the write survive power loss, not just a process crash.
	Sync bool
}

var Sync = &WriteOptions{Sync: true}

var NoSync = &WriteOptions{}

func (o *Options) validate() (*Options, error) {
	var validated Options
	if o == nil {
		validated = *DefaultOptions()
	} else {
		validated = *o
	}
	if validated.FS == nil {
		validated.FS = vfs.Default
	}
	if validated.Comparer == nil {
		validated.Comparer = DefaultComparer
	}
	if validated.Clock == nil {
		validated.Clock = time.Now
	}
	invalid := func(format string, args ...any) (*Options, error) {
		return nil, fmt.Errorf("%w: "+format, append([]any{ErrInvalidOptions}, args...)...)
	}
	switch {
	case validated.Comparer.Compare == nil || validated.Comparer.Name == "":
		return invalid("Comparer needs a Name and a Compare function")
	case validated.MemtableSize < 4<<10 || validated.MemtableSize > 1<<32:
		return invalid("MemtableSize %d is outside [4 KiB, 4 GiB]", validated.MemtableSize)
	case validated.MaxImmutableMemtables < 1:
		return invalid("MaxImmutableMemtables %d is below 1", validated.MaxImmutableMemtables)
	case validated.BlockSize < 128 || validated.BlockSize > 64<<20:
		return invalid("BlockSize %d is outside [128 B, 64 MiB]", validated.BlockSize)
	case validated.BlockRestartInterval < 1:
		return invalid("BlockRestartInterval %d is below 1", validated.BlockRestartInterval)
	case validated.BloomBitsPerKey < 0 || validated.BloomBitsPerKey > 64:
		return invalid("BloomBitsPerKey %d is outside [0, 64]", validated.BloomBitsPerKey)
	case validated.Compression > ZstdCompression:
		return invalid("unknown Compression %d", validated.Compression)
	case validated.TargetFileSize < int64(validated.BlockSize):
		return invalid("TargetFileSize %d is below BlockSize %d", validated.TargetFileSize, validated.BlockSize)
	case validated.L0CompactionTrigger < 1 || validated.L0SlowdownWritesTrigger < validated.L0CompactionTrigger || validated.L0StopWritesTrigger < validated.L0SlowdownWritesTrigger:
		return invalid("L0 triggers must satisfy 1 <= compaction (%d) <= slowdown (%d) <= stop (%d)",
			validated.L0CompactionTrigger, validated.L0SlowdownWritesTrigger, validated.L0StopWritesTrigger)
	case validated.NumLevels < 2 || validated.NumLevels > manifest.NumLevels:
		return invalid("NumLevels %d is outside [2, %d]", validated.NumLevels, manifest.NumLevels)
	case validated.L1MaxBytes < 1:
		return invalid("L1MaxBytes %d is below 1", validated.L1MaxBytes)
	case validated.LevelMultiplier < 2:
		return invalid("LevelMultiplier %d is below 2", validated.LevelMultiplier)
	case validated.CompactionStrategy != LeveledCompaction && validated.CompactionStrategy != SizeTieredCompaction:
		return invalid("unknown CompactionStrategy %d", validated.CompactionStrategy)
	case validated.CompactionStrategy == SizeTieredCompaction &&
		(validated.SizeTiered.MinMergeWidth < 2 || validated.SizeTiered.SizeRatio < 1 || validated.SizeTiered.MaxSizeAmplificationPercent < 1):
		return invalid("SizeTiered needs MinMergeWidth >= 2, SizeRatio >= 1 and MaxSizeAmplificationPercent >= 1")
	case validated.BlockCacheSize < 0:
		return invalid("BlockCacheSize %d is negative", validated.BlockCacheSize)
	case validated.MaxOpenFiles < 1:
		return invalid("MaxOpenFiles %d is below 1", validated.MaxOpenFiles)
	case validated.MaxManifestFileSize < 1:
		return invalid("MaxManifestFileSize %d is below 1", validated.MaxManifestFileSize)
	case validated.Merger != nil && validated.Merger.FullMerge == nil:
		return invalid("Merger needs a FullMerge function")
	}
	return &validated, nil
}

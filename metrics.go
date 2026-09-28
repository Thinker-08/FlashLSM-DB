package lsmkv

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Thinker-08/FlashLSM-DB/internal/compaction"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
)

type metricCounters struct {
	userBytesWritten    atomic.Int64
	walBytesWritten     atomic.Int64
	walSyncs            atomic.Int64
	walFilesCreated     atomic.Int64
	flushes             atomic.Int64
	flushBytesWritten   atomic.Int64
	compactions         atomic.Int64
	trivialMoves        atomic.Int64
	compactBytesRead    atomic.Int64
	compactBytesWritten atomic.Int64
	levelBytesIn        [manifest.NumLevels]atomic.Int64
	stalls              atomic.Int64
	stallDuration       atomic.Int64
	filesDeleted        atomic.Int64
}

type LevelMetrics struct {
	NumFiles int
	Size     int64
	// Score of at least 1 means a compaction is due; size-tiered compaction leaves it 0.
	Score   float64
	BytesIn int64
}

type Metrics struct {
	Strategy   string
	Levels     []LevelMetrics
	SortedRuns int

	UserBytesWritten int64
	Memtable         struct {
		Size      int64
		Immutable int
	}
	WAL struct {
		FilesCreated int64
		BytesWritten int64
		Syncs        int64
	}
	Flush struct {
		Count        int64
		BytesWritten int64
	}
	Compaction struct {
		Count        int64
		TrivialMoves int64
		BytesRead    int64
		BytesWritten int64
	}
	WriteStall struct {
		Count    int64
		Duration time.Duration
	}
	BlockCache struct {
		Capacity, Size, Count, Hits, Misses int64
	}
	TableCache struct {
		Open         int
		Hits, Misses int64
	}
	Filter struct {
		Checks, Negatives int64
	}
	// DataBlocksRead counts data blocks read from disk rather than cache.
	DataBlocksRead int64
	DataBytesRead  int64
	Snapshots      struct {
		Count     int
		OldestAge time.Duration
	}
	OpenIterators int64
	FilesDeleted  int64
}

// DiskSize returns the bytes held by live tables.
func (m Metrics) DiskSize() int64 {
	var n int64
	for _, level := range m.Levels {
		n += level.Size
	}
	return n
}

func (m Metrics) WriteAmplification() float64 {
	if m.UserBytesWritten == 0 {
		return 0
	}
	return float64(m.Flush.BytesWritten+m.Compaction.BytesWritten) / float64(m.UserBytesWritten)
}

func (db *DB) Metrics() Metrics {
	var metrics Metrics
	metrics.Strategy = db.opts.CompactionStrategy.String()
	db.mu.Lock()
	version := db.versions.Current()
	version.Ref()
	metrics.Memtable.Size = db.activeMemtable.ApproximateSize()
	metrics.Memtable.Immutable = len(db.immutableMemtables)
	for _, mem := range db.immutableMemtables {
		metrics.Memtable.Size += mem.ApproximateSize()
	}
	metrics.Snapshots.Count = db.snapshots.count
	if oldest := db.snapshots.oldest(); oldest != nil {
		metrics.Snapshots.OldestAge = time.Since(oldest.created)
	}
	db.mu.Unlock()
	defer version.Unref()

	var scores [manifest.NumLevels]float64
	if leveled, ok := db.compactionPicker.(*compaction.Leveled); ok {
		scores = leveled.Scores(version)
	}
	metrics.Levels = make([]LevelMetrics, db.opts.NumLevels)
	for level := range metrics.Levels {
		metrics.Levels[level] = LevelMetrics{
			NumFiles: len(version.Levels[level]),
			Size:     int64(version.LevelSize(level)),
			Score:    scores[level],
			BytesIn:  db.counters.levelBytesIn[level].Load(),
		}
		if level == 0 {
			metrics.SortedRuns += len(version.Levels[0])
		} else if len(version.Levels[level]) > 0 {
			metrics.SortedRuns++
		}
	}

	metrics.UserBytesWritten = db.counters.userBytesWritten.Load()
	metrics.WAL.FilesCreated = db.counters.walFilesCreated.Load()
	metrics.WAL.BytesWritten = db.counters.walBytesWritten.Load()
	metrics.WAL.Syncs = db.counters.walSyncs.Load()
	metrics.Flush.Count = db.counters.flushes.Load()
	metrics.Flush.BytesWritten = db.counters.flushBytesWritten.Load()
	metrics.Compaction.Count = db.counters.compactions.Load()
	metrics.Compaction.TrivialMoves = db.counters.trivialMoves.Load()
	metrics.Compaction.BytesRead = db.counters.compactBytesRead.Load()
	metrics.Compaction.BytesWritten = db.counters.compactBytesWritten.Load()
	metrics.WriteStall.Count = db.counters.stalls.Load()
	metrics.WriteStall.Duration = time.Duration(db.counters.stallDuration.Load())
	blockCache := db.blockCache.Metrics()
	metrics.BlockCache.Capacity, metrics.BlockCache.Size, metrics.BlockCache.Count = blockCache.Capacity, blockCache.Size, blockCache.Count
	metrics.BlockCache.Hits, metrics.BlockCache.Misses = blockCache.Hits, blockCache.Misses
	metrics.TableCache.Open = db.tableCache.openCount()
	metrics.TableCache.Hits, metrics.TableCache.Misses = db.tableCache.hits.Load(), db.tableCache.misses.Load()
	metrics.Filter.Checks = db.tableStats.FilterChecks.Load()
	metrics.Filter.Negatives = db.tableStats.FilterNegatives.Load()
	metrics.DataBlocksRead = db.tableStats.BlockReads.Load()
	metrics.DataBytesRead = db.tableStats.BlockBytesRead.Load()
	metrics.OpenIterators = db.openIterators.Load()
	metrics.FilesDeleted = db.counters.filesDeleted.Load()
	return metrics
}

func (m Metrics) String() string {
	var out strings.Builder
	fmt.Fprintf(&out, "strategy %s, %d sorted runs, %s on disk\n", m.Strategy, m.SortedRuns, formatBytes(m.DiskSize()))
	fmt.Fprintf(&out, "level  files        size  score     bytes in\n")
	for level, levelMetrics := range m.Levels {
		fmt.Fprintf(&out, "L%-5d %5d %11s %6.2f %12s\n", level, levelMetrics.NumFiles, formatBytes(levelMetrics.Size), levelMetrics.Score, formatBytes(levelMetrics.BytesIn))
	}
	fmt.Fprintf(&out, "memtables: %s in %d (%d immutable)\n", formatBytes(m.Memtable.Size), m.Memtable.Immutable+1, m.Memtable.Immutable)
	fmt.Fprintf(&out, "writes: %s from users, %s to the WAL in %d files, %d syncs\n",
		formatBytes(m.UserBytesWritten), formatBytes(m.WAL.BytesWritten), m.WAL.FilesCreated, m.WAL.Syncs)
	fmt.Fprintf(&out, "flushes: %d, %s; compactions: %d (%d trivial moves), %s read, %s written\n",
		m.Flush.Count, formatBytes(m.Flush.BytesWritten), m.Compaction.Count, m.Compaction.TrivialMoves,
		formatBytes(m.Compaction.BytesRead), formatBytes(m.Compaction.BytesWritten))
	fmt.Fprintf(&out, "write amplification: %.2f; stalls: %d for %s\n", m.WriteAmplification(), m.WriteStall.Count, m.WriteStall.Duration)
	fmt.Fprintf(&out, "block cache: %s of %s, %d hits, %d misses; table cache: %d open, %d hits, %d misses\n",
		formatBytes(m.BlockCache.Size), formatBytes(m.BlockCache.Capacity), m.BlockCache.Hits, m.BlockCache.Misses,
		m.TableCache.Open, m.TableCache.Hits, m.TableCache.Misses)
	fmt.Fprintf(&out, "filters: %d checks, %d negatives; data blocks read: %d (%s)\n",
		m.Filter.Checks, m.Filter.Negatives, m.DataBlocksRead, formatBytes(m.DataBytesRead))
	fmt.Fprintf(&out, "snapshots: %d (oldest %s); open iterators: %d\n", m.Snapshots.Count, m.Snapshots.OldestAge.Round(time.Millisecond), m.OpenIterators)
	return out.String()
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	divisor, exponent := int64(unit), 0
	for value := n / unit; value >= unit; value /= unit {
		divisor *= unit
		exponent++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(divisor), "KMGTPE"[exponent])
}

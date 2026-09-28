package compaction

import (
	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
)

type Options struct {
	NumLevels               int
	L0CompactionTrigger     int
	L0SlowdownWritesTrigger int
	L1MaxBytes              uint64
	LevelMultiplier         uint64
	TargetFileSize          uint64

	TieredMinMergeWidth     int
	TieredSizeRatio         float64
	TieredMaxSizeAmpPercent uint64
}

func (o *Options) MaxGrandparentOverlap() uint64 { return 10 * o.TargetFileSize }

func (o *Options) ExpandedCompactionLimit() uint64 { return 25 * o.TargetFileSize }

func (o *Options) MaxBytesForLevel(level int) uint64 {
	maxBytes := o.L1MaxBytes
	for i := 1; i < level; i++ {
		maxBytes *= o.LevelMultiplier
	}
	return maxBytes
}

type Env struct {
	CompactPointers [manifest.NumLevels]base.InternalKey
}

type Picker interface {
	Pick(v *manifest.Version, env Env) *Compaction
}

type Input struct {
	Level int
	Files []*manifest.FileMetadata
}

type Compaction struct {
	// Inputs are ordered newest level first.
	Inputs      []Input
	OutputLevel int
	// MaxOutputFileSize of 0 writes a single output file.
	MaxOutputFileSize     uint64
	Grandparents          []*manifest.FileMetadata
	MaxGrandparentOverlap uint64
	// OlderL0 holds the L0 files beneath an output that stays in L0.
	OlderL0           []*manifest.FileMetadata
	TrivialMove       bool
	CompactPointer    *manifest.CompactPointer
	Smallest, Largest []byte
	Reason            string
	Version           *manifest.Version

	compare            base.Compare
	numLevels          int
	levelCursors       [manifest.NumLevels]int
	grandparentIndex   int
	seenFirstKey       bool
	grandparentOverlap uint64
}

func newCompaction(compare base.Compare, numLevels int, version *manifest.Version, reason string) *Compaction {
	return &Compaction{compare: compare, numLevels: numLevels, Version: version, Reason: reason}
}

func (c *Compaction) StartLevel() int { return c.Inputs[0].Level }

func (c *Compaction) InputFiles() []*manifest.FileMetadata {
	var files []*manifest.FileMetadata
	for _, input := range c.Inputs {
		files = append(files, input.Files...)
	}
	return files
}

func (c *Compaction) InputBytes() uint64 { return manifest.TotalSize(c.InputFiles()) }

func (c *Compaction) setKeyRange() {
	c.Smallest, c.Largest = userKeyRange(c.compare, c.InputFiles())
}

// IsBaseLevelForKey needs keys in increasing order: each level's cursor only moves forward.
func (c *Compaction) IsBaseLevelForKey(userKey []byte) bool {
	for _, file := range c.OlderL0 {
		if file.ContainsUserKey(c.compare, userKey) {
			return false
		}
	}
	for level := c.OutputLevel + 1; level < c.numLevels; level++ {
		files := c.Version.Levels[level]
		for c.levelCursors[level] < len(files) {
			file := files[c.levelCursors[level]]
			if c.compare(userKey, file.Largest.UserKey) <= 0 {
				if c.compare(userKey, file.Smallest.UserKey) >= 0 {
					return false
				}
				break
			}
			c.levelCursors[level]++
		}
	}
	return true
}

// ShouldStopBefore splits outputs that overlap too much of Grandparents, so
// compacting them later stays cheap. It must see every key, in order.
func (c *Compaction) ShouldStopBefore(key base.InternalKey) bool {
	for c.grandparentIndex < len(c.Grandparents) && base.InternalCompare(c.compare, key, c.Grandparents[c.grandparentIndex].Largest) > 0 {
		if c.seenFirstKey {
			c.grandparentOverlap += c.Grandparents[c.grandparentIndex].Size
		}
		c.grandparentIndex++
	}
	c.seenFirstKey = true
	if c.MaxGrandparentOverlap > 0 && c.grandparentOverlap > c.MaxGrandparentOverlap {
		c.grandparentOverlap = 0
		return true
	}
	return false
}

func userKeyRange(compare base.Compare, files []*manifest.FileMetadata) (smallest, largest []byte) {
	for i, file := range files {
		if i == 0 || compare(file.Smallest.UserKey, smallest) < 0 {
			smallest = file.Smallest.UserKey
		}
		if i == 0 || compare(file.Largest.UserKey, largest) > 0 {
			largest = file.Largest.UserKey
		}
	}
	return smallest, largest
}

func largestKey(compare base.Compare, files []*manifest.FileMetadata) base.InternalKey {
	var largest base.InternalKey
	for i, file := range files {
		if i == 0 || base.InternalCompare(compare, file.Largest, largest) > 0 {
			largest = file.Largest
		}
	}
	return largest
}

func anyCompacting(files []*manifest.FileMetadata) bool {
	for _, file := range files {
		if file.Compacting {
			return true
		}
	}
	return false
}

func PickManual(compare base.Compare, opts *Options, version *manifest.Version, level int, start, end []byte, inPlace bool, resumeAfter []byte) *Compaction {
	inputs := version.Overlaps(level, compare, start, end)
	if inPlace && resumeAfter != nil {
		i := 0
		for i < len(inputs) && compare(inputs[i].Smallest.UserKey, resumeAfter) <= 0 {
			i++
		}
		inputs = inputs[i:]
	}
	if len(inputs) == 0 || anyCompacting(inputs) {
		return nil
	}
	if level > 0 {
		var total uint64
		for i, file := range inputs {
			total += file.Size
			if total >= opts.TargetFileSize {
				inputs = inputs[:i+1]
				break
			}
		}
	}
	if inPlace {
		job := newCompaction(compare, opts.NumLevels, version, "manual")
		job.Inputs = []Input{{Level: level, Files: inputs}}
		job.OutputLevel = level
		job.MaxOutputFileSize = opts.TargetFileSize
		job.setKeyRange()
		if level+1 < opts.NumLevels {
			job.Grandparents = version.Overlaps(level+1, compare, job.Smallest, job.Largest)
			job.MaxGrandparentOverlap = opts.MaxGrandparentOverlap()
		}
		return job
	}
	job := setupLeveledCompaction(compare, opts, version, level, inputs, "manual")
	if job != nil {
		// Rewrite rather than move, so tombstones and expired entries are dropped.
		job.TrivialMove = false
	}
	return job
}

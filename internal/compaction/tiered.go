package compaction

import (
	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
)

// Tiered merges only adjacent runs, which preserves age order and so the tombstone rules.
type Tiered struct {
	compare base.Compare
	opts    *Options
}

func NewTiered(compare base.Compare, opts *Options) *Tiered {
	return &Tiered{compare: compare, opts: opts}
}

type sortedRun struct {
	level int
	files []*manifest.FileMetadata
	size  uint64
}

func (tp *Tiered) sortedRuns(version *manifest.Version) []sortedRun {
	var runs []sortedRun
	for _, file := range version.Levels[0] {
		runs = append(runs, sortedRun{level: 0, files: []*manifest.FileMetadata{file}, size: file.Size})
	}
	for level := 1; level < tp.opts.NumLevels; level++ {
		if files := version.Levels[level]; len(files) > 0 {
			runs = append(runs, sortedRun{level: level, files: files, size: manifest.TotalSize(files)})
		}
	}
	return runs
}

func (tp *Tiered) NumRuns(version *manifest.Version) int { return len(tp.sortedRuns(version)) }

func (tp *Tiered) Pick(version *manifest.Version, env Env) *Compaction {
	runs := tp.sortedRuns(version)
	for _, run := range runs {
		if anyCompacting(run.files) {
			return nil
		}
	}
	if len(runs) < 2 {
		return nil
	}

	oldestSize := runs[len(runs)-1].size
	var newerSize uint64
	for _, run := range runs[:len(runs)-1] {
		newerSize += run.size
	}
	if newerSize*100 >= tp.opts.TieredMaxSizeAmpPercent*max(oldestSize, 1) {
		return tp.mergeRuns(version, runs, 0, len(runs)-1, "tiered-size-amp")
	}

	for first := range runs {
		last := first
		minSize, maxSize := runs[first].size, runs[first].size
		for last+1 < len(runs) {
			nextSize := runs[last+1].size
			newMinSize, newMaxSize := min(minSize, nextSize), max(maxSize, nextSize)
			if float64(newMaxSize) > tp.opts.TieredSizeRatio*float64(max(newMinSize, 1)) {
				break
			}
			minSize, maxSize = newMinSize, newMaxSize
			last++
		}
		if last-first+1 >= tp.opts.TieredMinMergeWidth {
			return tp.mergeRuns(version, runs, first, last, "tiered-size-ratio")
		}
	}

	// Fallback: merge all of L0 so writes can never stall for good.
	if numL0 := len(version.Levels[0]); numL0 >= tp.opts.L0SlowdownWritesTrigger && numL0 >= 2 {
		return tp.mergeRuns(version, runs, 0, numL0-1, "tiered-L0")
	}
	return nil
}

func (tp *Tiered) mergeRuns(version *manifest.Version, runs []sortedRun, first, last int, reason string) *Compaction {
	job := newCompaction(tp.compare, tp.opts.NumLevels, version, reason)
	var l0Files []*manifest.FileMetadata
	for _, run := range runs[first : last+1] {
		if run.level == 0 {
			l0Files = append(l0Files, run.files...)
		} else {
			job.Inputs = append(job.Inputs, Input{Level: run.level, Files: run.files})
		}
	}
	if len(l0Files) > 0 {
		job.Inputs = append([]Input{{Level: 0, Files: l0Files}}, job.Inputs...)
	}

	numL0 := len(version.Levels[0])
	switch oldestRun := runs[last]; {
	case oldestRun.level > 0:
		job.OutputLevel = oldestRun.level
	case last < numL0-1:
		// Older L0 files remain beneath the output, so it stays in L0.
		job.OutputLevel = 0
		job.OlderL0 = version.Levels[0][last+1:]
	default:
		firstNonEmpty := tp.opts.NumLevels
		for level := 1; level < tp.opts.NumLevels; level++ {
			if len(version.Levels[level]) > 0 {
				firstNonEmpty = level
				break
			}
		}
		job.OutputLevel = firstNonEmpty - 1
	}
	if job.OutputLevel > 0 {
		job.MaxOutputFileSize = tp.opts.TargetFileSize
	}
	job.setKeyRange()
	return job
}

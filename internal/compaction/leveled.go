package compaction

import (
	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
)

type Leveled struct {
	compare base.Compare
	opts    *Options
}

func NewLeveled(compare base.Compare, opts *Options) *Leveled {
	return &Leveled{compare: compare, opts: opts}
}

// Scores rates L0 by file count, since every L0 file costs a lookup however small.
func (lp *Leveled) Scores(version *manifest.Version) [manifest.NumLevels]float64 {
	var scores [manifest.NumLevels]float64
	for level := 0; level < lp.opts.NumLevels-1; level++ {
		if level == 0 {
			scores[0] = float64(len(version.Levels[0])) / float64(lp.opts.L0CompactionTrigger)
		} else {
			scores[level] = float64(version.LevelSize(level)) / float64(lp.opts.MaxBytesForLevel(level))
		}
	}
	return scores
}

func (lp *Leveled) Pick(version *manifest.Version, env Env) *Compaction {
	scores := lp.Scores(version)
	bestLevel := -1
	for level := 0; level < lp.opts.NumLevels-1; level++ {
		if scores[level] >= 1 && (bestLevel < 0 || scores[level] > scores[bestLevel]) {
			bestLevel = level
		}
	}
	if bestLevel < 0 {
		return nil
	}
	files := version.Levels[bestLevel]
	var inputs []*manifest.FileMetadata
	if bestLevel == 0 {
		// Taking the whole overlap closure keeps newer data above older data.
		oldest := files[len(files)-1]
		inputs = version.Overlaps(0, lp.compare, oldest.Smallest.UserKey, oldest.Largest.UserKey)
	} else {
		// The compaction pointer rotates compactions through the key space.
		pointer := env.CompactPointers[bestLevel]
		picked := files[0]
		if pointer.UserKey != nil {
			for _, file := range files {
				if base.InternalCompare(lp.compare, file.Largest, pointer) > 0 {
					picked = file
					break
				}
			}
		}
		inputs = []*manifest.FileMetadata{picked}
	}
	reason := "size"
	if bestLevel == 0 {
		reason = "L0"
	}
	return setupLeveledCompaction(lp.compare, lp.opts, version, bestLevel, inputs, reason)
}

func setupLeveledCompaction(compare base.Compare, opts *Options, version *manifest.Version, level int, startInputs []*manifest.FileMetadata, reason string) *Compaction {
	outputLevel := level + 1
	startInputs = addBoundaryInputs(compare, version, level, startInputs)
	smallest, largest := userKeyRange(compare, startInputs)
	outputInputs := addBoundaryInputs(compare, version, outputLevel, version.Overlaps(outputLevel, compare, smallest, largest))
	rangeStart, rangeEnd := userKeyRange(compare, append(append([]*manifest.FileMetadata{}, startInputs...), outputInputs...))

	if len(outputInputs) > 0 {
		expandedStart := addBoundaryInputs(compare, version, level, version.Overlaps(level, compare, rangeStart, rangeEnd))
		if len(expandedStart) > len(startInputs) &&
			manifest.TotalSize(outputInputs)+manifest.TotalSize(expandedStart) < opts.ExpandedCompactionLimit() &&
			!anyCompacting(expandedStart) {
			expandedSmallest, expandedLargest := userKeyRange(compare, expandedStart)
			expandedOutput := addBoundaryInputs(compare, version, outputLevel, version.Overlaps(outputLevel, compare, expandedSmallest, expandedLargest))
			if len(expandedOutput) == len(outputInputs) {
				startInputs, outputInputs = expandedStart, expandedOutput
				rangeStart, rangeEnd = userKeyRange(compare, append(append([]*manifest.FileMetadata{}, startInputs...), outputInputs...))
			}
		}
	}
	if anyCompacting(startInputs) || anyCompacting(outputInputs) {
		return nil
	}

	job := newCompaction(compare, opts.NumLevels, version, reason)
	job.Inputs = []Input{{Level: level, Files: startInputs}}
	if len(outputInputs) > 0 {
		job.Inputs = append(job.Inputs, Input{Level: outputLevel, Files: outputInputs})
	}
	job.OutputLevel = outputLevel
	job.MaxOutputFileSize = opts.TargetFileSize
	job.MaxGrandparentOverlap = opts.MaxGrandparentOverlap()
	if outputLevel+1 < opts.NumLevels {
		job.Grandparents = version.Overlaps(outputLevel+1, compare, rangeStart, rangeEnd)
	}
	job.Smallest, job.Largest = rangeStart, rangeEnd
	job.CompactPointer = &manifest.CompactPointer{Level: level, Key: largestKey(compare, startInputs).Clone()}
	job.TrivialMove = len(startInputs) == 1 && len(outputInputs) == 0 &&
		manifest.TotalSize(job.Grandparents) <= job.MaxGrandparentOverlap
	return job
}

// Never partly compact a user key split across files (a guard: outputs never split one).
func addBoundaryInputs(compare base.Compare, version *manifest.Version, level int, inputs []*manifest.FileMetadata) []*manifest.FileMetadata {
	if level == 0 || len(inputs) == 0 {
		return inputs
	}
	largest := largestKey(compare, inputs)
	for {
		var next *manifest.FileMetadata
		for _, file := range version.Levels[level] {
			if base.InternalCompare(compare, file.Smallest, largest) > 0 && compare(file.Smallest.UserKey, largest.UserKey) == 0 &&
				(next == nil || base.InternalCompare(compare, file.Smallest, next.Smallest) < 0) {
				next = file
			}
		}
		if next == nil {
			return inputs
		}
		inputs = append(inputs[:len(inputs):len(inputs)], next)
		largest = next.Largest
	}
}

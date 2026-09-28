package manifest

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

const NumLevels = 7

// Version is immutable. L0 is newest first (by LargestSeq) and may overlap; L1+
// are sorted, non-overlapping, and no user key spans two files of a level.
type Version struct {
	Levels [NumLevels][]*FileMetadata

	refs       atomic.Int32
	list       *versionList
	prev, next *Version
}

func (v *Version) Ref() { v.refs.Add(1) }

func (v *Version) Unref() {
	switch refs := v.refs.Add(-1); {
	case refs == 0:
		if list := v.list; list != nil {
			list.mu.Lock()
			list.remove(v)
			list.mu.Unlock()
		}
	case refs < 0:
		panic("manifest: version reference count went negative")
	}
}

func (v *Version) Refs() int32 { return v.refs.Load() }

func (v *Version) NumFiles(level int) int { return len(v.Levels[level]) }

func (v *Version) LevelSize(level int) uint64 { return TotalSize(v.Levels[level]) }

func TotalSize(files []*FileMetadata) uint64 {
	var total uint64
	for _, file := range files {
		total += file.Size
	}
	return total
}

// A nil bound is open. At L0 the range widens until no overlapping file is left out.
func (v *Version) Overlaps(level int, compare base.Compare, start, end []byte) []*FileMetadata {
	files := v.Levels[level]
	endsBeforeStart := func(file *FileMetadata) bool { return start != nil && compare(file.Largest.UserKey, start) < 0 }
	startsAfterEnd := func(file *FileMetadata) bool { return end != nil && compare(file.Smallest.UserKey, end) > 0 }
	if level > 0 {
		first := sort.Search(len(files), func(first int) bool { return !endsBeforeStart(files[first]) })
		limit := first
		for limit < len(files) && !startsAfterEnd(files[limit]) {
			limit++
		}
		return files[first:limit:limit]
	}
	var overlapping []*FileMetadata
restart:
	overlapping = overlapping[:0]
	for _, file := range files {
		if endsBeforeStart(file) || startsAfterEnd(file) {
			continue
		}
		overlapping = append(overlapping, file)
		if start != nil && compare(file.Smallest.UserKey, start) < 0 {
			start = file.Smallest.UserKey
			goto restart
		}
		if end != nil && compare(file.Largest.UserKey, end) > 0 {
			end = file.Largest.UserKey
			goto restart
		}
	}
	return overlapping
}

func (v *Version) CheckOrdering(compare base.Compare) error {
	for level, files := range v.Levels {
		for i, file := range files {
			if base.InternalCompare(compare, file.Smallest, file.Largest) > 0 {
				return fmt.Errorf("file %06d at L%d has smallest %s > largest %s", file.FileNum, level, file.Smallest, file.Largest)
			}
			if i == 0 {
				continue
			}
			prev := files[i-1]
			if level == 0 {
				if !newerL0(prev, file) {
					return fmt.Errorf("file %06d at L0 (largest seq %d) is ordered before older-or-equal file %06d (largest seq %d)",
						prev.FileNum, prev.LargestSeq, file.FileNum, file.LargestSeq)
				}
			} else if compare(prev.Largest.UserKey, file.Smallest.UserKey) >= 0 {
				return fmt.Errorf("files %06d and %06d at L%d overlap or share a user key: %s >= %s",
					prev.FileNum, file.FileNum, level, prev.Largest, file.Smallest)
			}
		}
	}
	return nil
}

func newerL0(a, b *FileMetadata) bool {
	if a.LargestSeq != b.LargestSeq {
		return a.LargestSeq > b.LargestSeq
	}
	return a.FileNum > b.FileNum
}

func sortLevel(level int, compare base.Compare, files []*FileMetadata) {
	if level == 0 {
		sort.Slice(files, func(i, j int) bool { return newerL0(files[i], files[j]) })
		return
	}
	sort.Slice(files, func(i, j int) bool {
		return base.InternalCompare(compare, files[i].Smallest, files[j].Smallest) < 0
	})
}

func (v *Version) String() string {
	var out strings.Builder
	for level, files := range v.Levels {
		if len(files) == 0 {
			continue
		}
		fmt.Fprintf(&out, "L%d:\n", level)
		for _, file := range files {
			fmt.Fprintf(&out, "  %s\n", file)
		}
	}
	return out.String()
}

// versionList's mutex is a leaf: nothing else is locked while it is held.
type versionList struct {
	mu   sync.Mutex
	root Version
}

func (vl *versionList) init() {
	vl.root.prev = &vl.root
	vl.root.next = &vl.root
}

func (vl *versionList) pushBack(version *Version) {
	version.list = vl
	version.prev = vl.root.prev
	version.next = &vl.root
	version.prev.next = version
	vl.root.prev = version
}

func (vl *versionList) remove(version *Version) {
	version.prev.next = version.next
	version.next.prev = version.prev
	version.prev, version.next, version.list = nil, nil, nil
}

type versionBuilder struct {
	compare     base.Compare
	baseVersion *Version
	added       [NumLevels]map[uint64]*FileMetadata
	deleted     [NumLevels]map[uint64]bool
}

func newVersionBuilder(compare base.Compare, baseVersion *Version) *versionBuilder {
	vb := &versionBuilder{compare: compare, baseVersion: baseVersion}
	for i := range vb.added {
		vb.added[i] = make(map[uint64]*FileMetadata)
		vb.deleted[i] = make(map[uint64]bool)
	}
	return vb
}

func (vb *versionBuilder) apply(edit *VersionEdit) error {
	for _, deletion := range edit.DeletedFiles {
		if _, ok := vb.added[deletion.Level][deletion.FileNum]; ok {
			delete(vb.added[deletion.Level], deletion.FileNum)
			continue
		}
		if !vb.inBaseVersion(deletion.Level, deletion.FileNum) || vb.deleted[deletion.Level][deletion.FileNum] {
			return base.CorruptionErrorf("manifest: deleting file %06d, which is not at L%d", deletion.FileNum, deletion.Level)
		}
		vb.deleted[deletion.Level][deletion.FileNum] = true
	}
	for _, addition := range edit.NewFiles {
		if _, ok := vb.added[addition.Level][addition.File.FileNum]; ok || (vb.inBaseVersion(addition.Level, addition.File.FileNum) && !vb.deleted[addition.Level][addition.File.FileNum]) {
			return base.CorruptionErrorf("manifest: adding file %06d, which is already at L%d", addition.File.FileNum, addition.Level)
		}
		vb.added[addition.Level][addition.File.FileNum] = addition.File
	}
	return nil
}

func (vb *versionBuilder) inBaseVersion(level int, fileNum uint64) bool {
	if vb.baseVersion == nil {
		return false
	}
	for _, file := range vb.baseVersion.Levels[level] {
		if file.FileNum == fileNum {
			return true
		}
	}
	return false
}

func (vb *versionBuilder) save() (*Version, error) {
	version := &Version{}
	levelOf := make(map[uint64]int)
	for level := range version.Levels {
		var baseFiles []*FileMetadata
		if vb.baseVersion != nil {
			baseFiles = vb.baseVersion.Levels[level]
		}
		if len(vb.added[level]) == 0 && len(vb.deleted[level]) == 0 {
			version.Levels[level] = baseFiles
		} else {
			files := make([]*FileMetadata, 0, len(baseFiles)+len(vb.added[level]))
			for _, file := range baseFiles {
				if !vb.deleted[level][file.FileNum] {
					files = append(files, file)
				}
			}
			for _, file := range vb.added[level] {
				files = append(files, file)
			}
			sortLevel(level, vb.compare, files)
			version.Levels[level] = files
		}
		for _, file := range version.Levels[level] {
			if otherLevel, ok := levelOf[file.FileNum]; ok {
				return nil, base.CorruptionErrorf("manifest: file %06d is at both L%d and L%d", file.FileNum, otherLevel, level)
			}
			levelOf[file.FileNum] = level
		}
	}
	if err := version.CheckOrdering(vb.compare); err != nil {
		return nil, base.CorruptionErrorf("manifest: %v", err)
	}
	return version, nil
}

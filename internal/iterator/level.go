package iterator

import (
	"sort"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
)

type OpenFunc func(f *manifest.FileMetadata) (base.InternalIterator, error)

type Level struct {
	compare base.Compare
	files   []*manifest.FileMetadata
	open    OpenFunc
	lower   []byte
	upper   []byte

	fileIndex int
	fileIter  base.InternalIterator
	err       error
}

var _ base.InternalIterator = (*Level)(nil)

// NewLevel requires files to be sorted and non-overlapping.
func NewLevel(compare base.Compare, files []*manifest.FileMetadata, open OpenFunc, lower, upper []byte) *Level {
	return &Level{compare: compare, files: files, open: open, lower: lower, upper: upper, fileIndex: -1}
}

func (li *Level) outsideBounds(i int) bool {
	file := li.files[i]
	return (li.upper != nil && li.compare(file.Smallest.UserKey, li.upper) >= 0) ||
		(li.lower != nil && li.compare(file.Largest.UserKey, li.lower) < 0)
}

func (li *Level) openFile(i int) bool {
	if li.fileIter != nil && li.fileIndex == i {
		return true
	}
	li.closeFile()
	if i < 0 || i >= len(li.files) || li.outsideBounds(i) {
		li.fileIndex = -1
		return false
	}
	fileIter, err := li.open(li.files[i])
	if err != nil {
		li.err = err
		li.fileIndex = -1
		return false
	}
	li.fileIndex, li.fileIter = i, fileIter
	return true
}

func (li *Level) closeFile() {
	if li.fileIter != nil {
		if err := li.fileIter.Close(); err != nil && li.err == nil {
			li.err = err
		}
		li.fileIter = nil
	}
}

func (li *Level) checkFileError() bool {
	if err := li.fileIter.Error(); err != nil {
		li.err = err
		return true
	}
	return false
}

func (li *Level) skipForward() {
	for li.fileIter != nil && !li.fileIter.Valid() {
		if li.checkFileError() {
			return
		}
		nextIndex := li.fileIndex + 1
		if !li.openFile(nextIndex) {
			return
		}
		li.fileIter.First()
	}
}

func (li *Level) skipBackward() {
	for li.fileIter != nil && !li.fileIter.Valid() {
		if li.checkFileError() {
			return
		}
		prevIndex := li.fileIndex - 1
		if !li.openFile(prevIndex) {
			return
		}
		li.fileIter.Last()
	}
}

func (li *Level) SeekGE(key base.InternalKey) {
	li.err = nil
	i := sort.Search(len(li.files), func(i int) bool {
		return base.InternalCompare(li.compare, li.files[i].Largest, key) >= 0
	})
	if !li.openFile(i) {
		return
	}
	li.fileIter.SeekGE(key)
	li.skipForward()
}

func (li *Level) SeekLT(key base.InternalKey) {
	li.err = nil
	i := sort.Search(len(li.files), func(i int) bool {
		return base.InternalCompare(li.compare, li.files[i].Smallest, key) >= 0
	}) - 1
	if !li.openFile(i) {
		return
	}
	li.fileIter.SeekLT(key)
	li.skipBackward()
}

func (li *Level) First() {
	li.err = nil
	i := 0
	if li.lower != nil {
		i = sort.Search(len(li.files), func(i int) bool { return li.compare(li.files[i].Largest.UserKey, li.lower) >= 0 })
	}
	if !li.openFile(i) {
		return
	}
	li.fileIter.First()
	li.skipForward()
}

func (li *Level) Last() {
	li.err = nil
	i := len(li.files) - 1
	if li.upper != nil {
		i = sort.Search(len(li.files), func(i int) bool { return li.compare(li.files[i].Smallest.UserKey, li.upper) >= 0 }) - 1
	}
	if !li.openFile(i) {
		return
	}
	li.fileIter.Last()
	li.skipBackward()
}

func (li *Level) Next() {
	if !li.Valid() {
		return
	}
	li.fileIter.Next()
	li.skipForward()
}

func (li *Level) Prev() {
	if !li.Valid() {
		return
	}
	li.fileIter.Prev()
	li.skipBackward()
}

func (li *Level) Valid() bool { return li.err == nil && li.fileIter != nil && li.fileIter.Valid() }

func (li *Level) Key() base.InternalKey { return li.fileIter.Key() }

func (li *Level) Value() []byte { return li.fileIter.Value() }

func (li *Level) Error() error { return li.err }

func (li *Level) Close() error {
	li.closeFile()
	return li.err
}

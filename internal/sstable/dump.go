package sstable

import (
	"fmt"
	"io"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/vfs"
)

func DumpFile(fs vfs.FS, path string, comparer *base.Comparer, w io.Writer, verify bool) error {
	file, err := fs.Open(path)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}
	reader, err := Open(file, uint64(info.Size()), ReaderOptions{Comparer: comparer, Name: path})
	if err != nil {
		file.Close()
		return err
	}
	defer reader.Close()
	return reader.Dump(w, verify)
}

func (r *Reader) Dump(w io.Writer, verify bool) error {
	fmt.Fprintf(w, "sstable %s: %d bytes, format version %d\n", r.name, r.size, r.footer.version)
	fmt.Fprintf(w, "footer: index %+v, filter %+v, properties %+v\n", r.footer.index, r.footer.filter, r.footer.properties)
	fmt.Fprintf(w, "properties:\n%s", r.props)

	var indexIter blockIter
	if err := indexIter.init(r.compare, r.indexBlock, false); err != nil {
		return r.wrap(err)
	}
	fmt.Fprintln(w, "index:")
	for indexIter.First(); indexIter.Valid(); indexIter.Next() {
		handle, ok := decodeHandleVarint(indexIter.Value())
		if !ok {
			return r.corrupt("bad block handle in index")
		}
		fmt.Fprintf(w, "  %s -> offset %d, length %d\n", indexIter.Key(), handle.Offset, handle.Length)
	}
	if err := indexIter.Error(); err != nil {
		return r.wrap(err)
	}

	fmt.Fprintln(w, "entries:")
	it := r.NewIter(false)
	defer it.Close()
	var prevKey base.InternalKey
	var entries, deletions, merges uint64
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		fmt.Fprintf(w, "  %s = %s\n", key, formatValue(it.Value()))
		if verify && entries > 0 && base.InternalCompare(r.compare, prevKey, key) >= 0 {
			return fmt.Errorf("sstable %s: entry %d: key %s does not follow %s", r.name, entries, key, prevKey)
		}
		prevKey = key.Clone()
		entries++
		switch key.Kind() {
		case base.KindDelete:
			deletions++
		case base.KindMerge:
			merges++
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	if verify && (entries != r.props.NumEntries || deletions != r.props.NumDeletions || merges != r.props.NumMerges) {
		return fmt.Errorf("sstable %s: counted %d entries, %d deletions, %d merges; properties say %d, %d, %d",
			r.name, entries, deletions, merges, r.props.NumEntries, r.props.NumDeletions, r.props.NumMerges)
	}
	return nil
}

func formatValue(value []byte) string {
	const maxShown = 32
	if len(value) > maxShown {
		return fmt.Sprintf("%q... (%d bytes)", value[:maxShown], len(value))
	}
	return fmt.Sprintf("%q", value)
}

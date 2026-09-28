package manifest

import (
	"errors"
	"fmt"
	"io"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/record"
	"github.com/Thinker-08/FlashLSM-DB/vfs"
)

func Dump(fs vfs.FS, path string, comparer *base.Comparer, w io.Writer) error {
	file, err := fs.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	builder := newVersionBuilder(comparer.Compare, nil)
	records := record.NewReader(file, info.Size())
	for i := 0; ; i++ {
		payload, err := records.Next()
		if err == io.EOF {
			break
		}
		var recordErr *record.Error
		if errors.As(err, &recordErr) {
			fmt.Fprintf(w, "damaged tail: %v\n", recordErr)
			break
		}
		if err != nil {
			return err
		}
		var edit VersionEdit
		if err := edit.Decode(payload); err != nil {
			return fmt.Errorf("edit %d at offset %d: %w", i, records.Offset(), err)
		}
		fmt.Fprintf(w, "edit %d at offset %d:\n%s", i, records.Offset(), &edit)
		if err := builder.apply(&edit); err != nil {
			return err
		}
	}
	version, err := builder.save()
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "resulting version:\n%s", version)
	return nil
}

package record

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

const (
	HeaderLen = 8
	// MaxRecordLen must exceed the largest batch, 64 MiB.
	MaxRecordLen         = 1<<26 + 1<<20
	directWriteThreshold = 1 << 20
)

// Writer errors are sticky: after a failed write, the file's tail is unknown.
type Writer struct {
	dst      io.Writer
	syncFile func() error
	buf      []byte
	size     int64
	err      error
}

type Syncer interface {
	io.Writer
	Sync() error
}

func NewWriter(file Syncer) *Writer {
	return &Writer{dst: file, syncFile: file.Sync}
}

func (w *Writer) WriteRecord(payload []byte) error {
	if w.err != nil {
		return w.err
	}
	if len(payload) > MaxRecordLen {
		return fmt.Errorf("record: payload of %d bytes exceeds the %d-byte limit", len(payload), MaxRecordLen)
	}
	var header [HeaderLen]byte
	binary.LittleEndian.PutUint32(header[4:], uint32(len(payload)))
	crc := base.CRCUpdate(base.CRC(header[4:]), payload)
	binary.LittleEndian.PutUint32(header[:4], crc)

	var written int
	var err error
	if len(payload) <= directWriteThreshold {
		w.buf = append(append(w.buf[:0], header[:]...), payload...)
		written, err = w.dst.Write(w.buf)
	} else {
		written, err = w.dst.Write(header[:])
		if err == nil {
			var payloadWritten int
			payloadWritten, err = w.dst.Write(payload)
			written += payloadWritten
		}
	}
	w.size += int64(written)
	if err != nil {
		w.err = err
	}
	return err
}

func (w *Writer) Sync() error {
	if w.err != nil {
		return w.err
	}
	if err := w.syncFile(); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *Writer) Size() int64 { return w.size }

func (w *Writer) Err() error { return w.err }

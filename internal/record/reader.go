package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

type Error struct {
	Offset int64
	Reason string
	// Torn means the damage runs to EOF, as a write cut short by a crash leaves it.
	Torn bool
}

func (e *Error) Error() string {
	return fmt.Sprintf("record at offset %d: %s", e.Offset, e.Reason)
}

func (e *Error) Is(target error) bool { return target == base.ErrCorruption }

type Reader struct {
	src          io.Reader
	size         int64
	offset       int64
	recordOffset int64
	buf          []byte
	header       [HeaderLen]byte
}

func NewReader(src io.Reader, size int64) *Reader {
	return &Reader{src: src, size: size}
}

// Next returns a payload that is valid only until the following call.
func (r *Reader) Next() ([]byte, error) {
	r.recordOffset = r.offset
	remaining := r.size - r.offset
	if remaining <= 0 {
		return nil, io.EOF
	}
	if remaining < HeaderLen {
		return nil, r.corrupt("truncated record header", true)
	}
	if _, err := io.ReadFull(r.src, r.header[:]); err != nil {
		return nil, r.readError(err)
	}
	r.offset += HeaderLen
	remaining -= HeaderLen
	crc := binary.LittleEndian.Uint32(r.header[:4])
	length := binary.LittleEndian.Uint32(r.header[4:])
	switch {
	case crc == 0 && length == 0:
		return nil, r.corrupt("zeroed record header", r.restIsZero())
	case length > MaxRecordLen:
		return nil, r.corrupt(fmt.Sprintf("record length %d exceeds the limit", length), false)
	case int64(length) > remaining:
		return nil, r.corrupt(fmt.Sprintf("record length %d exceeds the %d bytes left", length, remaining), true)
	}
	if cap(r.buf) < int(length) {
		r.buf = make([]byte, length)
	}
	payload := r.buf[:length]
	if _, err := io.ReadFull(r.src, payload); err != nil {
		return nil, r.readError(err)
	}
	r.offset += int64(length)
	if base.CRCUpdate(base.CRC(r.header[4:]), payload) != crc {
		return nil, r.corrupt("checksum mismatch", r.offset == r.size)
	}
	return payload, nil
}

func (r *Reader) Offset() int64 { return r.recordOffset }

func (r *Reader) corrupt(reason string, torn bool) error {
	return &Error{Offset: r.recordOffset, Reason: reason, Torn: torn}
}

func (r *Reader) readError(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return r.corrupt("file shorter than its size", true)
	}
	return err
}

func (r *Reader) restIsZero() bool {
	buf := make([]byte, 32<<10)
	for r.offset < r.size {
		n := int(min(int64(len(buf)), r.size-r.offset))
		if _, err := io.ReadFull(r.src, buf[:n]); err != nil {
			return false
		}
		r.offset += int64(n)
		for _, value := range buf[:n] {
			if value != 0 {
				return false
			}
		}
	}
	return true
}

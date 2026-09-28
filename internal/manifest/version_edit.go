package manifest

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

// Persisted edit tags. An unknown tag is corruption: only this package writes edits.
const (
	tagComparer       = 1
	tagLogNumber      = 2
	tagNextFileNum    = 3
	tagLastSeq        = 4
	tagCompactPointer = 5
	tagDeletedFile    = 6
	tagNewFile        = 7
)

type DeletedFile struct {
	Level   int
	FileNum uint64
}

type NewFile struct {
	Level int
	File  *FileMetadata
}

type CompactPointer struct {
	Level int
	Key   base.InternalKey
}

type VersionEdit struct {
	ComparerName    string
	LogNumber       uint64
	NextFileNum     uint64
	LastSeq         uint64
	HasLogNumber    bool
	HasNextFileNum  bool
	HasLastSeq      bool
	CompactPointers []CompactPointer
	DeletedFiles    []DeletedFile
	NewFiles        []NewFile
}

func (ve *VersionEdit) SetLogNumber(logNumber uint64) {
	ve.LogNumber, ve.HasLogNumber = logNumber, true
}
func (ve *VersionEdit) SetNextFileNum(fileNum uint64) {
	ve.NextFileNum, ve.HasNextFileNum = fileNum, true
}
func (ve *VersionEdit) SetLastSeq(seq uint64) { ve.LastSeq, ve.HasLastSeq = seq, true }

func (ve *VersionEdit) AddFile(level int, file *FileMetadata) {
	ve.NewFiles = append(ve.NewFiles, NewFile{Level: level, File: file})
}

func (ve *VersionEdit) DeleteFile(level int, fileNum uint64) {
	ve.DeletedFiles = append(ve.DeletedFiles, DeletedFile{Level: level, FileNum: fileNum})
}

func (ve *VersionEdit) SetCompactPointer(level int, key base.InternalKey) {
	ve.CompactPointers = append(ve.CompactPointers, CompactPointer{Level: level, Key: key})
}

func (ve *VersionEdit) Encode(dst []byte) []byte {
	appendBytes := func(data []byte) {
		dst = binary.AppendUvarint(dst, uint64(len(data)))
		dst = append(dst, data...)
	}
	if ve.ComparerName != "" {
		dst = binary.AppendUvarint(dst, tagComparer)
		appendBytes([]byte(ve.ComparerName))
	}
	if ve.HasLogNumber {
		dst = binary.AppendUvarint(dst, tagLogNumber)
		dst = binary.AppendUvarint(dst, ve.LogNumber)
	}
	if ve.HasNextFileNum {
		dst = binary.AppendUvarint(dst, tagNextFileNum)
		dst = binary.AppendUvarint(dst, ve.NextFileNum)
	}
	if ve.HasLastSeq {
		dst = binary.AppendUvarint(dst, tagLastSeq)
		dst = binary.AppendUvarint(dst, ve.LastSeq)
	}
	for _, pointer := range ve.CompactPointers {
		dst = binary.AppendUvarint(dst, tagCompactPointer)
		dst = binary.AppendUvarint(dst, uint64(pointer.Level))
		appendBytes(pointer.Key.Append(nil))
	}
	for _, deleted := range ve.DeletedFiles {
		dst = binary.AppendUvarint(dst, tagDeletedFile)
		dst = binary.AppendUvarint(dst, uint64(deleted.Level))
		dst = binary.AppendUvarint(dst, deleted.FileNum)
	}
	for _, added := range ve.NewFiles {
		file := added.File
		dst = binary.AppendUvarint(dst, tagNewFile)
		dst = binary.AppendUvarint(dst, uint64(added.Level))
		dst = binary.AppendUvarint(dst, file.FileNum)
		dst = binary.AppendUvarint(dst, file.Size)
		appendBytes(file.Smallest.Append(nil))
		appendBytes(file.Largest.Append(nil))
		dst = binary.AppendUvarint(dst, file.SmallestSeq)
		dst = binary.AppendUvarint(dst, file.LargestSeq)
	}
	return dst
}

type editDecoder struct {
	remaining []byte
	err       error
}

func (dec *editDecoder) fail(field string) {
	if dec.err == nil {
		dec.err = base.CorruptionErrorf("version edit: bad %s", field)
	}
}

func (dec *editDecoder) uvarint(field string) uint64 {
	if dec.err != nil {
		return 0
	}
	value, n := binary.Uvarint(dec.remaining)
	if n <= 0 {
		dec.fail(field)
		return 0
	}
	dec.remaining = dec.remaining[n:]
	return value
}

// bytes returns a copy, since the record buffer is reused.
func (dec *editDecoder) bytes(field string) []byte {
	length := dec.uvarint(field)
	if dec.err != nil {
		return nil
	}
	if length > uint64(len(dec.remaining)) {
		dec.fail(field)
		return nil
	}
	data := make([]byte, length)
	copy(data, dec.remaining[:length])
	dec.remaining = dec.remaining[length:]
	return data
}

func (dec *editDecoder) level() int {
	level := dec.uvarint("level")
	if dec.err == nil && level >= NumLevels {
		dec.fail(fmt.Sprintf("level %d", level))
	}
	return int(level)
}

func (dec *editDecoder) internalKey(field string) base.InternalKey {
	encoded := dec.bytes(field)
	if dec.err != nil {
		return base.InternalKey{}
	}
	key, ok := base.DecodeInternalKey(encoded)
	if !ok {
		dec.fail(field)
	}
	return key
}

func (ve *VersionEdit) Decode(encoded []byte) error {
	*ve = VersionEdit{}
	dec := &editDecoder{remaining: encoded}
	for len(dec.remaining) > 0 && dec.err == nil {
		switch tag := dec.uvarint("tag"); tag {
		case tagComparer:
			ve.ComparerName = string(dec.bytes("comparer name"))
		case tagLogNumber:
			ve.SetLogNumber(dec.uvarint("log number"))
		case tagNextFileNum:
			ve.SetNextFileNum(dec.uvarint("next file number"))
		case tagLastSeq:
			ve.SetLastSeq(dec.uvarint("last sequence number"))
		case tagCompactPointer:
			level := dec.level()
			ve.SetCompactPointer(level, dec.internalKey("compact pointer"))
		case tagDeletedFile:
			level := dec.level()
			ve.DeleteFile(level, dec.uvarint("deleted file number"))
		case tagNewFile:
			level := dec.level()
			file := &FileMetadata{}
			file.FileNum = dec.uvarint("file number")
			file.Size = dec.uvarint("file size")
			file.Smallest = dec.internalKey("smallest key")
			file.Largest = dec.internalKey("largest key")
			file.SmallestSeq = dec.uvarint("smallest sequence number")
			file.LargestSeq = dec.uvarint("largest sequence number")
			if dec.err == nil {
				ve.AddFile(level, file)
			}
		default:
			if dec.err == nil {
				dec.err = base.CorruptionErrorf("version edit: unknown tag %d", tag)
			}
		}
	}
	return dec.err
}

func (ve *VersionEdit) String() string {
	var out strings.Builder
	if ve.ComparerName != "" {
		fmt.Fprintf(&out, "  comparer: %s\n", ve.ComparerName)
	}
	if ve.HasLogNumber {
		fmt.Fprintf(&out, "  log number: %d\n", ve.LogNumber)
	}
	if ve.HasNextFileNum {
		fmt.Fprintf(&out, "  next file number: %d\n", ve.NextFileNum)
	}
	if ve.HasLastSeq {
		fmt.Fprintf(&out, "  last sequence number: %d\n", ve.LastSeq)
	}
	for _, pointer := range ve.CompactPointers {
		fmt.Fprintf(&out, "  compact pointer L%d: %s\n", pointer.Level, pointer.Key)
	}
	for _, deleted := range ve.DeletedFiles {
		fmt.Fprintf(&out, "  delete L%d: %06d\n", deleted.Level, deleted.FileNum)
	}
	for _, added := range ve.NewFiles {
		fmt.Fprintf(&out, "  add L%d: %s\n", added.Level, added.File)
	}
	return out.String()
}

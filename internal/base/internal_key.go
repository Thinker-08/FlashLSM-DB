package base

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

type Kind uint8

const (
	KindDelete Kind = 0
	KindSet    Kind = 1
	// KindRangeDelete is reserved; it is never written and reads as corruption.
	KindRangeDelete Kind = 2
	KindMerge       Kind = 3
	// KindSetTTL values start with an 8-byte little-endian expiry in Unix nanoseconds.
	KindSetTTL Kind = 4
	// KindSeek is never stored; being largest, it sorts before stored kinds at equal seq.
	KindSeek Kind = 255
)

func (k Kind) Valid() bool {
	switch k {
	case KindDelete, KindSet, KindMerge, KindSetTTL:
		return true
	}
	return false
}

func (k Kind) HasValue() bool {
	return k == KindSet || k == KindMerge || k == KindSetTTL
}

func (k Kind) String() string {
	switch k {
	case KindDelete:
		return "DEL"
	case KindSet:
		return "SET"
	case KindRangeDelete:
		return "RANGEDEL"
	case KindMerge:
		return "MERGE"
	case KindSetTTL:
		return "SETTTL"
	case KindSeek:
		return "SEEK"
	}
	return "KIND(" + strconv.Itoa(int(k)) + ")"
}

const (
	TrailerLen        = 8
	SeqNumMax  uint64 = 1<<56 - 1
)

func MakeTrailer(seq uint64, kind Kind) uint64 { return seq<<8 | uint64(kind) }

// InternalKey encodes as UserKey then the 8-byte little-endian trailer seq<<8|kind.
// Keys sort by user key ascending, then trailer descending (newest first).
type InternalKey struct {
	UserKey []byte
	Trailer uint64
}

func MakeInternalKey(userKey []byte, seq uint64, kind Kind) InternalKey {
	return InternalKey{UserKey: userKey, Trailer: MakeTrailer(seq, kind)}
}

func MakeSearchKey(userKey []byte, seq uint64) InternalKey {
	return MakeInternalKey(userKey, seq, KindSeek)
}

func (k InternalKey) SeqNum() uint64 { return k.Trailer >> 8 }

func (k InternalKey) Kind() Kind { return Kind(k.Trailer) }

func (k InternalKey) Size() int { return len(k.UserKey) + TrailerLen }

func (k InternalKey) Encode(buf []byte) {
	userKeyLen := copy(buf, k.UserKey)
	binary.LittleEndian.PutUint64(buf[userKeyLen:], k.Trailer)
}

func (k InternalKey) Append(dst []byte) []byte {
	dst = append(dst, k.UserKey...)
	return binary.LittleEndian.AppendUint64(dst, k.Trailer)
}

func (k InternalKey) Clone() InternalKey {
	return InternalKey{UserKey: CloneBytes(k.UserKey), Trailer: k.Trailer}
}

func (k InternalKey) String() string {
	return fmt.Sprintf("%q#%d,%s", k.UserKey, k.SeqNum(), k.Kind())
}

// The returned UserKey aliases encoded, capped so that appends cannot overwrite the trailer.
func DecodeInternalKey(encoded []byte) (InternalKey, bool) {
	userKeyLen := len(encoded) - TrailerLen
	if userKeyLen < 0 {
		return InternalKey{}, false
	}
	return InternalKey{UserKey: encoded[:userKeyLen:userKeyLen], Trailer: binary.LittleEndian.Uint64(encoded[userKeyLen:])}, true
}

func InternalCompare(compareUserKeys Compare, a, b InternalKey) int {
	if order := compareUserKeys(a.UserKey, b.UserKey); order != 0 {
		return order
	}
	switch {
	case a.Trailer > b.Trailer:
		return -1
	case a.Trailer < b.Trailer:
		return 1
	}
	return 0
}

// CloneBytes keeps nil and empty distinct, since some APIs read nil as "unbounded".
func CloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	clone := make([]byte, len(src))
	copy(clone, src)
	return clone
}

func SharedPrefixLen(a, b []byte) int {
	maxLen := min(len(a), len(b))
	i := 0
	for i < maxLen && a[i] == b[i] {
		i++
	}
	return i
}

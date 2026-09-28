package base

import "bytes"

type Compare func(a, b []byte) int

type Comparer struct {
	Name    string
	Compare Compare

	// Separator appends a short key in [start, limit) to dst; nil keeps index keys unshortened.
	Separator func(dst, start, limit []byte) []byte

	// Successor appends a short key at or after key to dst; it may be nil.
	Successor func(dst, key []byte) []byte
}

var DefaultComparer = &Comparer{
	Name:      "lsmkv.BytewiseComparator",
	Compare:   bytes.Compare,
	Separator: bytewiseSeparator,
	Successor: bytewiseSuccessor,
}

func bytewiseSeparator(dst, start, limit []byte) []byte {
	shared := SharedPrefixLen(start, limit)
	if shared >= len(start) || shared >= len(limit) {
		return append(dst, start...)
	}
	if diffByte := start[shared]; diffByte < 0xff && diffByte+1 < limit[shared] {
		dst = append(dst, start[:shared+1]...)
		dst[len(dst)-1]++
		return dst
	}
	return append(dst, start...)
}

func bytewiseSuccessor(dst, key []byte) []byte {
	for i, keyByte := range key {
		if keyByte != 0xff {
			dst = append(dst, key[:i+1]...)
			dst[len(dst)-1]++
			return dst
		}
	}
	return append(dst, key...)
}

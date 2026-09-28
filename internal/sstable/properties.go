package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

// Properties are encoded as a block of name -> uvarint or string entries;
// readers skip unknown names, so properties can be added without a format change.
type Properties struct {
	NumEntries    uint64
	NumDeletions  uint64
	NumMerges     uint64
	NumDataBlocks uint64
	SmallestSeq   uint64
	LargestSeq    uint64
	RawKeyBytes   uint64
	RawValueBytes uint64
	DataSize      uint64
	IndexSize     uint64
	FilterSize    uint64
	ComparerName  string
	FilterPolicy  string
	Compression   string
}

type propertyField struct {
	name   string
	number *uint64
	text   *string
}

func (p *Properties) fields() []propertyField {
	return []propertyField{
		{name: "comparer", text: &p.ComparerName},
		{name: "compression", text: &p.Compression},
		{name: "data_size", number: &p.DataSize},
		{name: "filter_policy", text: &p.FilterPolicy},
		{name: "filter_size", number: &p.FilterSize},
		{name: "index_size", number: &p.IndexSize},
		{name: "largest_seq", number: &p.LargestSeq},
		{name: "num_data_blocks", number: &p.NumDataBlocks},
		{name: "num_deletions", number: &p.NumDeletions},
		{name: "num_entries", number: &p.NumEntries},
		{name: "num_merges", number: &p.NumMerges},
		{name: "raw_key_bytes", number: &p.RawKeyBytes},
		{name: "raw_value_bytes", number: &p.RawValueBytes},
		{name: "smallest_seq", number: &p.SmallestSeq},
	}
}

func (p *Properties) encode() []byte {
	fields := p.fields()
	sort.Slice(fields, func(i, j int) bool { return fields[i].name < fields[j].name })
	writer := blockWriter{restartInterval: 1}
	var value []byte
	for _, field := range fields {
		value = value[:0]
		if field.number != nil {
			value = binary.AppendUvarint(value, *field.number)
		} else {
			value = append(value, *field.text...)
		}
		writer.add([]byte(field.name), value)
	}
	return bytes.Clone(writer.finish())
}

func decodeProperties(block []byte) (Properties, error) {
	var props Properties
	byName := make(map[string]propertyField)
	for _, field := range props.fields() {
		byName[field.name] = field
	}
	var it blockIter
	if err := it.init(bytes.Compare, block, true); err != nil {
		return props, err
	}
	for it.First(); it.Valid(); it.Next() {
		field, ok := byName[string(it.Key().UserKey)]
		if !ok {
			continue
		}
		if field.number != nil {
			value, n := binary.Uvarint(it.Value())
			if n <= 0 || n != len(it.Value()) {
				return props, base.CorruptionErrorf("bad value for property %q", field.name)
			}
			*field.number = value
		} else {
			*field.text = string(it.Value())
		}
	}
	return props, it.Error()
}

func (p Properties) String() string {
	var out strings.Builder
	fields := p.fields()
	for _, field := range fields {
		if field.number != nil {
			fmt.Fprintf(&out, "  %s: %d\n", field.name, *field.number)
		} else {
			fmt.Fprintf(&out, "  %s: %q\n", field.name, *field.text)
		}
	}
	return out.String()
}

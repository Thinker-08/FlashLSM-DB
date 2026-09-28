package sstable

import (
	"fmt"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
)

// Compression values are stored in block trailers; never renumber them.
type Compression uint8

const (
	NoCompression     Compression = 0
	SnappyCompression Compression = 1
	ZstdCompression   Compression = 2
)

func (c Compression) String() string {
	switch c {
	case NoCompression:
		return "none"
	case SnappyCompression:
		return "snappy"
	case ZstdCompression:
		return "zstd"
	}
	return fmt.Sprintf("compression(%d)", uint8(c))
}

// maxDecodedBlock stops hostile length prefixes from forcing huge allocations;
// it must exceed lsmkv.MaxBatchSize, since one block can hold a maximal value.
const maxDecodedBlockSize = 128 << 20

var (
	zstdOnce    sync.Once
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder
	zstdInitErr error
)

func initZstd() {
	zstdEncoder, zstdInitErr = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(false)) // blocks carry their own CRC
	if zstdInitErr != nil {
		return
	}
	zstdDecoder, zstdInitErr = zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(0),
		zstd.WithDecoderMaxMemory(maxDecodedBlockSize))
}

func compressBlock(compression Compression, dst, raw []byte) ([]byte, Compression) {
	var compressed []byte
	switch compression {
	case SnappyCompression:
		compressed = snappy.Encode(dst[:cap(dst)], raw)
	case ZstdCompression:
		zstdOnce.Do(initZstd)
		if zstdInitErr != nil {
			return raw, NoCompression
		}
		compressed = zstdEncoder.EncodeAll(raw, dst[:0])
	default:
		return raw, NoCompression
	}
	// Saving less than an eighth is not worth decompressing on reads.
	if len(compressed) > len(raw)-len(raw)/8 {
		return raw, NoCompression
	}
	return compressed, compression
}

func decompressBlock(compression Compression, payload, dst []byte) ([]byte, error) {
	switch compression {
	case NoCompression:
		return payload, nil
	case SnappyCompression:
		decodedLen, err := snappy.DecodedLen(payload)
		if err != nil {
			return nil, base.CorruptionErrorf("snappy block: %v", err)
		}
		if decodedLen > maxDecodedBlockSize {
			return nil, base.CorruptionErrorf("snappy block decodes to %d bytes", decodedLen)
		}
		if cap(dst) < decodedLen {
			dst = make([]byte, decodedLen)
		}
		decoded, err := snappy.Decode(dst[:decodedLen], payload)
		if err != nil {
			return nil, base.CorruptionErrorf("snappy block: %v", err)
		}
		return decoded, nil
	case ZstdCompression:
		zstdOnce.Do(initZstd)
		if zstdInitErr != nil {
			return nil, fmt.Errorf("sstable: zstd unavailable: %w", zstdInitErr)
		}
		decoded, err := zstdDecoder.DecodeAll(payload, dst[:0])
		if err != nil {
			return nil, base.CorruptionErrorf("zstd block: %v", err)
		}
		return decoded, nil
	}
	return nil, base.CorruptionErrorf("unknown block compression type %d", uint8(compression))
}

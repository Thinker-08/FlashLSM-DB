package bloom

import "math"

// HashName is stored in table properties; change it whenever the hash or probing changes.
const HashName = "fnv1a64-fmix64"

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
	maxProbes   = 30
	minBits     = 64
)

// Raw FNV-1a mixes its last bytes poorly, so fmix64 is needed to keep false positives low.
func Hash(key []byte) uint64 { return fmix64(FNV1a(key)) }

func FNV1a(data []byte) uint64 {
	hash := uint64(fnvOffset64)
	for _, dataByte := range data {
		hash ^= uint64(dataByte)
		hash *= fnvPrime64
	}
	return hash
}

func fmix64(hash uint64) uint64 {
	hash ^= hash >> 33
	hash *= 0xff51afd7ed558ccd
	hash ^= hash >> 33
	hash *= 0xc4ceb9fe1a85ec53
	hash ^= hash >> 33
	return hash
}

func Probes(bitsPerKey int) int {
	probes := int(math.Round(float64(bitsPerKey) * math.Ln2))
	return max(1, min(probes, maxProbes))
}

type Builder struct {
	bitsPerKey int
	hashes     []uint64
}

func NewBuilder(bitsPerKey int) *Builder {
	return &Builder{bitsPerKey: bitsPerKey}
}

func (b *Builder) Add(key []byte) { b.hashes = append(b.hashes, Hash(key)) }

func (b *Builder) Len() int { return len(b.hashes) }

func (b *Builder) Finish(dst []byte) []byte {
	probes := Probes(b.bitsPerKey)
	numBits := max(len(b.hashes)*b.bitsPerKey, minBits)
	numBytes := (numBits + 7) / 8
	numBits = numBytes * 8

	start := len(dst)
	dst = append(dst, make([]byte, numBytes+1)...)
	bits := dst[start : start+numBytes]
	totalBits := uint64(numBits)
	for _, hash := range b.hashes {
		baseHash, stepHash := uint64(uint32(hash)), uint64(hash>>32)
		for i := uint64(0); i < uint64(probes); i++ {
			bit := (baseHash + i*stepHash) % totalBits
			bits[bit/8] |= 1 << (bit % 8)
		}
	}
	dst[start+numBytes] = byte(probes)
	b.hashes = b.hashes[:0]
	return dst
}

// Filter encoding: bit array | probe count (1 byte).
type Filter []byte

func (f Filter) MayContain(key []byte) bool {
	if len(f) < 2 {
		return true
	}
	numBytes := len(f) - 1
	probes := int(f[numBytes])
	if probes < 1 || probes > maxProbes {
		return true
	}
	totalBits := uint64(numBytes) * 8
	hash := Hash(key)
	baseHash, stepHash := uint64(uint32(hash)), uint64(hash>>32)
	for i := uint64(0); i < uint64(probes); i++ {
		bit := (baseHash + i*stepHash) % totalBits
		if f[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

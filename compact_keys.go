package lsmkv

import (
	"log/slog"
	"slices"

	"github.com/Thinker-08/FlashLSM-DB/internal/base"
	"github.com/Thinker-08/FlashLSM-DB/internal/compaction"
)

type keyProcessor struct {
	db                *DB
	job               *compaction.Compaction
	output            *compactionWriter
	it                base.InternalIterator
	stats             *compactionStats
	oldestSnapshotSeq uint64
	applyFilter       bool
	now               int64

	userKey       []byte
	pendingMerges []pendingEntry
}

type pendingEntry struct {
	key   base.InternalKey
	value []byte
}

func (kp *keyProcessor) processKey() error {
	kp.userKey = append(kp.userKey[:0], kp.it.Key().UserKey...)
	userKey := kp.userKey
	compare := kp.db.compare
	shadowed := false
	isNewest := true
	for ; kp.it.Valid() && compare(kp.it.Key().UserKey, userKey) == 0; isNewest = false {
		key, value := kp.it.Key(), kp.it.Value()
		if shadowed {
			kp.stats.dropped++
			kp.it.Next()
			continue
		}
		if key.SeqNum() > kp.oldestSnapshotSeq {
			// Some snapshot may still need the older versions.
			if err := kp.output.add(key, value); err != nil {
				return err
			}
			kp.it.Next()
			continue
		}
		kind := key.Kind()
		switch kind {
		case base.KindSetTTL, base.KindSet:
			userValue := value
			if kind == base.KindSetTTL {
				unwrapped, live, err := decodeTTL(value, kp.now)
				if err != nil {
					return err
				}
				if !live {
					kind = base.KindDelete
				}
				userValue = unwrapped
			}
			if kind != base.KindDelete && isNewest && kp.applyFilter && kp.db.opts.CompactionFilter(kp.job.OutputLevel, userKey, userValue) {
				kind = base.KindDelete
			}
			if kind != base.KindDelete {
				if err := kp.output.add(key, value); err != nil {
					return err
				}
				shadowed = true
				kp.it.Next()
				continue
			}
			fallthrough
		case base.KindDelete:
			shadowed = true
			// Dropping a tombstone above the base level would resurrect older values.
			if kp.job.IsBaseLevelForKey(userKey) {
				kp.stats.dropped++
			} else if err := kp.output.add(base.MakeInternalKey(userKey, key.SeqNum(), base.KindDelete), nil); err != nil {
				return err
			}
			kp.it.Next()
		case base.KindMerge:
			folded, err := kp.foldMerge()
			if err != nil {
				return err
			}
			shadowed = folded
		default:
			return base.CorruptionErrorf("entry %s has an unknown kind", key)
		}
	}
	return nil
}

func (kp *keyProcessor) foldMerge() (shadowed bool, err error) {
	compare := kp.db.compare
	userKey := kp.userKey
	kp.pendingMerges = kp.pendingMerges[:0]
	for kp.it.Valid() && compare(kp.it.Key().UserKey, userKey) == 0 && kp.it.Key().Kind() == base.KindMerge {
		kp.pendingMerges = append(kp.pendingMerges, pendingEntry{key: kp.it.Key().Clone(), value: slices.Clone(kp.it.Value())})
		kp.it.Next()
	}
	var (
		baseValue       []byte
		exists, canFold bool
		baseEntry       *pendingEntry
	)
	if kp.it.Valid() && compare(kp.it.Key().UserKey, userKey) == 0 {
		key, value := kp.it.Key(), kp.it.Value()
		baseEntry = &pendingEntry{key: key.Clone(), value: slices.Clone(value)}
		canFold = true
		switch key.Kind() {
		case base.KindSet:
			baseValue, exists = baseEntry.value, true
		case base.KindSetTTL:
			// Folding onto an expiring value would use the compaction's clock, not the reader's.
			canFold = false
		case base.KindDelete:
		default:
			return false, base.CorruptionErrorf("entry %s has an unknown kind", key)
		}
		kp.it.Next()
	} else if kp.job.IsBaseLevelForKey(userKey) {
		canFold = true
	}

	if canFold && kp.db.opts.Merger != nil {
		operands := make([][]byte, len(kp.pendingMerges))
		for i, entry := range kp.pendingMerges {
			operands[len(operands)-1-i] = entry.value
		}
		merged, err := kp.db.opts.Merger.FullMerge(userKey, baseValue, exists, operands)
		if err == nil {
			kp.stats.dropped += int64(len(kp.pendingMerges))
			return true, kp.output.add(base.MakeInternalKey(userKey, kp.pendingMerges[0].key.SeqNum(), base.KindSet), merged)
		}
		kp.db.logf(slog.LevelWarn, "lsmkv: merge operator failed during compaction; keeping operands", "key", userKey, "err", err)
	}
	for _, entry := range kp.pendingMerges {
		if err := kp.output.add(entry.key, entry.value); err != nil {
			return false, err
		}
	}
	if baseEntry != nil {
		if baseEntry.key.Kind() == base.KindDelete && kp.job.IsBaseLevelForKey(userKey) {
			kp.stats.dropped++
		} else if err := kp.output.add(baseEntry.key, baseEntry.value); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

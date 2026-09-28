package lsmkv

import (
	"errors"
	"time"
)

var errForeignSnapshot = errors.New("lsmkv: snapshot belongs to another database")

// Snapshot stops compaction from dropping versions it can see until Release, so a long-lived one
// keeps old data on disk. It may be shared between goroutines.
type Snapshot struct {
	db       *DB
	seq      uint64
	created  time.Time
	released bool
	prev     *Snapshot
	next     *Snapshot
}

func (db *DB) NewSnapshot() *Snapshot {
	db.mu.Lock()
	defer db.mu.Unlock()
	snapshot := &Snapshot{db: db, seq: db.visibleSeq.Load(), created: time.Now()}
	db.snapshots.pushBack(snapshot)
	return snapshot
}

// Release is idempotent.
func (s *Snapshot) Release() {
	db := s.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if !s.released {
		s.released = true
		db.snapshots.remove(s)
	}
}

// snapshotList is oldest first; appending keeps it sorted, as new snapshots have the highest seq.
type snapshotList struct {
	root  Snapshot
	count int
}

func (sl *snapshotList) init() {
	sl.root.prev = &sl.root
	sl.root.next = &sl.root
}

func (sl *snapshotList) pushBack(snapshot *Snapshot) {
	snapshot.prev = sl.root.prev
	snapshot.next = &sl.root
	snapshot.prev.next = snapshot
	sl.root.prev = snapshot
	sl.count++
}

func (sl *snapshotList) remove(snapshot *Snapshot) {
	snapshot.prev.next = snapshot.next
	snapshot.next.prev = snapshot.prev
	snapshot.prev, snapshot.next = nil, nil
	sl.count--
}

func (sl *snapshotList) oldest() *Snapshot {
	if sl.count == 0 {
		return nil
	}
	return sl.root.next
}

// Compaction may drop a version only if a newer one is visible at the returned sequence number.
func (db *DB) oldestSnapshotSeqLocked() uint64 {
	if oldest := db.snapshots.oldest(); oldest != nil {
		return oldest.seq
	}
	return db.visibleSeq.Load()
}

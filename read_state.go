package lsmkv

import (
	"slices"
	"sync/atomic"

	"github.com/Thinker-08/FlashLSM-DB/internal/manifest"
	"github.com/Thinker-08/FlashLSM-DB/internal/memtable"
)

type readState struct {
	refs               atomic.Int32
	activeMemtable     *memtable.Memtable
	immutableMemtables []*memtable.Memtable // oldest first
	version            *manifest.Version
}

func (rs *readState) unref() {
	if rs.refs.Add(-1) == 0 {
		rs.version.Unref()
	}
}

func (db *DB) loadReadState() *readState {
	db.readStateMu.Lock()
	state := db.readState
	state.refs.Add(1)
	db.readStateMu.Unlock()
	return state
}

func (db *DB) updateReadStateLocked() {
	state := &readState{
		activeMemtable:     db.activeMemtable,
		immutableMemtables: slices.Clone(db.immutableMemtables),
		version:            db.versions.Current(),
	}
	state.refs.Store(1)
	state.version.Ref()
	db.readStateMu.Lock()
	previous := db.readState
	db.readState = state
	db.readStateMu.Unlock()
	if previous != nil {
		previous.unref()
	}
}

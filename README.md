# FlashLSM-DB

[![CI](https://github.com/Thinker-08/FlashLSM-DB/actions/workflows/ci.yml/badge.svg)](https://github.com/Thinker-08/FlashLSM-DB/actions/workflows/ci.yml)

`lsmkv` is an embeddable, crash-safe, ordered key-value storage engine in pure
Go, built on a log-structured merge tree and modeled on LevelDB and RocksDB. It
is a library your program imports, like SQLite or BoltDB, not a server.

```go
import lsmkv "github.com/Thinker-08/FlashLSM-DB"

db, err := lsmkv.Open("/var/lib/myapp/db", nil) // nil means DefaultOptions()
if err != nil {
	return err
}
defer db.Close()

db.Put([]byte("user:1"), []byte("ada"), nil)
db.Put([]byte("user:2"), []byte("grace"), lsmkv.Sync) // survives power loss

v, err := db.Get([]byte("user:1"), nil) // errors.Is(err, lsmkv.ErrNotFound) if absent

it := db.NewIterator(&lsmkv.ReadOptions{LowerBound: []byte("user:"), UpperBound: []byte("user;")})
defer it.Close()
for k, v := range it.All() {
	fmt.Printf("%s = %s\n", k, v)
}
```

## What it does

| Area | Details |
|---|---|
| Point operations | `Put`, `Get`, `Delete` on arbitrary byte-slice keys (up to 64 KiB) and values |
| Atomic batches | A `Batch` becomes visible all at once or not at all, and survives or is lost as a unit |
| Range scans | Forward and reverse iteration with lower and upper bounds, `iter.Seq2` support |
| Snapshots | Repeatable reads pinned to a sequence number |
| Durability | Checksummed write-ahead log: every write survives `kill -9`, and writes with `Sync: true` also survive power loss |
| SSTables | Prefix-compressed blocks, a block index, bloom filters, per-block CRC-32C |
| Compaction | Leveled by default or size-tiered by option, on background goroutines with write stalls as backpressure |
| Caching | A sharded LRU block cache and a reference-counted table cache |
| Advanced | Snappy and zstd compression, a merge operator, TTL writes, compaction filters, `CompactRange`, checkpoints, metrics |

[HOW_IT_WORKS.md](HOW_IT_WORKS.md) walks through every path step by step: writes,
reads, flushes, compaction, recovery and the on-disk formats.

## Architecture

```
            Put / Delete / Write(batch)
                      │
         1. append ───┼──────────────► WAL (000009.log)   deleted once its memtable is flushed
                      │ 2. insert
                      ▼
               active memtable ──full──► immutable memtable
                (skip list)                    │ flush
   memory ─────────────────────────────────────┼────────────────────────────────
   disk                                        ▼
                                   L0  overlapping SSTables
                                        │ compaction
                                   L1  sorted, non-overlapping      MANIFEST: which table
                                        │                            lives at which level
                                   L2 … L6  each ~10× larger

   Get: active memtable → immutable memtables → L0 newest first → one file per level
```

The engine is one public `DB` type over small internal packages, each owning one
on-disk format or one in-memory structure:

| Package | Owns |
|---|---|
| `lsmkv` (root) | `DB`, options, batches, iterators, snapshots; write path, group commit, flush, compaction loop, recovery, table cache, metrics |
| [`vfs`](vfs) | The filesystem interface; the OS implementation, an in-memory filesystem that produces crash images, and a fault injector |
| [`internal/base`](internal/base) | Internal key encoding, kinds, comparer, file names, checksums |
| [`internal/memtable`](internal/memtable) | Skip list with one writer and lock-free readers, slab-allocated |
| [`internal/record`](internal/record) | Checksummed record framing shared by the WAL and the MANIFEST |
| [`internal/sstable`](internal/sstable) | Block and table formats, writer, reader, two-level iterator, compression, `Dump` |
| [`internal/bloom`](internal/bloom) | Bloom filter builder and probe |
| [`internal/cache`](internal/cache) | Sharded LRU block cache |
| [`internal/manifest`](internal/manifest) | File metadata, immutable versions, version edits, the version set and `LogAndApply`, `Dump` |
| [`internal/compaction`](internal/compaction) | The `Picker` interface with leveled and size-tiered pickers |
| [`internal/iterator`](internal/iterator) | Merging and level (concatenating) iterators |

A database directory holds `LOCK`, `CURRENT` (the name of the live MANIFEST),
`MANIFEST-NNNNNN` (a log of version edits), `NNNNNN.log` (WALs) and `NNNNNN.sst`
(tables). All files share one increasing file-number counter, so a name always
identifies one immutable file.

### Concurrency

Readers take a reference to the current memtables and version and never block
writers. Concurrent writers join a FIFO queue; the writer at its head commits
every queued batch with one WAL append and at most one fsync (group commit, as
in LevelDB). One flusher and one compactor run in the background. The rule that
keeps this simple: no I/O while `db.mu` is held. Long work runs on immutable,
reference-counted state, and only the final install takes the lock.

## Design decisions

The implementation follows the design plan closely. These are the decisions
that go beyond it, or refine it, and why.

**The durability contract.** An asynchronous write costs one `write` syscall, so
it survives a process crash as the plan requires. A `Sync` write adds one fsync
per commit group. A new WAL's directory entry is synced lazily, before the first
synced write to it returns; a flush syncs the directory before any MANIFEST edit
names the WAL anyway. That removes a directory fsync per memtable rotation for
asynchronous workloads without weakening anything. Pebble goes further and
buffers asynchronous WAL writes in user space, which is faster but can lose
recent asynchronous writes to `kill -9`.

**Recovery keeps a clean prefix of history.** A torn record at the end of the
newest WAL is a torn write: recovery keeps everything before it. Every older WAL
was synced before its successor took writes, so damage there is real corruption
(`ErrCorruption`, unless `BestEffortRecovery` is set). A torn MANIFEST tail is
dropped, and Open then checks for the traces that lost edits would leave, a
referenced table that is missing or a missing WAL with newer WALs present, so a
MANIFEST damaged deeper inside fails loudly instead of losing data. Recovery
validates a whole batch before applying any of it, flushes what it recovered and
records the new WAL number before creating that WAL, so it never appends after a
torn tail.

**No garbage collection after a failed MANIFEST write.** If an fsync fails, the
data may or may not have reached the disk, so an edit that "failed" can still be
replayed on reopen. After any background error the engine therefore deletes
nothing, including the outputs of the failed flush or compaction; the next Open
sorts out what is referenced.

**Bloom hash.** The plan names 64-bit FNV-1a. Raw FNV-1a mixes its last input
bytes poorly: on sequential big-endian integer keys it gave a 5.2% false-positive
rate at 10 bits per key. The filter now applies murmur3's
`fmix64` finalizer to the FNV-1a hash, which brings every key shape to about
0.8%, the theoretical rate. The hash name is recorded in each table's properties,
so this can change again without misreading old files.

**Flushes skip down to L1 or L2** when nothing there overlaps, as in LevelDB.
With a separate flusher and compactor that is only safe if the choice cannot
race a compaction, so compaction picking holds the manifest lock and a flush
refuses any level at or below the output of an overlapping running compaction.

**Merge operands and TTL.** Merge operands are folded into their base value
during compaction when every snapshot sees them, and resolved lazily at read
time otherwise. An expired TTL entry becomes a tombstone at the same sequence
number during compaction, so readers see no change and older versions stay
hidden. Operands stacked on an expiring value are never folded, because the
result would depend on when compaction ran rather than when the key is read.

**Compaction filters** run only when no snapshot is open, and only on the newest
version of a key in the compaction, so they cannot change what a snapshot sees.

**Size-tiered compaction** treats each L0 file and each non-empty deeper level as
one sorted run, ordered by age, and merges adjacent runs of similar size; the
output lands at the level of its oldest input so age order, and with it the
tombstone rules, is preserved. A full merge triggers when the runs above the
oldest reach twice its size, and merging all of L0 is the fallback that keeps
writes from stalling for good.

**Memory.** The memtable allocates nodes, next pointers and key/value bytes
from typed slabs, so an insert usually costs no allocation. Blocks read without
filling the cache, such as compaction inputs and `DontFillCache` scans, reuse
one buffer per iterator instead of allocating one per block.

## Development

```sh
make              # gofmt check, vet and build
make staticcheck
```

The library needs Go 1.23 or newer. Its only dependencies are the Snappy and zstd
codecs. [CONVENTIONS.md](CONVENTIONS.md) sets the naming and file-layout rules
every package follows.

## Scope

Supported on Linux and macOS. Not goals: distribution or replication, a CLI or
network server, a query language or transactions beyond atomic batches, more
than one process per directory (the `LOCK` file prevents it), and on-disk
compatibility with LevelDB or RocksDB.

Planned but not built: range deletions, key-value separation (WiscKey),
parallel compactions, seek-triggered compaction, a block-format WAL, prefix bloom
filters, a background I/O rate limiter and an arena memtable.

# How lsmkv works

A plain-language guide to how this key-value store is built: what each part
does, how data moves between the parts, and which file in the code does it.

Most sections have the same shape: **In short** (one or two sentences), a
picture, the details, and **In the code** (where to look).

> **What lsmkv is.** A key-value database that runs inside your Go program. You
> call `Put`, `Get` and `Delete`; lsmkv keeps the data in one directory and
> keeps it safe across crashes. It is a library, not a server. It uses the same
> design as LevelDB and RocksDB, called a *log-structured merge tree* (LSM tree).

**Where to start:** sections 1 and 2 give the big picture and show how to run
it. Section 3 explains the words used everywhere else. Sections 5 to 14 follow
each operation step by step. Sections 15 to 21 are for looking things up.

## Contents

1. [The big picture](#1-the-big-picture)
2. [Running it yourself](#2-running-it-yourself)
3. [Vocabulary](#3-vocabulary)
4. [What is on disk](#4-what-is-on-disk)
5. [Opening a database: recovery](#5-opening-a-database-recovery)
6. [The write path](#6-the-write-path)
7. [The read path](#7-the-read-path)
8. [Flushing a memtable](#8-flushing-a-memtable)
9. [Compaction](#9-compaction)
10. [The MANIFEST and versions](#10-the-manifest-and-versions)
11. [Deleting obsolete files](#11-deleting-obsolete-files)
12. [Errors](#12-errors)
13. [Closing](#13-closing)
14. [Checkpoints](#14-checkpoints)
15. [Caches](#15-caches)
16. [Concurrency](#16-concurrency)
17. [Durability: what survives what](#17-durability-what-survives-what)
18. [On-disk formats](#18-on-disk-formats)
19. [Options](#19-options)
20. [Metrics](#20-metrics)
21. [Code map](#21-code-map)

---

## 1. The big picture

**In short:** a write goes into memory (fast) and into a log file (safe). When
memory fills up, it is saved to disk as a sorted file. In the background,
sorted files are merged, which keeps reads fast and throws away old data.

```text
                         your program
               Put / Delete / Merge / Write(batch)
                              │
             ┌────────────────┴────────────────┐
             ▼                                 ▼
    ┌──────────────────┐             ┌──────────────────┐
    │ WAL file         │             │ memtable         │  memory:
    │ a log of every   │             │ recent writes,   │  fast
    │ write, in order  │             │ kept sorted      │
    └──────────────────┘             └────────┬─────────┘
     deleted once its                         │ full (4 MiB)
     memtable is saved                        ▼
                                     ┌──────────────────┐
                                     │ immutable        │  memory:
                                     │ memtable         │  read-only
                                     └────────┬─────────┘
  ════════════════════════════════════════════╪════════════ disk ═════
                                              │ flush
                                              ▼
  L0      [sst] [sst] [sst]          newest tables; they may overlap
             │
             │  compaction: merge tables and clean up
             ▼
  L1      [sst][sst][sst][sst]       sorted; no two tables overlap
             │
             ▼
  L2…L6   [sst][sst][sst][sst][sst][sst]…      each level ~10× bigger
```

An **SSTable** (sorted string table, the `.sst` files) is a sorted file that is
never changed after it is written. A small log called the **MANIFEST** records
which SSTables are in which level.

| Part | What it does | In the code |
|---|---|---|
| WAL (write-ahead log) | Records every write as it happens, so nothing is lost in a crash | `write.go`, `internal/record` |
| Memtable | Holds recent writes, sorted, in memory | `internal/memtable` |
| Flush | Saves a full memtable as an SSTable | `flush.go` |
| SSTable | A sorted, immutable file on disk | `internal/sstable` |
| Compaction | Merges SSTables; drops old versions and deleted keys | `compact.go`, `compact_keys.go` |
| MANIFEST | The list of SSTables per level | `internal/manifest` |

**Reads** look at the newest data first (memtable, then L0, then L1 and down)
and stop at the first answer.

Three kinds of goroutine do the work:

```text
  your goroutines ──► write queue ──► commit leader
                                      the first writer in line writes its
                                      batch and those queued behind it,
                                      then wakes their writers

  flushLoop     1 goroutine    full memtable  ──► new SSTable
  compactLoop   1 goroutine    many SSTables  ──► fewer, merged SSTables
```

---

## 2. Running it yourself

**In short:** lsmkv is a library, so you run it by writing a small Go program
that uses it. The demo below uses every feature, and can fake a crash so you
can watch the data come back.

```text
  go run .                go run . crash              go run . recover
  ────────                ──────────────              ────────────────
  tries every feature,    writes 5000 keys, then      reopens the database:
  then closes cleanly     exits WITHOUT Close,        the WAL is replayed and
                          like kill -9                all 5000 keys are back
```

### What you need

- Go 1.23 or newer (`go version`).
- This repository checked out, for example at `~/code/FlashLSM-DB`.

To check the library itself: `make` (format check, vet, build) and
`make staticcheck`.

### Step 1: create a module next to the repository

```sh
mkdir ~/code/lsmkv-demo && cd ~/code/lsmkv-demo
go mod init lsmkv-demo
go mod edit -replace github.com/Thinker-08/FlashLSM-DB=../FlashLSM-DB
```

The `replace` line tells Go to use your local checkout instead of downloading
the module. Change `../FlashLSM-DB` if your checkout is somewhere else.

### Step 2: save the demo as `main.go`

<details>
<summary><b>main.go</b> (click to expand)</summary>

```go
package main

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"
	"time"

	lsmkv "github.com/Thinker-08/FlashLSM-DB"
)

const dir = "demo-db"

func main() {
	mode := "tour"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	db, err := lsmkv.Open(dir, options())
	check(err)
	switch mode {
	case "tour":
		tour(db)
		check(db.Close())
		listFiles()
	case "crash":
		for i := 0; i < 5000; i++ {
			check(db.Put(key(i), []byte("written before the crash"), nil))
		}
		fmt.Println("wrote 5000 keys; exiting without Close, like kill -9")
		os.Exit(1)
	case "recover":
		v, err := db.Get(key(4999), nil)
		check(err)
		fmt.Printf("after reopening, %s = %q\n", key(4999), v)
		check(db.Close())
	default:
		log.Fatalf("usage: go run . [tour|crash|recover]")
	}
}

// Small sizes make flushes and compactions happen within a few thousand writes.
func options() *lsmkv.Options {
	o := lsmkv.DefaultOptions()
	o.MemtableSize = 64 << 10
	o.TargetFileSize = 64 << 10
	o.L1MaxBytes = 256 << 10
	o.Compression = lsmkv.SnappyCompression
	o.Merger = &lsmkv.Merger{Name: "counter", FullMerge: addCounts}
	o.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return o
}

func addCounts(key, existing []byte, exists bool, operands [][]byte) ([]byte, error) {
	total := 0
	if exists {
		total, _ = strconv.Atoi(string(existing))
	}
	for _, op := range operands {
		n, err := strconv.Atoi(string(op))
		if err != nil {
			return nil, err
		}
		total += n
	}
	return []byte(strconv.Itoa(total)), nil
}

func tour(db *lsmkv.DB) {
	check(db.Put([]byte("user:1"), []byte("ada"), nil))
	check(db.Put([]byte("user:2"), []byte("grace"), lsmkv.Sync))
	v, err := db.Get([]byte("user:1"), nil)
	check(err)
	fmt.Printf("get user:1 = %s\n", v)

	check(db.Delete([]byte("user:2"), nil))
	_, err = db.Get([]byte("user:2"), nil)
	fmt.Println("get user:2 after delete: not found =", errors.Is(err, lsmkv.ErrNotFound))

	b := lsmkv.NewBatch()
	b.Put([]byte("acct:alice"), []byte("70"))
	b.Put([]byte("acct:bob"), []byte("30"))
	check(db.Write(b, lsmkv.Sync))
	fmt.Println("batch of 2 applied atomically")

	for i := 0; i < 3; i++ {
		check(db.Merge([]byte("visits"), []byte("1"), nil))
	}
	v, err = db.Get([]byte("visits"), nil)
	check(err)
	fmt.Printf("counter after 3 merges = %s\n", v)

	check(db.PutWithTTL([]byte("session"), []byte("token"), 100*time.Millisecond, nil))
	time.Sleep(150 * time.Millisecond)
	_, err = db.Get([]byte("session"), nil)
	fmt.Println("session after its TTL: not found =", errors.Is(err, lsmkv.ErrNotFound))

	snap := db.NewSnapshot()
	check(db.Put([]byte("user:1"), []byte("lovelace"), nil))
	old, err := db.Get([]byte("user:1"), &lsmkv.ReadOptions{Snapshot: snap})
	check(err)
	cur, err := db.Get([]byte("user:1"), nil)
	check(err)
	fmt.Printf("user:1 in the snapshot = %s, now = %s\n", old, cur)
	snap.Release()

	for i := 0; i < 20000; i++ {
		check(db.Put(key(i), []byte(fmt.Sprintf("value-%06d-%s", i, "padding-to-fill-memtables")), nil))
	}
	fmt.Println("loaded 20000 keys")

	it := db.NewIterator(&lsmkv.ReadOptions{LowerBound: key(100), UpperBound: key(103)})
	for k, v := range it.All() {
		fmt.Printf("  scan %s = %s\n", k, v)
	}
	check(it.Close())

	check(db.CompactRange(nil, nil))
	m := db.Metrics()
	fmt.Printf("flushes %d, compactions %d, tables on disk %d, write amplification %.2f\n",
		m.Flush.Count, m.Compaction.Count, tables(m), m.WriteAmplification())

	check(os.RemoveAll(dir + "-checkpoint"))
	check(db.Checkpoint(dir + "-checkpoint"))
	fmt.Println("checkpoint written to", dir+"-checkpoint")
}

func tables(m lsmkv.Metrics) int {
	n := 0
	for _, l := range m.Levels {
		n += l.NumFiles
	}
	return n
}

func listFiles() {
	entries, err := os.ReadDir(dir)
	check(err)
	fmt.Printf("files in %s:", dir)
	for _, e := range entries {
		fmt.Printf(" %s", e.Name())
	}
	fmt.Println()
}

func key(i int) []byte { return []byte(fmt.Sprintf("key%06d", i)) }

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
```

</details>

### Step 3: run the tour

```sh
go mod tidy
go run .
```

On an empty directory it prints:

```text
get user:1 = ada
get user:2 after delete: not found = true
batch of 2 applied atomically
counter after 3 merges = 3
session after its TTL: not found = true
user:1 in the snapshot = ada, now = lovelace
loaded 20000 keys
  scan key000100 = value-000100-padding-to-fill-memtables
  scan key000101 = value-000101-padding-to-fill-memtables
  scan key000102 = value-000102-padding-to-fill-memtables
flushes 40, compactions 5, tables on disk 4, write amplification 0.76
checkpoint written to demo-db-checkpoint
files in demo-db: 000081.log 000087.sst 000088.sst 000089.sst 000090.sst CURRENT LOCK MANIFEST-000002
```

The numbers can differ a little between runs, because flushes and compactions
run in the background. Write amplification is below 1 because Snappy
compresses the repetitive values.

### Step 4: crash it, then recover

```sh
go run . crash     # writes 5000 keys, then exits without Close
go run . recover   # reopens the database; the WAL is replayed
```

```text
wrote 5000 keys; exiting without Close, like kill -9
exit status 1
after reopening, key004999 = "written before the crash"
```

Nothing was flushed before the exit, but every write had already reached the
WAL, and the operating system keeps it even when the program dies. `Open` read
the WAL back (section 5).

### Things to try

- **Watch compactions:** change `slog.LevelInfo` to `slog.LevelDebug`.
- **Other strategy:** `o.CompactionStrategy = lsmkv.SizeTieredCompaction`.
- **Other compression:** `o.Compression = lsmkv.ZstdCompression`.
- **See all counters:** `fmt.Print(db.Metrics())`.
- **Open the copy:** `lsmkv.Open("demo-db-checkpoint", nil)`.
- **Start over:** `rm -rf demo-db demo-db-checkpoint`.

### Using it in your own program

Add the same `replace` line to your module (until the module is published),
then:

```go
db, err := lsmkv.Open("/var/lib/myapp/db", nil) // nil means DefaultOptions()
if err != nil {
	log.Fatal(err)
}
defer db.Close()
err = db.Put([]byte("k"), []byte("v"), nil)
v, err := db.Get([]byte("k"), nil) // errors.Is(err, lsmkv.ErrNotFound) if absent
```

Only one `DB` can have a directory open at a time; a second `Open` fails with
`ErrLocked`.

---

## 3. Vocabulary

**In short:** a few words explain the whole engine.

| Word | Meaning |
|---|---|
| **Sequence number** | A counter that goes up by one for every write and never goes back, even after a restart. It puts all writes in time order. |
| **Entry** | One stored write: a key, its sequence number, its kind, and maybe a value. |
| **Kind** | What an entry means: `Set` (a value), `Delete` (a tombstone), `Merge` (a change to combine with the old value) or `SetTTL` (a value that expires). |
| **Tombstone** | A delete marker. The old value is not erased right away; the tombstone hides it until compaction removes both. |
| **Level** | A group of SSTables. L0 holds the newest; L1 to L6 hold older data, each level about 10 times bigger than the one above. |
| **Version** | A list of which SSTables are in which level. Every flush and compaction makes a new version. |
| **Snapshot** | A saved sequence number. Reading through it shows the database as it was at that moment. |
| **Read state** | What a read holds on to: the memtables and version it started with, so its view cannot change underneath it. |

Every entry is stored under an **internal key**: the user key plus 8 bytes that
hold the sequence number and the kind.

```text
  ┌──────────────┬───────────────────────────┬──────────┐
  │ user key     │ sequence number (7 bytes) │ kind (1) │
  │ "apple"      │ 9                         │ Set      │
  └──────────────┴───────────────────────────┴──────────┘
                 ◄───────── the 8-byte trailer ─────────►
```

Entries are sorted by user key, and each key's versions newest first:

```text
  apple    seq 9   Set  "red"      ◄── a normal read sees this
  apple    seq 4   Set  "green"    ◄── a snapshot taken at seq 4 to 8 sees this
  apple    seq 2   Delete
  banana   seq 7   Set  "yellow"
```

A read at sequence *S* only looks at entries numbered *S* or lower, and the
first one it finds for a key decides the answer.

Two more kinds exist internally: `Seek` is only used as a search marker and is
never stored; `RangeDelete` is reserved and never written.

---

## 4. What is on disk

**In short:** one directory. `CURRENT` points to the MANIFEST, the MANIFEST
lists the SSTables, and the WAL holds recent writes that are not in an SSTable
yet.

```text
  demo-db/
  ├── LOCK              held while the database is open
  ├── CURRENT           one line: the name of the live MANIFEST
  ├── MANIFEST-000092   which SSTables are in which level
  ├── 000091.log        WAL: writes not yet saved in an SSTable
  ├── 000087.sst  ─┐
  ├── 000088.sst   ├─   SSTables: sorted, never modified
  └── 000090.sst  ─┘
```

How the files point to each other:

```text
  CURRENT ──► MANIFEST-000092 ──┬──► L0: 000090.sst
                                ├──► L2: 000087.sst, 000088.sst
                                └──► oldest WAL still needed: 000091.log
```

Rules:

- WALs, SSTables and MANIFESTs share one counter for their numbers, so a file
  name is never reused.
- An SSTable is never modified after it is written; WALs and MANIFESTs only
  grow at the end.
- lsmkv ignores, and never deletes, any file it did not create.

In the code: `internal/base/filenames.go`.

---

## 5. Opening a database: recovery

**In short:** `Open` rebuilds everything from the files: `CURRENT` leads to the
MANIFEST, the MANIFEST gives the SSTables, and the WALs give back the writes
that were still in memory.

```text
  Open(dir, opts)
     │
     ▼
  check the options ──► create the directory ──► take the LOCK file
     │                   (if CreateIfMissing)     (fails if already open)
     ▼
  is there a CURRENT file? ── no ──► a brand-new, empty database
     │ yes
     ▼
  read the MANIFEST ──► rebuild "which SSTables are in which level"
     │
     ▼
  sanity check: does every listed SSTable exist?
     │           is the WAL we still need there?
     │           (if not: ErrCorruption; nothing is guessed)
     ▼
  replay the WALs, oldest first, into a memtable
     │   newest WAL damaged?  stop there and keep everything before it
     │                        (a crash can leave the last write half-done)
     │   older WAL damaged?   ErrCorruption, unless BestEffortRecovery
     ▼
  save the recovered writes as SSTables in L0
     │
     ▼
  write a NEW MANIFEST that names the next WAL, and only then create that WAL
     │
     ▼
  delete leftovers ──► start the flush and compaction workers ──► ready
```

- **Whole batches only.** Each WAL record is checked completely before any of
  it is applied, so recovery never applies half a batch.
- **Why the WAL is created last.** Its number is saved in the MANIFEST first.
  Otherwise a crash could leave a new, empty WAL next to an old half-written
  one, and the next `Open` would mistake the old one's damaged end for
  corruption.

In the code: `recover.go` (`Open`, `recoverFromDisk`, `replayWAL`,
`handleCorruptWAL`), `internal/manifest/version_set.go` (`Load`).

---

## 6. The write path

**In short:** writers wait in a queue. The first writer in line, the
**leader**, takes the whole queue's batches, writes them to the WAL in one go,
puts them in the memtable and then makes them visible to readers, all at once.

`Put`, `Delete`, `Merge` and `PutWithTTL` each wrap one change in a batch and
call `Write`, so every write takes this path.

```text
  writer A ─┐
  writer B ─┼──► write queue:  [ A │ B │ C ]
  writer C ─┘                    ▲
                                 A is first in line: A is the LEADER
                                 and commits B's and C's batches too
                                        │
                                        ▼
            ┌───────────────────────────────────────────────┐
            │ 1. number the writes: 101, 102, 103           │
            │ 2. append ONE record to the WAL               │
            │ 3. fsync if the group asked for Sync          │
            │    (one fsync covers every writer)            │
            │ 4. insert the writes into the memtable        │
            │ 5. visibleSeq = 103  ◄── readers now see all  │
            │                          three writes at once │
            └───────────────────────────────────────────────┘
                                        │
                                        ▼
            A, B and C return; D, next in line, leads the next group
```

### Before writing: is there room?

```text
  the leader checks, in order:

  a background error happened?         ──► fail the write (section 12)
  L0 has 8 or more SSTables?           ──► sleep 1 ms, once (slow down)
  the memtable still has room?         ──► write
  a full memtable is still waiting     ──► wait for the flush       stall
    to be saved?
  L0 has 12 or more SSTables?          ──► wait for compaction      stall
  otherwise                            ──► switch to a new memtable
```

The two L0 checks only apply while automatic compactions are on. Time spent
waiting is counted in `Metrics().WriteStall`.

### Switching to a new memtable

```text
          before                               after
  ┌─────────────────────────┐          ┌─────────────────────────┐
  │ active memtable (full)  │  ──►     │ active memtable         │ new, empty
  │ + WAL 000041.log        │          │ + WAL 000042.log        │
  └─────────────────────────┘          └─────────────────────────┘
                                       ┌─────────────────────────┐
                                       │ immutable memtable      │ waits for
                                       │ + WAL 000041.log        │ the flusher
                                       └─────────────────────────┘
```

The old WAL is fsynced before the switch. That way only the newest WAL can
ever be half-written after a crash.

### Why a batch is all-or-nothing

```text
  memtable:    …  100  101  102  103   ◄── 101–103 still being inserted
  visibleSeq = 100
               └─ readers ignore everything above 100, until the leader
                  sets visibleSeq to 103 in one step
```

### Checks on every write

- A key can be up to 64 KiB (`ErrKeyTooLarge`), a batch up to 64 MiB
  (`ErrBatchTooLarge`).
- `Merge` needs `Options.Merger` (`ErrNoMerger`).
- A batch copies its keys and values, so you can reuse your slices right away.

In the code: `db.go` (`Put`, `Write`, …), `write.go` (`commit`,
`buildGroupLocked`, `commitGroupLocked`, `makeRoomForWriteLocked`,
`rotateMemtableLocked`), `batch.go`, `internal/memtable`.

---

## 7. The read path

**In short:** `Get` looks in the newest place first and stops at the first
answer. An iterator merges every place into one sorted stream. A snapshot
freezes a moment in time.

### Get

```text
  Get("apple")       newest ──────────────────────────────► oldest

  ┌──────────┐   ┌───────────┐   ┌──────────────┐   ┌───────────────┐
  │ active   │ ► │ immutable │ ► │ L0 SSTables, │ ► │ L1 … L6:      │
  │ memtable │   │ memtables │   │ newest first │   │ one SSTable   │
  └──────────┘   └───────────┘   └──────────────┘   │ per level     │
                                                    └───────────────┘
  stop at the first "apple" entry the read is allowed to see
```

Inside one SSTable:

```text
  bloom filter: is "apple" maybe here?  ── no ──► skip this SSTable
     │ maybe                                      (no disk read at all)
     ▼
  index (kept in memory) ──► the one data block that could hold "apple"
     │
     ▼
  block cache has it? ── yes ──► use it
     │ no
     ▼
  read the block from disk, check its checksum, decompress, cache it
```

What the first visible entry means:

| Entry | Result |
|---|---|
| `Set` | The value |
| `SetTTL` | The value, or not found once it has expired |
| `Delete` | Not found (`ErrNotFound`) |
| `Merge` | Keep collecting older entries, then combine them all with `Options.Merger` |

The value you get back is a copy; you own it.

### Iterators

```text
  Iterator            what you use: First, Next, SeekGE, Prev, Last …
     │                hides old versions, deleted and expired keys,
     │                and combines merge operands
     ▼
  merging iterator    always hands out the smallest key among its sources
     ├── active memtable
     ├── immutable memtables
     ├── one iterator per L0 SSTable
     └── one level iterator per level   opens that level's SSTables
                                         one at a time, as it moves
```

What is stored, and what the iterator returns:

```text
  stored (all sources merged)          what the iterator returns
  ───────────────────────────          ─────────────────────────
  apple   seq 9  Set "red"       ──►   apple  = red
  apple   seq 4  Set "green"           (older version: skipped)
  banana  seq 8  Delete                (deleted: skipped)
  banana  seq 3  Set "yellow"          (hidden by the delete)
  cherry  seq 5  Set "dark"      ──►   cherry = dark
```

- `LowerBound` is included and `UpperBound` is excluded. SSTables outside the
  bounds are never opened.
- `Key()` and `Value()` are only valid until the next move; copy what you keep.
- Close every iterator: it keeps its SSTables from being deleted until then.

### Snapshots

```text
  seq 10   Put a = 1
           NewSnapshot()  ──► remembers seq 10
  seq 11   Put a = 2

  db.Get(a)                        ──► "2"
  db.Get(a) through the snapshot   ──► "1"   (sees only seq 10 and older)
```

A snapshot keeps compaction from throwing away versions it can still see, so
`Release` it when you are done.

In the code: `get.go`, `iterator.go`, `snapshot.go`, `read_state.go`,
`internal/iterator`, `table_cache.go`.

---

## 8. Flushing a memtable

**In short:** the flush worker saves the oldest full memtable as a new
SSTable, records it in the MANIFEST, and then the memtable's WAL can be
deleted.

```text
  oldest immutable memtable  (its WAL is 000049.log)
     │
     ▼
  1. write it out as 000051.sst: sorted, with an index and a bloom filter
     │
     ▼
  2. fsync the file, then the directory
     │
     ▼
  3. MANIFEST: "000051.sst is in L0;
                WALs older than 000050.log are not needed"   ◄── saved
     │
     ▼
  4. drop the memtable from memory, delete 000049.log,
     and wake the compaction worker
```

While writing, only the newest version of each key is kept, plus any older
ones a snapshot still needs. Tombstones are always kept, because older values
may still sit in SSTables below.

### Which level does the new SSTable go to?

```text
  overlaps an SSTable in L0? ── yes ──► L0
     │ no
     ▼
  push it down to L1, then L2, as long as:
     • nothing in that level overlaps it
     • it would not overlap too much of the level below
     • no running compaction is writing into that key range
```

Skipping empty levels saves rewriting the SSTable later. Size-tiered
compaction always flushes to L0.

**If something fails:** a half-written SSTable is removed. If the MANIFEST
write fails, the SSTable is kept, because the record may have reached the disk
anyway; the next `Open` sorts it out. Either way the error becomes a
background error (section 12).

In the code: `flush.go` (`flushLoop`, `flushPending`, `flushMemtable`,
`writeMemtable`, `pickFlushLevel`).

---

## 9. Compaction

**In short:** compaction merges some SSTables into the level below. Along the
way it throws away overwritten values, deleted keys and expired entries, so
reads stay fast and disk use stays small.

### When it runs

The compaction worker wakes after every flush, after `Open`, and when writers
are stalled on L0. It keeps going until nothing is due, one compaction at a
time.

### What it picks (leveled, the default)

```text
  1. score each level (1.0 or more means "too full")
        L0   number of files ÷ 4
        L1   size ÷ 10 MiB
        L2   size ÷ 100 MiB           each level allows 10× more
        …
  2. take the level with the highest score
  3. choose the input SSTables
        L0:  the oldest file, plus every L0 file that overlaps it
        L1+: the next SSTable after where this level's last compaction stopped
  4. add the SSTables in the next level down that overlap them
```

### What a merge does

```text
  before
    L1   [ a ─────────────── f ]
    L2   [ a ─── c ] [ d ───── h ] [ i ───── m ]
         └───────────┬───────────┘
            these two overlap the L1 SSTable

  after: the three are merged into new L2 SSTables
    L1   (nothing left in a…h)
    L2   [ a ──── d ] [ e ──── h ] [ i ───── m ]
         └──── new SSTables ─────┘ untouched
```

If only one SSTable is picked and nothing below overlaps it, it is simply
**moved** down a level with a MANIFEST record, without rewriting it:

```text
  L1   [ p ─ r ]                         L1   (moved away)
  L2   [ a ─ c ]          [ x ─ z ]  ──► L2   [ a ─ c ] [ p ─ r ] [ x ─ z ]
```

### What it keeps, key by key

This is the heart of compaction (`compact_keys.go`). For each key, it looks at
the versions from newest to oldest:

```text
  newer than the oldest open snapshot? ──► keep it (a snapshot may need it)

  the first version every reader can see decides; older ones are dropped:
     Set                     ──► keep it
     SetTTL, expired         ──► turn it into a tombstone
     compaction filter says  ──► turn it into a tombstone
       "remove" (only when no snapshots are open)
     Delete                  ──► drop it if nothing below could hold the key;
                                 otherwise keep it, since it still hides
                                 older values underneath
     Merge                   ──► fold the operands into one value when that
                                 is safe (never on top of a TTL value)
```

### Writing the result

- A new output SSTable starts about every 2 MiB (`TargetFileSize`), and only
  between two different keys, so one key's versions never span two files.
- Outputs are also cut early if they would overlap too much of the level
  below, so later merges stay small.
- Then: fsync, a MANIFEST record ("remove the inputs, add the outputs"), and
  the old files are deleted.

### Size-tiered strategy

Set `CompactionStrategy: SizeTieredCompaction`. Each L0 SSTable and each deeper
level counts as one **run**, ordered from newest to oldest:

```text
   newest ────────────────────────────────────────────────────► oldest
   [ L0 #4 ] [ L0 #3 ] [ L0 #2 ] [ L0 #1 ] [  L1  ] [ L2 ........ ]
     5 MB      5 MB      6 MB      5 MB     40 MB       90 MB

   checked in order; the first rule that applies wins:
   1. the newer runs add up to 2× the oldest?    ──► merge everything
   2. 4 or more neighbours of similar size       ──► merge those
      (the biggest at most 1.5× the smallest)?
   3. L0 has 8 or more files?                    ──► merge all of L0

   here rule 1 doesn't apply (61 MB < 2 × 90 MB), but rule 2 does:
   the four L0 files (5, 5, 6, 5 MB) are merged into one
```

Only neighbouring runs are merged, so newer data always stays above older
data. That keeps the tombstone rules correct.

### Manual: CompactRange

```text
  CompactRange(start, end)
     flush the memtable
     L0 ──► L1 ──► … ──► the deepest level that has data in [start, end]
     then rewrite that deepest level in place
  result: deleted and expired keys in the range are gone from disk
```

In the code: `compact.go` (`compactLoop`, `pickCompaction`, `runCompaction`,
`writeCompactionOutputs`), `compact_keys.go` (`processKey`, `foldMerge`),
`compact_range.go`, `internal/compaction` (`leveled.go`, `tiered.go`).

---

## 10. The MANIFEST and versions

**In short:** the MANIFEST is a log of changes ("add this SSTable to L0",
"remove that one from L1"). Replaying it from the start gives the current
**version**: the list of SSTables in each level.

```text
  MANIFEST-000092  (only ever appended to)
  ┌──────────────────────────────────────────────┐
  │ #1  snapshot:  L2 = {87, 88}                 │
  │ #2  add 90 to L0;  oldest WAL needed = 91    │
  │ #3  remove 90 from L0;  add 93 to L1         │
  └──────────────────────────────────────────────┘
                      │ replay #1 → #2 → #3
                      ▼
  current version:   L1 = {93}    L2 = {87, 88}
```

Every change goes through `LogAndApply`:

```text
  a flush or compaction has a change
     │
     ▼
  build the new version in memory, and check it
  (levels sorted, no overlaps, no file listed twice)
     │
     ▼
  append the change to the MANIFEST and fsync   ◄── from here it is permanent
     │
     ▼
  switch to the new version
```

### Starting a fresh MANIFEST, safely

lsmkv writes a new MANIFEST on the first change after every `Open`, after an
error, and once the old one reaches 64 MiB:

```text
  1. write MANIFEST-000094 (the whole current version), fsync, sync the dir
  2. write CURRENT.tmp containing "MANIFEST-000094", fsync
  3. rename CURRENT.tmp over CURRENT       ◄── the switch is one rename
  4. sync the dir
  a crash at any point leaves CURRENT naming a complete MANIFEST
```

### Why old files stay until nobody needs them

```text
  version 7  ◄── an iterator is still reading it    SSTables 10, 11, 12
  version 8  ◄── current                             SSTables 10, 13

  11 and 12 are no longer current, but stay on disk until the iterator
  closes and version 7 goes away
```

In the code: `internal/manifest` (`version_set.go`: `LogAndApply`, `Load`;
`version.go`; `version_edit.go`).

---

## 11. Deleting obsolete files

**In short:** after every flush, compaction and `Open`, lsmkv deletes files
that nothing refers to any more.

```text
  list the directory
     │
     ▼
  live = SSTables in any version still in use
       + SSTables being written right now
     │
     ▼
  delete:  SSTables that are not live
           WALs older than the oldest WAL still needed
           MANIFESTs other than the current one
```

After a background error, nothing is deleted until the database is reopened.
A failed fsync may still have saved a change, so a file that looks unused
might in fact be needed.

In the code: `obsolete_files.go` (`deleteObsoleteFiles`).

---

## 12. Errors

**In short:** most errors are simply returned to the caller. A failure in
the background, such as a disk error during a flush, stops all writes until
you close and reopen the database; reads keep working.

```text
  ┌───────────┐   a WAL, flush, compaction or MANIFEST   ┌──────────────────┐
  │  healthy  │ ────────────── write fails ────────────► │ background error │
  └───────────┘                                          │  • writes fail   │
        ▲                                                │  • reads work    │
        │                  Close, then Open again        │  • nothing is    │
        └────────────────────────────────────────────────┤    deleted       │
                                                         └──────────────────┘
```

| Error | Meaning |
|---|---|
| `ErrNotFound` | The key does not exist |
| `ErrCorruption` | Damaged data; the error names the file and offset |
| `ErrClosed` | The database is closed |
| `ErrLocked` | Another `DB` has this directory open |
| `ErrExists`, `ErrDoesNotExist` | From `ErrorIfExists` or `CreateIfMissing` |
| `ErrKeyTooLarge`, `ErrBatchTooLarge` | A key over 64 KiB or a batch over 64 MiB |
| `ErrNoMerger` | `Merge` used without `Options.Merger` |
| `ErrInvalidOptions` | The options don't make sense together |

Every WAL record, MANIFEST record and SSTable block has a checksum, so damaged
bytes come back as `ErrCorruption`, never as wrong data.

In the code: `errors.go`, `db.go` (`setBackgroundErrLocked`).

---

## 13. Closing

**In short:** `Close` stops everything cleanly. It does not flush the memtable:
those writes are already in the WAL and come back on the next `Open`.

```text
  Close()
     │
     ├─1─► refuse new calls (they get ErrClosed)
     ├─2─► let the writes already queued finish
     ├─3─► stop the workers
     │       a running compaction is abandoned; its half-written files
     │       are removed. A flush in progress finishes.
     ├─4─► fsync the WAL
     ├─5─► with ParanoidChecks: check the tree, and fail if an iterator
     │       or snapshot was never closed
     └─6─► close the files and release the LOCK
```

In the code: `db.go` (`Close`), `check.go`.

---

## 14. Checkpoints

**In short:** `Checkpoint(dir)` makes a separate copy of the database that you
can open on its own, for example as a backup.

```text
  Checkpoint("backup")
     │
     ▼
  flush the memtable, then hold on to the current version
     │
     ▼
  demo-db/000087.sst ═══ hard link ═══► backup/000087.sst
  demo-db/000088.sst ═══ hard link ═══► backup/000088.sst
     │               (copied instead, if a link isn't possible)
     ▼
  write backup/MANIFEST-… and backup/CURRENT
     │
     ▼
  lsmkv.Open("backup", nil) opens an independent database
```

Hard links are safe because SSTables never change. The target directory must
be empty or not exist yet.

In the code: `checkpoint.go`.

---

## 15. Caches

**In short:** two caches avoid repeated work: one keeps SSTables open, the
other keeps recently read data blocks in memory.

```text
  Get / Iterator
     │
     ▼
  table cache      keeps up to 1000 SSTables open; for each one it holds
     │             the open file, the index and the bloom filter
     ▼
  block cache      8 MiB of recently used data blocks (least recently
     │ miss        used are evicted first)
     ▼
  disk             read the block, check its checksum, decompress it
```

- Compaction reads, and reads with `DontFillCache`, still use cached blocks but
  don't add new ones, so a big scan doesn't push out the hot data.
- File numbers are never reused, so a deleted SSTable's cached blocks can never
  be confused with another file's; they just age out.

In the code: `table_cache.go`, `internal/cache`.

---

## 16. Concurrency

**In short:** everything on a `DB` is safe to use from many goroutines at once.
Readers never wait for writers.

```text
  goroutines                          shared state
  ──────────                          ────────────
  your goroutines (any number) ─┐
  commit leader (one at a time) ├───► db.mu guards the memtables, the
  flushLoop                     │     write queue, snapshots and errors
  compactLoop                  ─┘

  lock order (always taken in this order, never the reverse):
     compactionMu ──► manifest lock ──► db.mu

  golden rule: no disk I/O while holding db.mu
```

- **Readers** grab the current read state (a few pointers) and then read
  without any lock. Only the leader writes to the memtable, and the memtable
  lets readers look at it while it is being written.
- **Long work** (writing files) happens without locks, on data that can no
  longer change; only the final switch-over takes `db.mu`.
- A `Batch` or an `Iterator` belongs to one goroutine at a time; a `Snapshot`
  may be shared.

In the code: `db.go` (the lock comments on the `DB` struct), `read_state.go`.

---

## 17. Durability: what survives what

**In short:** every write survives the program crashing. Writes with
`Sync: true` also survive the machine losing power.

```text
  Put ──write()──► operating system's memory ──fsync──► disk
                   survives kill -9                     survives a power cut
```

| Your write | Program crashes (`kill -9`) | Power cut or OS crash |
|---|---|---|
| `Put` with default options | Survives | May be lost, unless a later synced write or a flush covered it |
| `Put` with `lsmkv.Sync` | Survives | Survives |
| Anything already flushed to an SSTable | Survives | Survives |

- `db.Write(nil, lsmkv.Sync)` makes every earlier write durable.
- Whatever is lost is always the most recent writes, never a gap in the
  middle, and a batch is kept or lost as a whole.

Where lsmkv calls fsync:

| Moment | What is synced |
|---|---|
| A `Sync` write | The WAL (and the directory, the first time for a new WAL) |
| Switching memtables | The old WAL |
| Flush and compaction | Each new SSTable, then the directory |
| Every MANIFEST change | The MANIFEST |
| A new MANIFEST | The MANIFEST, the directory, `CURRENT.tmp`, and the directory again |
| `Close` | The WAL |

On macOS, fsync means `F_FULLFSYNC`, which also flushes the drive's own cache.

In the code: `write.go` (`syncWAL`, `rotateWAL`), `flush.go`,
`internal/manifest/version_set.go`, `vfs/os_unix.go`.

---

## 18. On-disk formats

**In short:** four kinds of file. All numbers are little-endian.

### WAL and MANIFEST records

Both files are a list of records:

```text
  ┌───────────────┬───────────────┬────────────────────────┐
  │ checksum (4)  │ length (4)    │ payload (length bytes) │
  └───────────────┴───────────────┴────────────────────────┘
  the checksum (CRC-32C) covers the length and the payload
```

In a WAL, each record is one group of batches; in a MANIFEST, one change.

### A batch (the WAL payload)

```text
  ┌─────────────────────┬────────────┬──────┬──────┬───
  │ first seq (8 bytes) │ count (4)  │ op 1 │ op 2 │ …
  └─────────────────────┴────────────┴──────┴──────┴───

  op = kind (1) │ key length │ key │ value length │ value
                  (lengths are varints; no value for Delete)

  SetTTL value = expiry time (8 bytes) │ the user's value
```

### An SSTable

```text
  ┌──────┬──────┬─────┬────────┬────────────┬───────┬────────────┐
  │ data │ data │  …  │ bloom  │ properties │ index │   footer   │
  │ block│ block│     │ filter │            │       │ (64 bytes) │
  └──────┴──────┴─────┴────────┴────────────┴───────┴────────────┘
     ▲                                          │          │
     └──── "block for keys up to K" ────────────┘          │
                                                           │
     the footer says where the index, filter and properties are
```

Every block ends with a 5-byte trailer: the compression type (1 byte) and a
checksum (4 bytes).

Inside a data block, keys share prefixes with the key before them, which saves
space:

```text
  key             stored as
  ──────────      ───────────────────────────────
  apple           shared 0  +  "apple"
  applesauce      shared 5  +  "sauce"
  apricot         shared 2  +  "ricot"

  every 16th key is stored in full (a "restart point"), so a search can
  binary-search the restart points and then scan a few keys
```

(Stored keys also carry the 8-byte trailer from section 3; it is left out here
for clarity.)

| Part | Contents |
|---|---|
| Data block | Sorted keys and values, about 4 KiB, optionally compressed (Snappy or zstd; kept only if it saves at least 1/8) |
| Bloom filter | One per SSTable, 10 bits per key; rules out about 99% of keys that aren't there |
| Properties | Counts, sizes, the key order used, the compression used |
| Index | One entry per data block: a key and where that block is |
| Footer | Where the index, filter and properties are; format version; checksum; the magic text `LSMKV001` |

### MANIFEST changes

| Tag | Field |
|---|---|
| 1 | Name of the key order (comparer) |
| 2 | Oldest WAL still needed |
| 3 | Next file number |
| 4 | Last sequence number |
| 5 | Where a level's last compaction stopped |
| 6 | SSTable removed from a level |
| 7 | SSTable added to a level (number, size, key range) |

### CURRENT

One line: the MANIFEST's name, for example `MANIFEST-000092`.

In the code: `internal/record`, `batch.go`, `internal/sstable`,
`internal/bloom`, `internal/manifest/version_edit.go`.

---

## 19. Options

**In short:** start from `lsmkv.DefaultOptions()` and change only what you
need. A zero number means zero, not "use the default".

**The ones you are most likely to change:**

| Option | Default | What it does |
|---|---|---|
| `MemtableSize` | 4 MiB | How big the memtable gets before it is flushed |
| `Compression` | none | `SnappyCompression` or `ZstdCompression` for data blocks |
| `BlockCacheSize` | 8 MiB | Memory for cached data blocks; 0 turns it off |
| `CompactionStrategy` | leveled | `SizeTieredCompaction` writes less but uses more disk |
| `Merger` | none | Needed to use `Merge` |
| `Logger` | none | A `*slog.Logger`; at debug level it logs every compaction |

**Tuning:**

| Option | Default | What it does |
|---|---|---|
| `BloomBitsPerKey` | 10 | Bloom filter size; 0 turns filters off |
| `BlockSize` | 4 KiB | Data block size before compression |
| `TargetFileSize` | 2 MiB | Size of the SSTables compaction writes |
| `L1MaxBytes`, `LevelMultiplier` | 10 MiB, 10 | L1's size limit, and how much bigger each level below is |
| `L0CompactionTrigger` | 4 | L0 files that start a compaction |
| `L0SlowdownWritesTrigger` | 8 | L0 files at which writes slow down by 1 ms each |
| `L0StopWritesTrigger` | 12 | L0 files at which writes wait for compaction |
| `MaxImmutableMemtables` | 1 | Full memtables allowed to wait for a flush |
| `MaxOpenFiles` | 1000 | SSTables kept open by the table cache |
| `SizeTiered` | 4, 1.5, 200% | Merge width, size ratio and space limit for size-tiered |

**Behaviour:**

| Option | Default | What it does |
|---|---|---|
| `CreateIfMissing`, `ErrorIfExists` | true, false | What `Open` does when the database does or doesn't exist |
| `CompactionFilter` | none | Called during compaction; return true to delete a key. Must always give the same answer |
| `Clock` | `time.Now` | The clock used for TTL expiry |
| `DisableAutomaticCompactions` | false | Only flushes and `CompactRange` run |
| `BestEffortRecovery` | false | Recover what it can from a damaged older WAL instead of failing |
| `ParanoidChecks` | false | Check the tree regularly; make `Close` fail if iterators or snapshots leaked |
| `FS`, `Comparer` | OS files, bytewise | Where files live (`vfs.NewMem()` is in-memory), and the key order |
| `NumLevels`, `MaxManifestFileSize` | 7, 64 MiB | Rarely changed |

Per call: `ReadOptions{Snapshot, LowerBound, UpperBound, DontFillCache}` and
`WriteOptions{Sync}`. Passing `nil` means the defaults; `lsmkv.Sync` and
`lsmkv.NoSync` are ready to use.

In the code: `options.go`.

---

## 20. Metrics

**In short:** `db.Metrics()` returns counters describing what the engine has
done; `fmt.Print(db.Metrics())` prints them all.

| Group | What it tells you |
|---|---|
| Levels | Files, bytes and compaction score per level (1 or more means a compaction is due) |
| Writes | Bytes written, WAL files and syncs, write stalls and how long they took |
| Flush and compaction | How many, bytes read and written, moves without rewriting, write amplification |
| Caches | Block cache and table cache hits and misses |
| Reads | Bloom filter checks and how many it ruled out; blocks read from disk |
| Resources | Open snapshots, open iterators, files deleted, `DiskSize()` |

**Write amplification** is bytes written to disk by flushes and compactions,
divided by bytes you wrote. Lower means less disk wear.

In the code: `metrics.go`.

---

## 21. Code map

**In short:** the root package `lsmkv` is the database. It uses small internal
packages, each owning one data structure or file format.

```text
                        lsmkv   (the DB: root package)
                              │ uses
   ┌──────────┬───────────┬───┴──────┬────────────┬────────────┐
   ▼          ▼           ▼          ▼            ▼            ▼
 memtable   record     sstable    manifest    compaction    iterator
 in-memory  WAL and    SSTable    versions    what to       merge many
 sorted     MANIFEST   files      and the     compact       sources into
 writes     records       │       MANIFEST                  one stream
                          ▼
                    bloom, cache

   at the bottom, used by almost everything above:
     base   keys, kinds, file names, checksums
     vfs    files and directories: real, in-memory, or fault-injecting
```

Root package, one concern per file:

| File | What it does |
|---|---|
| `doc.go` | The package comment |
| `db.go` | The `DB` type; `Put`, `Delete`, `Merge`, `PutWithTTL`, `Write`, `Flush`, `Close` |
| `write.go` | Write queue, group commit, making room, switching memtables and WALs |
| `batch.go` | `Batch`, and how batches and TTL values are encoded |
| `get.go` | `Get` |
| `iterator.go` | `Iterator` |
| `snapshot.go` | Snapshots |
| `read_state.go` | What a read holds on to |
| `flush.go` | The flush worker |
| `compact.go` | The compaction worker, running one compaction |
| `compact_keys.go` | What compaction keeps, key by key |
| `compact_range.go` | `CompactRange` |
| `obsolete_files.go` | Deleting files nothing needs |
| `recover.go` | `Open` and WAL replay |
| `checkpoint.go` | `Checkpoint` |
| `table_cache.go` | Keeping SSTables open |
| `options.go`, `errors.go`, `metrics.go`, `check.go` | Options, errors, metrics, `ParanoidChecks` |

Internal packages:

| Package | What it does |
|---|---|
| `internal/base` | Internal keys, kinds, key order, file names, checksums |
| `internal/memtable` | The memtable (a skip list) |
| `internal/record` | WAL and MANIFEST records |
| `internal/sstable` | Writing and reading SSTables |
| `internal/bloom` | Bloom filters |
| `internal/cache` | The block cache |
| `internal/manifest` | Versions, MANIFEST changes, `LogAndApply` |
| `internal/compaction` | Choosing compactions (leveled and size-tiered) |
| `internal/iterator` | Merging and level iterators |
| `internal/invariants` | Extra self-checks, on under `-race` or `-tags invariants` |
| `vfs` | The filesystem: real files, an in-memory version, and fault injection |

The naming and layout rules for all of this are in
[CONVENTIONS.md](CONVENTIONS.md).

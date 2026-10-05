# Code conventions

These rules keep every package in this repository reading the same way. New
code should follow them; `make` and `make staticcheck` must pass.

## Layout

- Every package has a `doc.go` holding its package comment and nothing else.
- One concern per file, named after it: `write.go` is the write path,
  `compact_keys.go` the per-key compaction rules, `obsolete_files.go` file
  deletion. Split a file when it starts holding a second concern.
- Declare a type before its methods, and keep helpers below the code that
  calls them.
- A helper lives with the format or concept it belongs to, not with its first
  caller: batch and TTL-value encoding is all in `batch.go`.
- Imports come in three groups: standard library, third-party, this module.

## Names

Names say what a value is, not its type. Spell words out; the abbreviations
below are the only ones in use.

**Receivers** are one or two letters taken from the type's name, the same on
every method: `db *DB`, `bi *blockIter`, `tc *tableCache`, `sl *skiplist`,
`vs *VersionSet`, `it *Iterator`.

**Recurring values** use the same name everywhere:

| Value | Name |
|---|---|
| `*manifest.FileMetadata` | `file`, `files` |
| `vfs.File` | its role: `walFile`, `tableFile`, `manifestFile`, `srcFile` |
| `*manifest.Version`, `*manifest.VersionEdit` | `version`, `edit` |
| `*compaction.Compaction` | `job` |
| `*readState` | `state` |
| `*memtable.Memtable` | `mem`; the DB fields are `activeMemtable` and `immutableMemtables` |
| `*sstable.Writer`, `*sstable.Reader` | `tableWriter`, `reader` |
| `*record.Reader`, one record | `records`, `payload` |
| `base.Compare`, `*base.Comparer` | `compare`, `comparer` |
| user key, internal key | `key` or `userKey`, `key` or `internalKey` when both appear |
| `*ReadOptions`, `*WriteOptions` | `readOpts`, `writeOpts` |
| `os.FileInfo` | `info` |
| a second error | what it came from: `closeErr`, `recordErr` |

**Short names** are allowed only here: `i` and `j` for indices, `n` for a
count returned by `Read`, `Write`, `copy` or `binary.Uvarint`, `ok`, `err`, `it`
for an iterator, `mu` for a mutex, `fs` for a filesystem, `a` and `b` for the
two sides of a comparison, and `w`, `r` and `p` where the `io` interfaces use
them.

**Abbreviations** in use: `seq` (sequence number), `num` (file number), `opts`,
`props`, `buf`, `dst`, `src`, `dir`, `crc`, `wal`, and `L0` to `L6`.

**Sizes and counts** carry their unit: `headerLen` for bytes of an encoded
piece, `maxGroupBytes` or `TargetFileSize` for byte sizes, `numLevels` or
`numL0Files` for counts.

**Booleans** read as a state or a question: `closing`, `hasMerge`, `dbExists`,
`isNewest`, `canFold`.

**Functions** are verbs (`flushMemtable`, `replayWAL`), accessors have no `Get`
prefix, and constructors are `New…`/`new…`. A `maybe…` function acts only if
needed. A `…Locked` suffix means the caller holds the mutex: `db.mu` for `DB`
methods, the receiver's own `mu` elsewhere.

Locals never reuse the name of an imported package or a builtin (`record`,
`cache`, `max`). A constructor's local for the value it builds uses the
receiver name: `vs := &VersionSet{…}`.

## Comments

Default to none. Keep only:

- functional comments (`//go:` directives);
- the package comment in `doc.go`, one or two sentences;
- a one-line doc comment on exported API where it states a contract the
  signature doesn't: errors, what nil or zero means, slice ownership,
  durability, concurrency;
- short why-comments for crash-safety ordering, lock order, data-loss
  invariants and on-disk layouts.

No comments that restate a name, narrate the code or record history.

## Errors

Messages start with the package that produced them (`lsmkv: …`,
`sstable …: …`, `vfs: …`) and wrap causes with `%w`. Damaged data is reported
through `base.CorruptionErrorf`, so callers can test it with
`errors.Is(err, ErrCorruption)`.

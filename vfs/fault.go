package vfs

import (
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
)

type Op uint32

const (
	OpCreate Op = 1 << iota
	OpOpen
	OpRemove
	OpRename
	OpLink
	OpMkdirAll
	OpList
	OpStat
	OpLock
	OpSyncDir
	OpRead
	OpReadAt
	OpWrite
	OpSync
	OpFileStat
)

const (
	OpsMutating = OpCreate | OpRemove | OpRename | OpLink | OpMkdirAll | OpSyncDir | OpWrite | OpSync
	OpsRead     = OpOpen | OpList | OpStat | OpRead | OpReadAt | OpFileStat
	// OpsAll omits OpLock, since failing it only keeps a database from opening.
	OpsAll = OpsMutating | OpsRead
)

var opNames = map[Op]string{
	OpCreate: "create", OpOpen: "open", OpRemove: "remove", OpRename: "rename",
	OpLink: "link", OpMkdirAll: "mkdir", OpList: "readdir", OpStat: "stat",
	OpLock: "lock", OpSyncDir: "syncdir", OpRead: "read", OpReadAt: "readat",
	OpWrite: "write", OpSync: "sync", OpFileStat: "fstat",
}

func (o Op) String() string {
	if name, ok := opNames[o]; ok {
		return name
	}
	return "op"
}

var ErrInjected = errors.New("vfs: injected fault")

// FaultPolicy selects the faults a FaultFS injects; the zero value injects none.
type FaultPolicy struct {
	// Ops selects the counted, failable operations; zero means OpsMutating.
	Ops      Op
	FailNth  int64
	FailFrom int64
	// ShortWrites makes injected Write failures first write a random prefix.
	ShortWrites bool
	// BitFlipEvery flips one random bit in the result of every Nth successful ReadAt.
	BitFlipEvery int64
	Seed         uint64
}

type FaultFS struct {
	inner FS

	mu     sync.Mutex
	policy FaultPolicy
	random *rand.Rand

	counted     atomic.Int64
	readAtCount atomic.Int64
	injected    atomic.Int64
	disabled    atomic.Bool
}

var _ FS = (*FaultFS)(nil)

func NewFault(inner FS, policy FaultPolicy) *FaultFS {
	ffs := &FaultFS{inner: inner}
	ffs.SetPolicy(policy)
	return ffs
}

// SetPolicy replaces the policy and resets the operation counters.
func (ffs *FaultFS) SetPolicy(policy FaultPolicy) {
	if policy.Ops == 0 {
		policy.Ops = OpsMutating
	}
	ffs.mu.Lock()
	ffs.policy = policy
	ffs.random = rand.New(rand.NewPCG(policy.Seed, policy.Seed^0x9e3779b97f4a7c15))
	ffs.mu.Unlock()
	ffs.counted.Store(0)
	ffs.readAtCount.Store(0)
}

func (ffs *FaultFS) Disable() { ffs.disabled.Store(true) }

func (ffs *FaultFS) Enable() { ffs.disabled.Store(false) }

func (ffs *FaultFS) Injected() int64 { return ffs.injected.Load() }

func (ffs *FaultFS) Counted() int64 { return ffs.counted.Load() }

func (ffs *FaultFS) Inner() FS { return ffs.inner }

func (ffs *FaultFS) maybeFail(op Op, name string) error {
	if ffs.disabled.Load() {
		return nil
	}
	ffs.mu.Lock()
	policy := ffs.policy
	ffs.mu.Unlock()
	if op&policy.Ops == 0 {
		return nil
	}
	count := ffs.counted.Add(1)
	if (policy.FailNth > 0 && count == policy.FailNth) || (policy.FailFrom > 0 && count >= policy.FailFrom) {
		ffs.injected.Add(1)
		return &os.PathError{Op: op.String(), Path: name, Err: ErrInjected}
	}
	return nil
}

func (ffs *FaultFS) randIntN(n int) int {
	ffs.mu.Lock()
	defer ffs.mu.Unlock()
	return ffs.random.IntN(n)
}

func (ffs *FaultFS) wrap(file File, name string) File {
	return &faultFile{File: file, faults: ffs, name: name}
}

func (ffs *FaultFS) Create(name string) (File, error) {
	if err := ffs.maybeFail(OpCreate, name); err != nil {
		return nil, err
	}
	file, err := ffs.inner.Create(name)
	if err != nil {
		return nil, err
	}
	return ffs.wrap(file, name), nil
}

func (ffs *FaultFS) Open(name string) (File, error) {
	if err := ffs.maybeFail(OpOpen, name); err != nil {
		return nil, err
	}
	file, err := ffs.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return ffs.wrap(file, name), nil
}

func (ffs *FaultFS) Remove(name string) error {
	if err := ffs.maybeFail(OpRemove, name); err != nil {
		return err
	}
	return ffs.inner.Remove(name)
}

func (ffs *FaultFS) Rename(oldname, newname string) error {
	if err := ffs.maybeFail(OpRename, oldname); err != nil {
		return err
	}
	return ffs.inner.Rename(oldname, newname)
}

func (ffs *FaultFS) Link(oldname, newname string) error {
	if err := ffs.maybeFail(OpLink, oldname); err != nil {
		return err
	}
	return ffs.inner.Link(oldname, newname)
}

func (ffs *FaultFS) MkdirAll(dir string, perm os.FileMode) error {
	if err := ffs.maybeFail(OpMkdirAll, dir); err != nil {
		return err
	}
	return ffs.inner.MkdirAll(dir, perm)
}

func (ffs *FaultFS) List(dir string) ([]string, error) {
	if err := ffs.maybeFail(OpList, dir); err != nil {
		return nil, err
	}
	return ffs.inner.List(dir)
}

func (ffs *FaultFS) Stat(name string) (os.FileInfo, error) {
	if err := ffs.maybeFail(OpStat, name); err != nil {
		return nil, err
	}
	return ffs.inner.Stat(name)
}

func (ffs *FaultFS) Lock(name string) (io.Closer, error) {
	if err := ffs.maybeFail(OpLock, name); err != nil {
		return nil, err
	}
	return ffs.inner.Lock(name)
}

func (ffs *FaultFS) SyncDir(dir string) error {
	if err := ffs.maybeFail(OpSyncDir, dir); err != nil {
		return err
	}
	return ffs.inner.SyncDir(dir)
}

type faultFile struct {
	File
	faults *FaultFS
	name   string
}

func (ff *faultFile) Read(p []byte) (int, error) {
	if err := ff.faults.maybeFail(OpRead, ff.name); err != nil {
		return 0, err
	}
	return ff.File.Read(p)
}

func (ff *faultFile) ReadAt(p []byte, off int64) (int, error) {
	if err := ff.faults.maybeFail(OpReadAt, ff.name); err != nil {
		return 0, err
	}
	n, err := ff.File.ReadAt(p, off)
	if n > 0 && !ff.faults.disabled.Load() {
		ff.faults.mu.Lock()
		flipEvery := ff.faults.policy.BitFlipEvery
		ff.faults.mu.Unlock()
		if flipEvery > 0 && ff.faults.readAtCount.Add(1)%flipEvery == 0 {
			ff.faults.injected.Add(1)
			index := ff.faults.randIntN(n)
			p[index] ^= 1 << ff.faults.randIntN(8)
		}
	}
	return n, err
}

func (ff *faultFile) Write(p []byte) (int, error) {
	if err := ff.faults.maybeFail(OpWrite, ff.name); err != nil {
		ff.faults.mu.Lock()
		shortWrites := ff.faults.policy.ShortWrites
		ff.faults.mu.Unlock()
		if shortWrites && len(p) > 1 {
			n, _ := ff.File.Write(p[:ff.faults.randIntN(len(p))])
			return n, err
		}
		return 0, err
	}
	return ff.File.Write(p)
}

func (ff *faultFile) Sync() error {
	if err := ff.faults.maybeFail(OpSync, ff.name); err != nil {
		return err
	}
	return ff.File.Sync()
}

func (ff *faultFile) Stat() (os.FileInfo, error) {
	if err := ff.faults.maybeFail(OpFileStat, ff.name); err != nil {
		return nil, err
	}
	return ff.File.Stat()
}

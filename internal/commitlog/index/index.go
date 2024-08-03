// package index provides an index for a commit log. This speeds up reads by
// maintaining an association between records and their offsets in a store.
package index

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
)

var EmptyFile = errors.New("index cannot be backed by empty file")

// Index encapulates the association between records and their offset on disk.
type Index struct {
	*os.File        // The file backing the index
	buf      []byte // The region in memory onto which [File] is mapped.
	size     uint64 // The size of the backing file.
}

// New creates a new index against [f] upper bounded by [maxBytes]. The file at
// [f] is truncated to [maxBytes].
func New(f *os.File, maxBytes uint64) (*Index, error) {
	if err := os.Truncate(f.Name(), int64(maxBytes)); err != nil {
		return nil, err
	}

	prot := unix.PROT_READ | unix.PROT_WRITE
	flags := unix.MAP_SHARED

	stat, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := stat.Size()
	if size == 0 {
		return nil, EmptyFile
	}

	// Map [f] as a shared region. This serves as the storage for the index.
	b, err := unix.Mmap(int(f.Fd()), 0, int(size), prot, flags)
	if err != nil {
		return nil, err
	}

	return &Index{
		File: f,
		buf:  b,
		size: uint64(size),
	}, nil
}

// Close persists the memory-mapped file to stable storage and truncates the
// memory region to the actual size of the index.
func (i *Index) Close() error {
	// Flush
	if err := i.File.Sync(); err != nil {
		return err
	}

	return i.File.Close()
}

// Read accepts an offset and computes the corresponding record's position in
// the store.
func (i *Index) Read(in int64) (uint32, uint64, error) {
	return 0, 0, nil
}

// Write stores the position of the record at [off] in the index.
func (i *Index) Write(off uint32, pos uint64) error {
	return nil
}

// Name returns the name of the memory-mapped file backing the index.
func (i *Index) Name() string {
	return i.File.Name()
}

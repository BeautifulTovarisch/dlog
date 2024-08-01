// package index provides an index for a commit log. This speeds up reads by
// maintaining an association between records and their offsets in a store.
package index

import (
	"os"
	// "syscall"
)

// Index encapulates the association between records and their offset on disk.
type Index struct {
	*os.File
	size uint64
}

// New creates a new index against [f] upper bounded by [maxBytes]
func New(f *os.File, maxBytes uint64) (*Index, error) {
	return &Index{}, nil
}

// Close persists the memory-mapped file to stable storage and truncates the
// memory region to the actual size of the file.
func (i *Index) Close() error {
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

// package index provides an index for a commit log. This speeds up reads by
// maintaining an association between records and their offsets in a store.
package index

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

var (
	enc = binary.BigEndian
)

// ErrEmptyFile is returned when attempting to back an index with an empty file
var ErrEmptyFile = errors.New("index cannot be backed by empty file")

const (
	posWidth    = 8                      // The width of the section containing the position of a record
	offsetWidth = 4                      // The width of the section containing the offset of a record
	recordWidth = posWidth + offsetWidth // The total width of an index entry
)

// Index encapulates the association between records and their offset on disk.
type Index struct {
	*os.File        // The file backing the index
	buf      []byte // The region in memory onto which [File] is mapped.
	size     uint64 // The size of the backing file.
}

// New creates a new index against [f] upper bounded by [maxBytes]. The file at
// [f] is truncated to [maxBytes].
func New(f *os.File, maxBytes uint64) (*Index, error) {
	if maxBytes == 0 {
		return nil, ErrEmptyFile
	}

	stat, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	if err := os.Truncate(f.Name(), int64(maxBytes)); err != nil {
		return nil, err
	}

	prot := unix.PROT_READ | unix.PROT_WRITE
	flags := unix.MAP_SHARED

	// Map [f] as a shared region. This serves as the storage for the index.
	b, err := unix.Mmap(int(f.Fd()), 0, int(maxBytes), prot, flags)
	if err != nil {
		return nil, err
	}

	return &Index{
		File: f,
		buf:  b,
		size: uint64(stat.Size()),
	}, nil
}

// Close persists the memory-mapped file to stable storage and truncates the
// memory region to the actual size of the index.
func (i *Index) Close() error {
	// Flush contents of the buffer and file before unmapping
	if err := unix.Msync(i.buf, unix.MS_SYNC); err != nil {
		return nil
	}

	if err := i.File.Sync(); err != nil {
		return err
	}

	// Truncate the file to the measured size of the index.
	if err := i.File.Truncate(int64(i.size)); err != nil {
		return err
	}

	// TODO: Determine if this call to unmap is needed/intended.
	if err := unix.Munmap(i.buf); err != nil {
		return err
	}

	return i.File.Close()
}

// Read accepts an offset and computes the corresponding record's position in
// the store. Providing [in=-1] will read the record from the end of the index.
//
// NOTE: Unlike indexing into Python lists, values other than -1 will return an
// error rather than correspond to the [n+i]th element.
func (i *Index) Read(in int64) (uint32, uint64, error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	var off uint32 = uint32(in)
	if in == -1 {
		// This computes the offset of the last record in the index.
		off = uint32((i.size / recordWidth) - 1)
	}

	// Compute the absolute position of the record.
	pos := uint64(off) * recordWidth
	if i.size < pos+recordWidth {
		return 0, 0, io.EOF
	}

	// 0      3        11
	// [offset|position]
	off = enc.Uint32(i.buf[pos : pos+offsetWidth])
	pos = enc.Uint64(i.buf[pos+offsetWidth : pos+recordWidth])

	return off, pos, nil
}

// Write stores the position of the record at [off] in the index. If the record
// would not fit in the index, [io.EOF] is returned.
func (i *Index) Write(off uint32, pos uint64) error {
	if uint64(len(i.buf)) < i.size+recordWidth {
		return io.EOF
	}

	enc.PutUint32(i.buf[i.size:i.size+offsetWidth], off)
	enc.PutUint64(i.buf[i.size+offsetWidth:i.size+recordWidth], pos)

	i.size += uint64(recordWidth)

	return nil
}

// Name returns the name of the memory-mapped file backing the index.
func (i *Index) Name() string {
	return i.File.Name()
}

// Size returns the current size of the index in bytes.
func (i *Index) Size() uint64 {
	return i.size
}

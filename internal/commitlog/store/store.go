// package store provides I/O operations which maintain a commitlog on disk.
package store

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// Store represents a data store on disk to which records are written.
type Store struct {
	*os.File
	buf  *bufio.Writer
	mu   sync.Mutex
	size uint64
}

var (
	enc = binary.BigEndian
)

const (
	// Number of bytes to store the record's length
	lenWidth = 8
)

// Create a new store from a [*File].
func New(file *os.File) (*Store, error) {
	f, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(f.Size())

	return &Store{
		File: file,
		size: size,
		buf:  bufio.NewWriter(file),
	}, nil
}

// Appends persists [p] to the given store [s] returning the length of the
// record and the position of the bytes in the store.
//
// [len(r1)][r1][len(r2)][r2]...[len(rn)][rn]
//
// Where each len(ri) block is [lenWidth] bytes in size.
func (s *Store) Append(p []byte) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// The current size of the store is the position of the new record
	pos := s.size

	// Write the length of the record to the buffer first. This metadata is always
	// [lenWidth] bytes in length.
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	n, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	length := uint64(n + lenWidth)
	s.size += length

	return length, pos, nil
}

// Read returns the record at [pos] in the store.
func (s *Store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Transfer bytes from the buffer to disk.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Read the length of the record from the first [lenWidth] bytes after the
	// offset.
	length := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(length, int64(pos)); err != nil {
		return nil, err
	}

	// Allocate a buffer the length of the record.
	b := make([]byte, enc.Uint64(length))

	// Finally, read the actual record contents, skipping past the bytes storing
	// the record's length
	//
	// [ ... ][ length ][ content ]
	//        ^pos      ^pos+lenWidth
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// ReadAt reads [len(p)] bytes beginning at offset [off] from the store.
func (s *Store) ReadAt(p []byte, off int64) (int, error) {
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// Close closes the file descriptor pointing to the store. Any bytes currently
// in the buffer are written out before closing.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}

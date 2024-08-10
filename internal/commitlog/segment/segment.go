// package segment ties together a store and index.
package segment

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/beautifultovarisch/dlog/internal/schema"

	"github.com/beautifultovarisch/dlog/internal/commitlog/index"
	"github.com/beautifultovarisch/dlog/internal/commitlog/proto"
	"github.com/beautifultovarisch/dlog/internal/commitlog/store"
)

const (
	flags    = os.O_RDWR | os.O_CREATE
	fileMode = 0644
)

// Config governs the parameters of the [Segment], [Store], and [Index]
// TODO: Determine whether this is the right place for this.
type Config struct {
	MaxBytes uint64
}

// Segment encapsulates operations on a [Store] and [Index], ensuring the
// entries in both correspond.
type Segment struct {
	store *store.Store
	index *index.Index
	Config
	baseOffset, nextOffset uint64
}

// New constructs a segment, initializing the encapsulated store and index and
// creating their respective backing files.
func New(dir string, baseOffset uint64, c Config) (*Segment, error) {
	var err error

	s := Segment{
		Config:     c,
		baseOffset: baseOffset,
	}

	path := filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store"))
	// Open a file under [dir] to back the store. The store files are named
	// numerically, beginning with [baseOffset] and suffixed with '.store'.
	storefile, err := os.OpenFile(path, flags|os.O_APPEND, fileMode)
	if err != nil {
		return nil, err
	}

	path = filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index"))
	indexfile, err := os.OpenFile(path, flags, fileMode)
	if err != nil {
		return nil, err
	}

	if s.store, err = store.New(storefile); err != nil {
		return nil, err
	}

	if s.index, err = index.New(indexfile, c.MaxBytes); err != nil {
		return nil, err
	}

	// If the index is empty, the next offset is simply the base. Otherwise, the
	// nextOffset is computed by advancing exactly one byte past the last record
	// in the index:
	//
	//  [ ... | ... ]
	//  ^      ^
	//  base   base+off+1
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return &s, nil
}

// Append adds [record] to its store and index, returning its offset.
func (s *Segment) Append(record *proto.Record) (uint64, error) {
	cur := s.nextOffset
	record.Offset = cur

	// Encode the record into binary and persist to the store.
	c, err := schema.GetCodec(schema.RECORD)
	if err != nil {
		return 0, err
	}

	// Avro requires this type.
	r := map[string]interface{}{
		"value":  record.Value,
		"offset": int32(record.Offset),
	}

	data, err := c.BinaryFromNative(nil, r)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(data)
	if err != nil {
		return 0, err
	}

	// TODO: I need a picture describing the relationship of these offsets.
	// Add a nice diagram to the README later.
	off := uint32(s.nextOffset - uint64(s.baseOffset))
	if err := s.index.Write(off, pos); err != nil {
		return 0, err
	}

	s.nextOffset++

	return cur, nil
}

// Read retrieves the record in its store located at offset [off].
func (s *Segment) Read(off uint64) (*proto.Record, error) {
	c, err := schema.GetCodec(schema.RECORD)
	if err != nil {
		return nil, err
	}

	_, pos, err := s.index.Read(int64(off))
	if err != nil {
		return nil, err
	}

	data, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	rec, _, err := c.NativeFromBinary(data)
	if err != nil {
		return nil, err
	}

	// I don't actually know if this assertion will ever fail.
	if m, ok := rec.(map[string]interface{}); ok {
		value, ok := m["value"]
		if !ok {
			return nil, fmt.Errorf("unable to retrieve 'value' from record")
		}

		offset, ok := m["offset"]
		if !ok {
			return nil, fmt.Errorf("unable to retrieve 'offset' from record")
		}

		// Let it panic. See if I care...
		return &proto.Record{
			Offset: uint64(offset.(int64)),
			Value:  value.([]byte),
		}, nil
	} else {
		return nil, fmt.Errorf("invalid type. %v is not a map", rec)
	}
}

func (s *Segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

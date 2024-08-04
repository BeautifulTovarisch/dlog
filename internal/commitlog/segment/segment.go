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
	*store.Store
	*index.Index
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

	if s.Store, err = store.New(storefile); err != nil {
		return nil, err
	}

	if s.Index, err = index.New(indexfile, c.MaxBytes); err != nil {
		return nil, err
	}

	// If the index is empty, the next offset is simply the base. Otherwise, the
	// nextOffset is computed by advancing exactly one byte past the last record
	// in the index:
	//
	//  [ ... | ... ]
	//  ^      ^
	//  base   base+off+1
	if off, _, err := s.Index.Read(-1); err != nil {
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

	_, pos, err := s.Store.Append(data)
	if err != nil {
		return 0, err
	}

	// TODO: I need a picture describing the relationship of these offsets.
	off := uint32(s.nextOffset - uint64(s.baseOffset))
	if err := s.Index.Write(off, pos); err != nil {
		return 0, err
	}

	s.nextOffset++

	return cur, nil
}

func (s *Segment) Close() error {
	if err := s.Index.Close(); err != nil {
		return err
	}

	if err := s.Store.Close(); err != nil {
		return err
	}

	return nil
}

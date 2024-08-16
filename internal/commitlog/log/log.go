// package log manages the creation of segments and provides client software a
// way to interface with a the commitlog
package log

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/beautifultovarisch/dlog/internal/commitlog/record"
	"github.com/beautifultovarisch/dlog/internal/commitlog/segment"
)

const (
	defaultMaxIndex = (1 << 10)
	defaultMaxStore = (1 << 10)
)

// ErrOutOfBounds occurs when no segment in the Log contains the given offset.
type ErrOutOfBounds struct {
	offset uint64
}

func (e ErrOutOfBounds) Error() string {
	return fmt.Sprintf("offset %d out of range", e.offset)
}

// Config is the configuration for the log.
type Config struct {
	Segment segment.Config // Segment configures the log segments.
}

// Log is a list of segments with a pointer to the active segment.
type Log struct {
	mu     sync.RWMutex // shared readers, exclusive writers. I always forget this.
	Dir    string       // Dir is the directory in which the store and index is kept.
	Config              // Config is the configuration of the log

	segments      []*segment.Segment
	activeSegment *segment.Segment
}

func setup(dir string, c Config) (*Log, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// Gather the offsets in order to reconstruct a log from disk files.
	var baseOffsets []uint64
	// Files have the following form: <offset>.<index|store>. Getting the base
	// offset is a matter of slicing off the suffix and converting to an int.
	for _, file := range files {
		name := file.Name()
		prefix := strings.TrimSuffix(filepath.Base(name), filepath.Ext(name))

		offset, err := strconv.ParseUint(prefix, 10, 0)
		if err != nil {
			return nil, err
		}

		baseOffsets = append(baseOffsets, offset)
	}

	// This is a brand new log. Create from the initial offset and return
	if len(baseOffsets) == 0 {
		seg, err := segment.New(dir, c.Segment.InitialOffset, c.Segment)
		if err != nil {
			return nil, err
		}

		return &Log{
			Dir:           dir,
			Config:        c,
			segments:      []*segment.Segment{seg},
			activeSegment: seg,
		}, nil
	}

	// Sort here so later when iterating through the offsets adjacent offsets
	// can be skipped since they will be the same for the index and store.
	// NOTE: I am extremely unsure about this, but the book seems to think it's
	// okay...
	slices.SortFunc(baseOffsets, func(a, b uint64) int {
		// This quantity could not be negative otherwise
		return int(a) - int(b)
	})

	var segments []*segment.Segment
	// We only need one offset per pair of index and store, so we may advance [i]
	// by two each iteration.
	for i := 0; i < len(baseOffsets); i += 2 {
		// Create a new segment
		s, err := segment.New(dir, baseOffsets[i], c.Segment)
		if err != nil {
			return nil, err
		}

		segments = append(segments, s)
	}

	// The active segment is always the last segment. This is because segments are
	// numbered monotonically and guaranteed to only be created in the event the
	// current active segment is full (see Append).
	active := segments[len(segments)-1]

	return &Log{
		Dir:           dir,
		Config:        c,
		segments:      segments,
		activeSegment: active,
	}, nil
}

// New constructs a new [Log] whose store and index are located under [dir],
// and whose segment configuration is given by [c].
func New(dir string, c Config) (*Log, error) {
	// Configure defaults if not provided
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = defaultMaxIndex
	}

	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = defaultMaxStore
	}

	return setup(dir, c)
}

// Append appends a [record] to the log's active segment, returning its offset.
// If the segment is full after the append operation, a new segment is created
// and promoted to the active segment.
//
// If an error occurs when appending the [record], an offset of 0 is returned
// along with the error.
func (l *Log) Append(record *record.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	// If the active segment is full, create a new segment at the next offset
	// and promote to active segment.
	if l.activeSegment.IsFull() {
		s, err := segment.New(l.Dir, off+1, l.Config.Segment)
		if err != nil {
			return 0, err
		}

		l.segments = append(l.segments, s)
		l.activeSegment = s

		return off + 1, nil
	}

	return off, nil
}

// Read retrieves the record stored at [off]. The correct segment is chosen via
// linear search through the Log's segments. If [off] is outside the range of
// any segment, [ErrOutOfBounds] is returned.
//
// NOTE: Can we do anything about the linear search? Aren't these segments in
// increasing order???
func (l *Log) Read(off uint64) (*record.Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, seg := range l.segments {
		if seg.BaseOffset <= off && off < seg.NextOffset {
			return seg.Read(off)
		}
	}

	return nil, ErrOutOfBounds{off}
}

// Close closes each segment in the log.
//
// NOTE: This does not remove the files backing the segment. The Remove method
// is instead responsible for completely removing the underlying files.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Remove closes the log and removes ALL files in its backing directory. As a
// result, it is advised to persist the log to an exclusive directory.
//
// If an error occurs when closing the log, its directory will not be removed.
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

// LowestOffset retrieves the [BaseOffset] of the first segment. This is always
// the lowest offset since segments are totally ordered in the log.
func (l *Log) LowestOffset() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.segments[0].BaseOffset
}

// HighestOffset returns the highest occupied offset, that is, the [NextOffset]
// of the last segment minus 1 position. If the log is empty, this method will
// return 0.
func (l *Log) HighestOffset() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Isn't this always equivalent to the active segment??? This book is very
	// confusing...
	//
	// TODO: Verify whether the active segment is always the one at the end of
	// the slice.
	if off := l.segments[len(l.segments)-1].NextOffset; off > 0 {
		return off - 1
	}

	return 0
}

// Compact eliminates segments whose higest offset is lower than [lowest].
func (l *Log) Compact(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	// NOTE: It's like the author completely forgot we sorted these segments...
	// We only need to find the first segment whose highest offset is above the
	// threshold. Iterating over any segment after this is pointless!! As before,
	// I'm almost certain we could find such a segment in O(log n) instead of a
	// linear search.
	var segments []*segment.Segment
	for _, seg := range l.segments {
		if seg.NextOffset < lowest {
			if err := seg.Remove(); err != nil {
				return err
			}

			continue
		}

		segments = append(segments, seg)
	}

	l.segments = segments

	return nil
}

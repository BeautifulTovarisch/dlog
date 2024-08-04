package segment

import (
	"os"
	"testing"

	"github.com/beautifultovarisch/dlog/internal/commitlog/proto"
)

const (
	maxBytes = (1 << 10)
)

var (
	tmpDir = os.TempDir()
)

func TestSegment(t *testing.T) {
	run := func(name string, fn func(s *Segment, t *testing.T)) {
		t.Run(name, func(t *testing.T) {
			seg, err := New(tmpDir, 0, Config{maxBytes})
			if err != nil {
				t.Fatalf("error creating segment: %v", err)
			}

			t.Cleanup(func() {
				seg.Close()
			})

			fn(seg, t)
		})
	}

	t.Run("New", func(t *testing.T) {
		seg, err := New(tmpDir, 0, Config{maxBytes})
		if err != nil {
			t.Errorf("error creating segment: %v", err)
		}

		if actual := seg.baseOffset; actual != 0 {
			t.Errorf("expected baseoffset=%d. Got: %d", 0, actual)
		}

		if seg.Store == nil {
			t.Error("failed to initialize store")
		}

		if seg.Index == nil {
			t.Error("failed to initialize index")
		}

		t.Cleanup(func() {
			seg.Close()
		})
	})

	run("Append", func(s *Segment, t *testing.T) {
		record := proto.Record{
			Value: []byte("the record"),
		}

		off, err := s.Append(&record)
		if err != nil {
			t.Errorf("error appending record: %v", err)
		}

		// Should set the offset correctly
		if record.Offset != off {
			t.Errorf("expected record offset=%d. Got %d", record.Offset, off)
		}
	})
}

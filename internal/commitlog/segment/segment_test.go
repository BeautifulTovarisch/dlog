package segment

import (
	"fmt"
	"os"
	"testing"

	"github.com/beautifultovarisch/dlog/internal/schema"

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
				os.Remove(seg.index.File.Name())
				os.Remove(seg.store.File.Name())
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

		if seg.store == nil {
			t.Error("failed to initialize store")
		}

		if seg.index == nil {
			t.Error("failed to initialize index")
		}

		t.Cleanup(func() {
			seg.Close()

			os.Remove(seg.index.File.Name())
			os.Remove(seg.store.File.Name())
		})
	})

	run("Append", func(s *Segment, t *testing.T) {
		c, err := schema.GetCodec(schema.RECORD)
		if err != nil {
			t.Fatal(err)
		}

		msg := []byte("the record")

		record := proto.Record{
			Value: msg,
		}

		off, err := s.Append(&record)
		if err != nil {
			t.Errorf("error appending record: %v", err)
		}

		// Should set the offset correctly
		if record.Offset != off {
			t.Errorf("expected record offset=%d. Got %d", record.Offset, off)
		}

		// Read the index to retrieve the position in the store. Deserialize and
		// ensure the original record's data matches.
		pos, _, _ := s.index.Read(-1)
		data, _ := s.store.Read(uint64(pos))

		rec, _, err := c.NativeFromBinary(data)
		if err != nil {
			t.Errorf("error decoding store record: %v", err)
		}

		// This is the biggest argument against using goavro for this kind of thing
		// extremely verbose and annoying.
		if v, ok := rec.(map[string]interface{}); ok {
			value, ok := v["value"]
			if !ok {
				t.Errorf("record missing value field")
			}

			// A hack since directly converting to string does not work.
			if actual := fmt.Sprintf("%s", value); string(msg) != actual {
				t.Errorf("expected: %s. Got: %s", msg, actual)
			}
		}
	})

	run("Read", func(s *Segment, t *testing.T) {
		msg := []byte("hello, world!")
		record := proto.Record{
			Value: msg,
		}

		off, err := s.Append(&record)
		if err != nil {
			t.Fatalf("error appending record: %v", err)
		}

		rec, err := s.Read(off)
		if err != nil {
			t.Errorf("error reading segment: %v", err)
		}

		if actual := rec.Value; string(actual) != string(msg) {
			t.Errorf("invalid data from store. Expected: %s. Got: %s", actual, msg)
		}
	})
}

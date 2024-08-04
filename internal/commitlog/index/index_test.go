package index

import (
	"io"
	"os"
	"testing"
)

var maxBytes uint64 = (1 << 10)

func TestIndex(t *testing.T) {
	run := func(name string, fn func(i *Index, t *testing.T)) {
		t.Run(name, func(t *testing.T) {
			tmp, err := os.CreateTemp("", "index_test")
			if err != nil {
				t.Fatal(err)
			}

			i, err := New(tmp, maxBytes)
			if err != nil {
				t.Fatalf("error creating index: %v", err)
			}

			fn(i, t)

			t.Cleanup(func() {
				i.Close()
				os.Remove(tmp.Name())
			})
		})
	}

	t.Run("New", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			tmp, err := os.CreateTemp("", "index_empty")
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() {
				os.Remove(tmp.Name())
			})

			i, err := New(tmp, 0)
			if err != ErrEmptyFile {
				t.Errorf("expected New() to return EmptyFile given empty file")
			}

			if i != nil {
				t.Errorf("expected Index to be nil")
			}
		})

		t.Run("NonEmpty", func(t *testing.T) {
			tmp, err := os.CreateTemp("", "index_nonempty")
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() {
				os.Remove(tmp.Name())
			})

			i, err := New(tmp, maxBytes)
			if err != nil {
				t.Fatalf("error creating index: %v", err)
			}

			stat, err := os.Stat(i.File.Name())
			if err != nil {
				t.Fatal(err)
			}

			if size := stat.Size(); uint64(size) != maxBytes {
				t.Errorf("wrong index size. Expected: %d. Got: %d", maxBytes, size)
			}
		})
	})

	t.Run("Restore", func(t *testing.T) {
		tmp, err := os.CreateTemp("", "index_nonempty")
		if err != nil {
			t.Fatal(err)
		}

		i, err := New(tmp, maxBytes)
		if err != nil {
			t.Fatalf("error creating index: %v", err)
		}

		// Write to index, then create a new instance against the same file to
		// simulate a recovery scenario.

		i.Write(1, 10)
		i.Write(2, 20)

		i, _ = New(tmp, maxBytes)

		for k := 0; k < 2; k++ {
			o, p, _ := i.Read(int64(k))

			if expected := (k + 1); uint32(expected) != o {
				t.Errorf("expected: %d. Got %d", expected, o)
			}

			if expected := (k + 1) * 10; uint64(expected) != p {
				t.Errorf("expected: %d. Got %d", expected, p)
			}
		}

		t.Cleanup(func() {
			i.Close()
			os.Remove(tmp.Name())
		})
	})

	t.Run("Close", func(t *testing.T) {
		tmp, err := os.CreateTemp("", "index_close")
		if err != nil {
			t.Fatal(err)
		}

		i, err := New(tmp, maxBytes)
		if err != nil {
			t.Fatal(err)
		}

		if err := i.Close(); err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			os.Remove(tmp.Name())
		})
	})

	// No idea if these are good test inputs or not...
	run("Write", func(i *Index, t *testing.T) {
		tests := []struct {
			o uint32
			p uint64
		}{
			{0, 10},
			{10, 100},
			{5, 4},
			{2, 6},
		}

		for k, test := range tests {
			off, pos := test.o, test.p

			err := i.Write(off, pos)
			if err != nil {
				t.Errorf("error writing to (off=%d,pos=%d): %v", off, pos, err)
			}

			idx := k * recordWidth
			actualOff := enc.Uint32(i.buf[idx : idx+offsetWidth])
			actualPos := enc.Uint64(i.buf[idx+offsetWidth : idx+recordWidth])

			if actualOff != off {
				t.Errorf("expected offset %d. Got %d", off, actualOff)
			}

			if actualPos != pos {
				t.Errorf("expected position %d. Got %d", pos, actualPos)
			}
		}

		// force EOF
		i.buf = []byte{}
		err := i.Write(0, 1)
		if err != io.EOF {
			t.Error("expected EOF")
		}
	})

	run("Read", func(i *Index, t *testing.T) {
		// Force EOF
		old := i.size
		i.size = 0
		if _, _, err := i.Read(0); err != io.EOF {
			t.Error("expected EOF on read from empty index")
		}
		i.size = old

		tests := []struct {
			o uint32
			p uint64
		}{
			{0, 10},
			{10, 100},
			{5, 4},
			{2, 6},
		}

		for idx, test := range tests {
			off, pos := test.o, test.p

			enc.PutUint32(i.buf[i.size:i.size+offsetWidth], off)
			enc.PutUint64(i.buf[i.size+offsetWidth:i.size+recordWidth], pos)

			i.size += uint64(recordWidth)

			actualOff, actualPos, err := i.Read(int64(idx))
			if err != nil {
				t.Errorf("error reading from (off=%d,pos=%d): %v", off, pos, err)
			}

			if off != actualOff {
				t.Errorf("expected offset of %d. Got %d", off, actualOff)
			}

			if pos != actualPos {
				t.Errorf("expected position of %d. Got %d", pos, actualPos)
			}
		}

		actualOff, actualPos, err := i.Read(-1)
		if err != nil {
			t.Errorf("error reading from back of index: %v", err)
		}

		if expected := tests[len(tests)-1].o; actualOff != expected {
			t.Errorf("expected offset of %d. Got %d", expected, actualOff)
		}

		if expected := tests[len(tests)-1].p; actualPos != expected {
			t.Errorf("expected position of %d. Got %d", expected, actualPos)
		}
	})
}

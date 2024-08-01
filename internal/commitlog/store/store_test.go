package store

import (
	"os"
	"testing"
)

// Write bytes to a temporary file and assert that
func TestStoreAppend(t *testing.T) {
	tmp, err := os.CreateTemp("", "test_store")
	if err != nil {
		t.Fatalf("error creating temporary file: %v", err)
	}

	t.Cleanup(func() {
		os.Remove(tmp.Name())
	})

	t.Run("Empty", func(t *testing.T) {
		// appending an empty byte slice is a no-op
		store, err := New(tmp)
		if err != nil {
			t.Fatalf("error creating store: %v", err)
		}

		defer store.Close()

		n, pos, err := store.Append([]byte{})
		if err != nil {
			t.Fatalf("error appending bytes: %v", err)
		}

		// 8 is needed to store the length of the record.
		if n != 8 {
			t.Errorf("expected 8 bytes written. Got: %d", n)
		}

		if pos > 0 {
			t.Errorf("expected position to be 0. Got: %d", pos)
		}
	})

	t.Run("Write fixed", func(t *testing.T) {
		data := []byte("my data")
		n := uint64(len(data))

		store, err := New(tmp)
		if err != nil {
			t.Fatalf("error creating store: %v", err)
		}

		// Record the original size of the store.
		size := store.size

		length, pos, err := store.Append(data)
		if err != nil {
			t.Fatalf("error appending record: %v", err)
		}

		if length != n+lenWidth {
			t.Errorf("expected record length of %d. Got: %d", n+lenWidth, length)
		}

		if pos != lenWidth {
			t.Errorf("expected position of record to be %d. Got: %d", lenWidth, pos)
		}

		if store.size != size+n+lenWidth {
			t.Errorf("expected store size of %d. Got: %d", store.size, n+lenWidth)
		}
	})

	t.Run("Write variable", func(t *testing.T) {
		data := [][]byte{
			[]byte("aaaa"),
			[]byte("bbbb"),
			[]byte("cccc"),
			[]byte("hello, world!"),
			[]byte("goodnight, moon"),
		}

		store, err := New(tmp)
		if err != nil {
			t.Fatalf("error creating store: %v", err)
		}

		// Keep track of the previous position
		var prevPos, prevLen uint64 = 8, 0
		for _, d := range data {
			length, pos, err := store.Append(d)
			if err != nil {
				t.Fatalf("error appending record: %v", err)
			}

			// Expected length is the length of the bytes plus the metadata block.
			if expected := uint64(len(d)) + lenWidth; length != expected {
				t.Errorf("Expected record length of %d. Got: %d", expected, length)
			}

			// Expected position is defined in terms of the previous position and data
			// length.
			//
			// NOTE: the length returned from Read already includes the meta block!
			if expected := prevPos + prevLen; expected != pos {
				t.Errorf("Expected record position of %d. Got: %d", expected, pos)
			}

			prevPos, prevLen = pos, length
		}
	})
}

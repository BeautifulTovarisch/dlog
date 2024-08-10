package store

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestStore(t *testing.T) {
	run := func(name string, fn func(*Store, *testing.T)) {
		t.Run(name, func(t *testing.T) {
			tmp, err := os.CreateTemp("", "test_store")
			if err != nil {
				t.Fatal(err)
			}

			store, err := New(tmp)
			if err != nil {
				t.Fatalf("error creating store: %v", err)
			}

			t.Cleanup(func() {
				store.Close()
				os.Remove(tmp.Name())
			})

			fn(store, t)
		})
	}

	run("Append", func(store *Store, t *testing.T) {
		data := [][]byte{
			[]byte{},
			[]byte("aaaa"),
			[]byte("bbbb"),
			[]byte("cccc"),
			[]byte("hello, world!"),
			[]byte("goodnight, moon"),
		}

		// Keep track of the previous position
		var prevPos, prevLen uint64 = 0, 0
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

	// This test suite is interesting in that it uses Append in order to set up the
	// test file for Read operations. The jury is still out on whether this is poor
	// test design or not.
	run("Read", func(store *Store, t *testing.T) {
		tests := map[string][]byte{
			"":       []byte{},
			"first":  []byte("first"),
			"second": []byte("second"),
		}

		// Read back the records as we write them for a quick sanity check.
		for str, slice := range tests {
			_, pos, err := store.Append(slice)
			if err != nil {
				t.Errorf("error appending record: %v", err)
			}

			record, err := store.Read(pos)
			if err != nil && err != io.EOF {
				t.Errorf("error reading record: %v", err)
			}

			// This is evidently more efficient than the stanard for range loop since
			// string(a) == string(b) does not perform allocations.
			//
			// TODO: Dig into the details here for educational purposes.
			if !bytes.Equal(slice, record) {
				t.Errorf("expected record to equal %v. Got: %v", slice, record)
			}

			if actual := string(record); str != actual {
				t.Errorf("expected record to contain %s. Got: %v", str, actual)
			}
		}
	})

	// "Recovery" in this sense is the ability to 'recreate' a store destroyed by
	// some failure.
	t.Run("Recovery", func(t *testing.T) {
		msg := "canary"
		data := []byte(msg)

		tmp, err := os.CreateTemp("", "test_recover")
		if err != nil {
			t.Fatal(err)
		}

		// Create a store and append like normal.
		store, err := New(tmp)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			os.Remove(tmp.Name())
			store.Close()
		})

		_, pos, err := store.Append(data)
		if err != nil {
			t.Fatal(err)
		}

		// NOTE: A store 'naturally' flushes on Read, meaning appends without a
		// corresponding read or manual buffer flush will appear to be incorrect!
		if err := store.buf.Flush(); err != nil {
			t.Fatal(err)
		}

		// Create a new store pointing to the same file on disk. This represents an
		// attempt at recovery.
		store, err = New(tmp)
		if err != nil {
			t.Fatal(err)
		}

		record, err := store.Read(pos)
		if err != nil {
			t.Errorf("error reading from recovery store: %v", err)
		}

		if !bytes.Equal(record, data) {
			t.Errorf("expected %s from recovery store. Got %s", data, record)
		}
	})
}

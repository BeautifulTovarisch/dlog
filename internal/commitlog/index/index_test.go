package index

import (
	"os"
	"testing"
)

var maxBytes uint64 = (1 << 10)

func TestIndex(t *testing.T) {
	run := func(name string, fn func(i *Index, t *testing.T)) {
		tmp, err := os.CreateTemp("", "index_test")
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			os.Remove(tmp.Name())
		})
	}

	t.Run("New", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			tmp, err := os.CreateTemp("", "index_test")
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() {
				os.Remove(tmp.Name())
			})

			i, err := New(tmp, 0)
			if err != EmptyFile {
				t.Errorf("expected New() to return EmptyFile given empty file")
			}

			if i != nil {
				t.Errorf("expected Index to be nil")
			}
		})

		t.Run("NonEmpty", func(t *testing.T) {
			tmp, err := os.CreateTemp("", "index_test")
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

			if i.size != maxBytes {
				t.Errorf("wrong index size. Expected: %d. Got: %d", maxBytes, i.size)
			}
		})
	})

	run("Write", func(i *Index, t *testing.T) {
	})
	run("Read", func(i *Index, t *testing.T) {
	})
}

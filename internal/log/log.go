// package commitlog implements a commit log
package commitlog

import (
	"fmt"
	"sync"
)

// Record is an entry in a commit log
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

// Log is a basic commit log
type Log struct {
	mu      sync.Mutex
	records []Record
}

// It's not yet clear when we would need logs instantiated by the caller. Also
// unclear is a good mechanism to share such a log without embedding in a type
// and introducing convoluted dependency injection.
var globalLog Log

func read(log *Log, offset uint64) (Record, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	if offset >= uint64(len(log.records)) {
		return Record{}, fmt.Errorf("record not found at offset: %d", offset)
	}

	return log.records[offset], nil
}

func append(log *Log, record Record) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	offset := uint64(len(log.records))

	record.Offset = offset
	log.records = append(log.records, record)

	return offset, nil
}

// Read returns the record at [offset] or an error if [offset] is out of
// bounds.
//
// This function operations on the global, package-level log.
func Read(offset uint64) (Record, error) {
	return read(&globalLog, offset)
}

// Append places [record] at the end of [log]'s commit ledger and returns its
// off set. The offset of a record is computed as the current length of the log
//
//	[r1, r2, ... rn][new record]
//	               ^
//
// This function operations on the global, package-level log.
func (log *Log) Append(record Record) (uint64, error) {
	return append(log, record)
}

// Read returns the record at [offset] or RecordNotFound if [offset] is out of
// bounds.
func (log *Log) Read(offset uint64) (Record, error) {
	return read(log, offset)
}

// Append places [record] at the end of [log]'s commit ledger and returns its
// off set. The offset of a record is computed as the current length of the log
//
//	[r1, r2, ... rn][new record]
//	               ^
func (log *Log) Append(record Record) (uint64, error) {
	return append(log, record)
}

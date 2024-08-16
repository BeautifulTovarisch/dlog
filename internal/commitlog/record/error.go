package record

import (
	"fmt"
)

// RecordNotFound occurs when a given offset does not contain a record.
type RecordNotFound struct {
	Offset uint64
}

func (r RecordNotFound) Error() string {
	return fmt.Sprintf("record not found at offset: %d", r.Offset)
}

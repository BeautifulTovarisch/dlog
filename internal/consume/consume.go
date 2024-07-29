// package consume defines read operations on the commit log. The request and
// response types are dual to those found in the produce package.
package consume

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/beautifultovarisch/dlog/internal/commitlog"
)

// Request contains information for requesting a particular record based on an
// offset.
type Request struct {
	Offset uint64 `json:"offset"`
}

// Response is a record corresponding to an offset
type Response struct {
	Record commitlog.Record `json:"record"`
}

// GET /consume/{offset}
//
// Consume returns the record specified by [offset] or an error if not found.
func Consume(req Request, w http.ResponseWriter, r *http.Request) (*Response, error) {
	offset, err := strconv.ParseUint(r.PathValue("offset"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid offset: %v", offset)
	}

	w.Header().Set("x-trace-id", "123")

	record, err := commitlog.Read(offset)
	if err != nil {
		if errors.Is(err, commitlog.RecordNotFound{}) {
			w.WriteHeader(http.StatusNotFound)
		}

		return nil, err
	}

	res := Response{record}

	return &res, nil
}

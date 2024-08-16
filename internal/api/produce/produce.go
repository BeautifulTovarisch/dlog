// package produce specifies the POST /produce endpoint
package produce

import (
	"net/http"

	"github.com/beautifultovarisch/dlog/internal/commitlog/record"
)

// Request contains a [Record] to be appended to the commit log.
type Request struct {
	Record record.Record `json:"record"`
}

// Response contains the offset of a processed [Record] contained in a [Request]
type Response struct {
	Offset uint64 `json:"offset"`
}

// Produce accepts a [Request] containing a record and appends it to the commit
// log. A [Response] containing the offset of the response is returned.
func Produce(req Request, w http.ResponseWriter, r *http.Request) (*Response, error) {
	offset, err := record.Append(req.Record)
	if err != nil {
		return nil, err
	}

	res := Response{offset}

	return &res, nil
}

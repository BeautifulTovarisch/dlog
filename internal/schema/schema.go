// package schema provides mechanisms to encode and decode avros schemas. Avro
// schemas are located under directories corresponding to the package defining
// the data type (e.g internal/commitlog <=> schemas/commitlog)
package schema

import (
	"os"

	"github.com/linkedin/goavro"
)

// Provide paths to schemas for convenience and future-proofing.
const (
	COMMIT_LOG_RECORD = "./internal/schema/commitlog/record.json"
)

// MakeCodec accepts a path to an avro schema and produces a [goavro.Codec].
func MakeCodec(path string) (*goavro.Codec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return goavro.NewCodec(string(data))
}

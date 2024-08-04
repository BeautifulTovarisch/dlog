// package schema provides avro codecs for schemas located under the package's
// directory structure. Each codec corresponds to a constant defined in the
// package. Codecs are lazily and idempotently initialized.
package schema

import (
	_ "embed"

	"fmt"

	"github.com/linkedin/goavro"
)

// CODEC corresponds to a codec for a particular schema.
type CODEC uint8

const (
	RECORD CODEC = iota
)

var (
	//go:embed commitlog/record.json
	record string

	// Lookup associates a constant value representing a schema with the correct
	// avro codec.
	Lookup = make(map[CODEC]*goavro.Codec)
)

func getCodec(c CODEC, schema string) (*goavro.Codec, error) {
	codec, ok := Lookup[c]
	if ok {
		return codec, nil
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}

	// Store the codec for future lookups
	Lookup[c] = codec

	return codec, nil
}

// GetCodec retrieves the codec specified by [c]. The codec will be initialized
// only on the first call to GetCodec; subsequent invocations are idempotent.
func GetCodec(c CODEC) (*goavro.Codec, error) {
	switch c {
	case RECORD:
		return getCodec(c, record)
	default:
		return nil, fmt.Errorf("codec not found")
	}
}

package server

import (
	"io"
	"net/http"

	"github.com/linkedin/goavro"
)

// RouteAvro associates the handler [f] with requests matching [path]. Inputs
// are encoded and outputs decoded according to [in] and [out], otherwise this
// function behaves exactly like [Route] with the important exception that the
// input and output types are not generic.
//
// If [in] or [out] are nil, the corresponding operation for that codec is not
// performed.
func RouteAvro[Req any, Res any](path string, in, out *goavro.Codec, f Handler[Req, Res]) {
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		var req Req
		// Optionally decode the input into the specified type.
		if in != nil {
			native, _, err := in.NativeFromTextual(body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)

				return
			}

			if v, ok := native.(Req); ok {
				req = v
			}
		}

		res, err := handleRequest(req, w, r, f)
		if err != nil {
			// Do we encode the error as binary too?
			// TODO: Figure out what to actually do here.
			w.Write([]byte(err.Error() + "\n"))

			return
		}

		// Optionally decode the output
		if out != nil {
			binary, err := out.BinaryFromNative(nil, *res)
			if err != nil {
				w.Write([]byte(err.Error() + "\n"))

				return
			}

			w.Write(binary)

			return
		}

		// Write nothing to the client.
		w.Write([]byte{})
	})
}

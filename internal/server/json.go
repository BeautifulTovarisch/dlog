package server

import (
	"encoding/json"
	"io"
	"net/http"
)

// Route associates the handler function [f] with requests that match [path].
// Input and output data are serialized as JSON
//
// Example:
//
//	type MyInput struct {}
//	type MyOutput struct {}
//
//	Route("GET /", func(MyInput, r *http.Request)(*MyOutput, error) {
//	  return MyOutput{}, nil
//	})
func Route[Req any, Res any](path string, f Handler[Req, Res]) {
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		// Automagically deserialize the input type from the request body.
		var req Req
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			// Ignore if the error is caused by an empty request body.
			if err != io.EOF {
				http.Error(w, err.Error(), http.StatusBadRequest)

				return
			}
		}

		res, err := handleRequest(req, w, r, f)
		if err != nil {
			json.NewEncoder(w).Encode(err.Error())

			return
		}

		// Provider ResponseWriter as a write stream and simply pipe [res] to the
		// client.
		if res != nil {
			json.NewEncoder(w).Encode(res)
		}
	})
}

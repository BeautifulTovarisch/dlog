// package server handles the basic creation and shutdown of an HTTP server.
package server

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"os"
	"os/signal"
)

// Allow users to provide input and output types to support more "go-like" HTTP
// handlers. This package implements a custom [ResponseWriter] to capture data
// about the request.
type Handler[Req any, Res any] func(Req, http.ResponseWriter, *http.Request) (*Res, error)

// Basically exists to act like a spy and gather headers and the status code
// while removing the ability to write to the client from the handler directly.
type responseWriter struct {
	header http.Header
	status int
}

func (r *responseWriter) Header() http.Header {
	return r.header
}

func (r *responseWriter) WriteHeader(status int) {
	r.status = status
}

// Write is implemented as a no-op as the custom response writer only exists to
// capture values from the HTTP handler. The actual writing is performed by the
// [http.ResponseWriter] provided to [HandleFunc].
func (r responseWriter) Write(b []byte) (int, error) {
	return 0, nil
}

var (
	mux *http.ServeMux
	srv http.Server
)

// TODO: Have a proper configuration flow.
func init() {
	mux = http.NewServeMux()
	srv = http.Server{
		Addr: "127.0.0.1:8080",
	}
}

// Create a goroutine to listen of SIGINT, SIGTERM, etc... and allow the caller
// to block until gracefully shut down.
func shutdown() <-chan struct{} {
	// Channel to block until idle connections are closed.
	conns := make(chan struct{})
	defer close(conns)

	// This goroutine blocks until receiving SIGINT
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)

		<-sigint

		Shutdown()
	}()

	return conns
}

// Route associates the handler function [f] with requests that match [path]
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

		// Allow these default headers to be overwritten by the handler
		hdr := http.Header{
			"Content-Type": []string{"application/json"},
		}

		rw := &responseWriter{
			header: hdr,
		}

		// Perform the request.
		res, err := f(req, rw, r)

		// Copy headers and status to the ResponseWriter actually performing the I/O
		maps.Copy(w.Header(), rw.Header())

		if err != nil {
			if rw.status != 0 {
				w.WriteHeader(rw.status)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}

			json.NewEncoder(w).Encode(err.Error())

			return
		}

		// The call to Write() will automatically set the status to 200 if not set
		// after this point.
		if rw.status != 0 {
			w.WriteHeader(rw.status)
		}

		// Provider ResponseWriter as a write stream and simply pipe [res] to the
		// client.
		if res != nil {
			json.NewEncoder(w).Encode(res)
		}
	})
}

// Shutdown attempts a graceful shutdown of the HTTP server, panicking on error
func Shutdown() {
	if err := srv.Shutdown(context.Background()); err != nil {
		panic(err)
	}
}

// Run starts the HTTP server and does not return except on a fatal error.
func Run() {
	cxn := shutdown()

	srv.Handler = mux
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		panic(err)
	}

	<-cxn
}

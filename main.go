// Distributed Commit Log
//
// This application implements a basic distributed commit (append-only) log.
package main

import (
	"net/http"

	"github.com/beautifultovarisch/dlog/internal/server"
)

// Placeholders until I wrangle this mess I've made.
type Input struct{}
type Output struct{}

func main() {
	server.Route("GET /{$}", func(in Input, w http.ResponseWriter, r *http.Request) (*Output, error) {
		return nil, nil
	})

	server.Route("GET /pizza", func(in Input, w http.ResponseWriter, r *http.Request) (*Output, error) {
		return nil, nil
	})

	server.Run()
}

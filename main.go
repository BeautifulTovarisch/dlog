// Distributed Commit Log
//
// This application implements a basic distributed commit (append-only) log.
package main

import (
	"net/http"

	"github.com/beautifultovarisch/distributed-project/internal/server"
)

func main() {
	server.Route("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello, world!\n"))
	})

	server.Route("GET /pizza", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pizza pie!\n"))
	})

	server.Run()
}

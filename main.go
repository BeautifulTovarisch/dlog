// Distributed Commit Log
//
// This application implements a basic distributed commit (append-only) log.
package main

import (
	"github.com/beautifultovarisch/dlog/internal/server"

	"github.com/beautifultovarisch/dlog/internal/api/consume"
	"github.com/beautifultovarisch/dlog/internal/api/produce"
)

func main() {
	server.Route("GET /consume/{offset}", consume.Consume)
	server.Route("POST /produce", produce.Produce)

	server.Run()
}

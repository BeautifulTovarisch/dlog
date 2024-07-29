// Distributed Commit Log
//
// This application implements a basic distributed commit (append-only) log.
package main

import (
	"net/http"

	"github.com/beautifultovarisch/dlog/internal/consume"
	"github.com/beautifultovarisch/dlog/internal/produce"
	"github.com/beautifultovarisch/dlog/internal/schema"
	"github.com/beautifultovarisch/dlog/internal/server"
)

// TODO: Play around with these types.
type Req struct {
	A string
}

type Res map[string]interface{}

func handleAvro(req Req, w http.ResponseWriter, r *http.Request) (*Res, error) {
  res := make(Res)

	return &res, nil
}

func main() {
	codec, err := schema.MakeCodec(schema.COMMIT_LOG_RECORD)
	if err != nil {
		panic(err)
	}

	server.Route("GET /consume/{offset}", consume.Consume)
	server.Route("POST /produce", produce.Produce)

	// No need to decode incoming input.
	server.RouteAvro[Req, Res]("GET /avro", nil, codec, handleAvro)

	server.Run()
}

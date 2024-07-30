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

type Req struct {
	A string
}

func handleAvro(req Req, w http.ResponseWriter, r *http.Request) (*map[string]interface{}, error) {
	res := make(map[string]interface{})

  res["offset"] = 1
  res["value"] = []byte{}

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
	server.RouteAvro("GET /avro", nil, codec, handleAvro)

	server.Run()
}

package main

import (
	"context"
	"github.com/go-po/po"
	"github.com/go-po/po/examples/teller/api"
	"github.com/go-po/po/examples/teller/app"
	"github.com/go-po/po/examples/teller/commands"
	"github.com/go-po/po/internal/broker/channels"
	"github.com/go-po/po/internal/store/inmemory"
	"log"
	"net/http"
)

func main() {

	rootCtx := context.Background()

	db := inmemory.New()
	store := po.New(db, channels.New())

	err := store.Subscribe(rootCtx, "command handler", "vars:commands", commands.NewCommandSubscriber(store))
	if err != nil {
		log.Fatalf("failed subscribing: %s", err)
	}

	app := app.New(store)
	err = http.ListenAndServe(":8000", api.Root(app))
	log.Printf("server stopped: %s", err)

}

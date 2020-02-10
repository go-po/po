package main

import (
	"context"
	"github.com/go-po/po"
	"github.com/go-po/po/examples/simple"
	"github.com/go-po/po/internal/broker/channels"
	"github.com/go-po/po/internal/store/inmemory"
	"log"
	"time"
)

func main() {
	rootCtx := context.Background()
	db := inmemory.New()
	store := po.New(db, channels.New(db))
	err := store.Subscribe(rootCtx, "messages handler", "messages", simple.Sub{})
	if err != nil {
		log.Fatalf("failed subscribing: %s", err)
	}

	err = store.Stream(context.Background(), "messages").
		Append(
			simple.HelloMessage{Greeting: "world"},
			simple.HelloMessage{Greeting: "my friend"},
			simple.HelloMessage{Greeting: "to you as well!"},
		)

	if err != nil {
		log.Fatalf("failed appending: %s", err)
	}

	err = store.Stream(context.Background(), "messages-german").
		Append(
			simple.HelloMessage{Greeting: "Guten Tag"},
		)

	if err != nil {
		log.Fatalf("failed appending: %s", err)
	}

	// message delivery is eventually consistent. Give time to build that up.
	time.Sleep(50 * time.Millisecond)
}

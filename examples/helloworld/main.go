package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-po/po"
	"github.com/go-po/po/streams"
	"log"
	"time"
)

func main() {
	rootCtx := context.Background()
	es := po.New(po.NewStoreInMemory(), po.NewProtocolChannels())

	id := streams.ParseId("messages")
	err := es.Subscribe(rootCtx, "messages handler", id, Subscriber{})
	if err != nil {
		log.Fatalf("failed subscribing: %s", err)
	}

	_, err = es.Stream(context.Background(), streams.ParseId("messages")).
		Append(
			HelloMessage{Greeting: "world"},
			HelloMessage{Greeting: "my friend"},
			HelloMessage{Greeting: "to you as well!"},
		)

	if err != nil {
		log.Fatalf("failed appending: %s", err)
	}

	_, err = es.Stream(context.Background(), streams.ParseId("messages-german")).
		Append(
			HelloMessage{Greeting: "Guten Tag"},
		)

	if err != nil {
		log.Fatalf("failed appending: %s", err)
	}

	// message delivery is eventually consistent. Give time to build that up.
	time.Sleep(50 * time.Millisecond)
}

// Message Definitions

type HelloMessage struct {
	Greeting string
}

func init() {
	po.RegisterMessages(
		func(b []byte) (interface{}, error) {
			msg := HelloMessage{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
	)
}

// A Message Subscriber

type Subscriber struct{}

func (sub Subscriber) Handle(ctx context.Context, msg streams.Message) error {
	switch message := msg.Data.(type) {
	case HelloMessage:
		fmt.Printf("[%d/%d] {%s} Greet: %s\n", msg.Number, msg.GroupNumber, msg.Stream, message.Greeting)
	default:
		fmt.Printf("[%d/%d] {%s} Unknown type: %T\n", msg.Number, msg.GroupNumber, msg.Stream, message)
	}
	return nil
}

// implements stream.Handler
var _ streams.Handler = Subscriber{}

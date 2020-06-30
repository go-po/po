package broker

import (
	"context"

	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
)

func newStreamHandler(id streams.Id, subscriberId string, store Store, inner Handler) *streamHandler {
	return &streamHandler{
		id:       subscriberId,
		store:    store,
		handler:  inner,
		stream:   id,
		position: -1,
	}
}

type streamHandler struct {
	id       string
	handler  Handler
	stream   streams.Id
	position int64
	store    Store
}

func (sh *streamHandler) Handle(ctx context.Context, msg streams.Message) error {
	var nextPosition int64
	if sh.stream.HasEntity() {
		if sh.stream.Entity != msg.Stream.Entity {
			return nil
		}
		if msg.Number > sh.position {
			return nil
		}
		nextPosition = msg.Number
	} else {
		if msg.GroupNumber <= sh.position {
			return nil
		}
		nextPosition = msg.GroupNumber
	}
	err := sh.handler.Handle(ctx, msg)
	if err != nil {
		return err
	}
	sh.position = nextPosition
	return nil
}

func (sh *streamHandler) Process(ctx context.Context, tx store.Tx, inbox <-chan streams.Message) {
	failed := false
	for msg := range inbox {
		if failed {
			continue
		}
		err := sh.Handle(ctx, msg)
		if err != nil {
			// TODO log this error
			failed = true
		}
	}
	err := sh.store.SetSubscriptionPosition(tx, sh.stream, store.SubscriptionPosition{
		SubscriptionId: sh.id,
		Position:       sh.position,
	})
	if err != nil {
		// TODO log this error
	}

}

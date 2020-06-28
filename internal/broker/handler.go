package broker

import (
	"context"

	"github.com/go-po/po/streams"
)

func newStreamHandler(id streams.Id, inner Handler) *streamHandler {
	return &streamHandler{
		handler:  inner,
		stream:   id,
		position: -1,
	}
}

type streamHandler struct {
	handler  Handler
	stream   streams.Id
	position int64
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

package broker

import (
	"context"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
)

type Handler interface {
	// Receives messages from a stream
	Handle(ctx context.Context, msg streams.Message) error
}

type transport interface {
	Publish(ctx context.Context, channel string) error
}

type OptimisticBroker struct {
}

func (broker *OptimisticBroker) Notify(ctx context.Context, records ...record.Record) error {
	panic("implement me")
}

func (broker *OptimisticBroker) Register(ctx context.Context, subscriberId string, streamId streams.Id, subscriber Handler) error {
	panic("implement me")
}

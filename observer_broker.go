package po

import (
	"context"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
)

func observeBroker(broker Broker) *observesBroker {
	return &observesBroker{broker: broker}
}

type observesBroker struct {
	broker Broker
}

func (obs *observesBroker) Notify(ctx context.Context, positions ...record.Record) error {
	return obs.broker.Notify(ctx, positions...)
}

func (obs *observesBroker) Register(ctx context.Context, subscriberId string, streamId streams.Id, subscriber streams.Handler) error {
	return obs.Register(ctx, subscriberId, streamId, subscriber)
}

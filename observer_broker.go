package po

import (
	"context"
	"strconv"

	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/internal/observer/binary"
	"github.com/go-po/po/internal/observer/unary"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
)

func observeBroker(broker Broker, obs *observer.Builder) *observesBroker {
	return &observesBroker{
		broker:   broker,
		onNotify: obs.Unary().Build(),
		onRegister: obs.Binary().
			LogInfof("broker register stream:%s subscriber:%s").
			Build(),
	}
}

type observesBroker struct {
	broker     Broker
	onNotify   unary.ClientTrace
	onRegister binary.ClientTrace
}

func (obs *observesBroker) Notify(ctx context.Context, positions ...record.Record) error {
	done := obs.onNotify.Observe(ctx, strconv.Itoa(len(positions)))
	defer done()
	return obs.broker.Notify(ctx, positions...)
}

func (obs *observesBroker) Register(ctx context.Context, subscriberId string, streamId streams.Id, subscriber streams.Handler) error {
	done := obs.onRegister.Observe(ctx, streamId.String(), subscriberId)
	defer done()
	return obs.broker.Register(ctx, subscriberId, streamId, subscriber)
}

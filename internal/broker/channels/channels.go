package channels

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
)

func New() *Channels {
	return &Channels{}
}

type Channels struct {
	pub         *publisher
	sub         *subscriber
	distributor broker.Distributor
}

func (ch *Channels) Subscribe(ctx context.Context, streamId stream.Id) error {
	streamChannel := ch.pub.getChan(ctx, streamId)
	err := ch.sub.addInbound(ctx, streamId, streamChannel)
	if err != nil {
		return err
	}

	return nil
}

func (ch *Channels) Prepare(distributor broker.Distributor, groupAssigner broker.GroupAssigner) {
	ch.distributor = distributor
	ch.pub = newPublisher()
	ch.sub = newSubscriber(groupAssigner)
}

func (ch *Channels) Notify(ctx context.Context, records ...record.Record) error {
	for _, record := range records {
		err := ch.pub.notify(ctx, record)
		if err != nil {
			return err
		}
	}
	return nil
}

func wrapSubscriber(subscriber interface{}) (stream.Handler, error) {
	switch h := subscriber.(type) {
	case stream.Handler:
		return h, nil
	default:
		return nil, fmt.Errorf("no way to handle")
	}
}

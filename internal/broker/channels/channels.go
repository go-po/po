package channels

import (
	"context"
	"fmt"
	"github.com/go-po/po"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
)

func New(seq GroupNumberAssigner) *Channels {
	return &Channels{
		pub: newPublisher(),
		sub: newSubscriber(seq),
	}
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

func (ch *Channels) Distributor(distributor broker.Distributor) {
	ch.distributor = distributor
}

var _ po.Broker = &Channels{}

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

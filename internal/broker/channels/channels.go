package channels

import (
	"context"
	"fmt"
	"github.com/go-po/po"
	"github.com/go-po/po/internal/record"
)

func New(seq GroupNumberAssigner) *Channels {
	return &Channels{
		pub: newPublisher(),
		sub: newSubscriber(seq),
	}
}

type Channels struct {
	pub *publisher
	sub *subscriber
}

var _ po.Broker = &Channels{}

func (ch *Channels) Subscribe(ctx context.Context, subscriptionId, stream string, subscriber interface{}) error {
	streamId := po.ParseStreamId(stream)

	streamChannel := ch.pub.getChan(ctx, streamId)

	err := ch.sub.addInbound(ctx, streamId, streamChannel, subscriber)
	if err != nil {
		return err
	}

	return nil
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

func wrapSubscriber(subscriber interface{}) (po.Handler, error) {
	switch h := subscriber.(type) {
	case po.Handler:
		return h, nil
	default:
		return nil, fmt.Errorf("no way to handle")
	}
}

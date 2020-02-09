package channels

import (
	"context"
	"github.com/go-po/po"
	"github.com/go-po/po/internal/record"
	"sync"
)

func newPublisher() *publisher {
	return &publisher{
		out: make(map[string]chan record.Record),
	}
}

type publisher struct {
	out map[string]chan record.Record
	mu  sync.Mutex
}

func (pub *publisher) getChan(context context.Context, streamId po.StreamId) chan record.Record {
	pub.mu.Lock()
	defer pub.mu.Unlock()
	_, ok := pub.out[streamId.Group]
	if !ok {
		pub.out[streamId.Group] = make(chan record.Record)
	}
	return pub.out[streamId.Group]
}

func (pub *publisher) notify(ctx context.Context, record record.Record) error {
	streamId := po.ParseStreamId(record.Stream)
	go func() {
		ch := pub.getChan(ctx, streamId)
		ch <- record
	}()
	return nil

}

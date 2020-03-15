package channels

import (
	"context"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
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

func (pub *publisher) getChan(context context.Context, streamId stream.Id) chan record.Record {
	pub.mu.Lock()
	defer pub.mu.Unlock()
	_, ok := pub.out[streamId.Group]
	if !ok {
		pub.out[streamId.Group] = make(chan record.Record)
	}
	return pub.out[streamId.Group]
}

func (pub *publisher) notify(ctx context.Context, record record.Record) error {
	ch := pub.getChan(ctx, record.Stream)
	ch <- record
	return nil

}

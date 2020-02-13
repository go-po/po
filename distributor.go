package po

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/stream"
	"sync"
)

func newDistributor() *distributor {
	return &distributor{
		subs: make(map[string][]Handler),
	}
}

type distributor struct {
	mu   sync.Mutex           // guards below maps
	subs map[string][]Handler // stream group to handler
}

func (dist *distributor) Register(ctx context.Context, subscriberId string, stream stream.Id, subscriber interface{}) error {
	dist.mu.Lock()
	defer dist.mu.Unlock()
	handler, err := wrapSubscriber(subscriber)
	if err != nil {
		return err
	}
	dist.subs[stream.Group] = append(dist.subs[stream.Group], handler)

	return nil
}

func (dist *distributor) Distribute(ctx context.Context, record record.Record) error {
	stream := stream.ParseId(record.Stream)
	subs, hasSubs := dist.subs[stream.Group]
	if !hasSubs {
		return nil
	}
	var msg Message
	msg, err := ToMessage(registry.DefaultRegistry, record)
	if err != nil {
		// TODO faulty implementation, catch later
		return err
	}
	for _, sub := range subs {
		err := sub.Handle(ctx, msg)
		if err != nil {
			// TODO faulty implementation, catch later
			return err
		}
	}
	return nil
}

func wrapSubscriber(subscriber interface{}) (Handler, error) {
	switch h := subscriber.(type) {
	case Handler:
		return h, nil
	default:
		return nil, fmt.Errorf("no way to handle")
	}
}

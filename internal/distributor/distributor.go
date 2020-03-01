package distributor

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/stream"
	"sync"
)

type registry interface {
	ToMessage(r record.Record) (stream.Message, error)
}

func New(registry registry, store Store) *distributor {
	return &distributor{
		subs:     make(map[string][]stream.Handler),
		registry: registry,
		store:    store,
	}
}

type distributor struct {
	registry registry
	mu       sync.Mutex // guards below maps
	subs     map[string][]stream.Handler
	store    Store
}

type Store interface {
	Begin(ctx context.Context) (store.Tx, error)
	GetLastPosition(tx store.Tx, subscriberId string, stream stream.Id) (int64, error)
	SetPosition(tx store.Tx, subscriberId string, stream stream.Id, position int64) error
	ReadRecordsFrom(ctx context.Context, id stream.Id, from int64) ([]record.Record, error)
}

func (dist *distributor) Register(ctx context.Context, subscriberId string, stream stream.Id, subscriber interface{}) error {
	dist.mu.Lock()
	defer dist.mu.Unlock()
	handler, err := wrapSubscriber(subscriber)
	if err != nil {
		return err
	}

	dist.subs[stream.Group] = append(dist.subs[stream.Group], &recordingSubscription{
		id:       subscriberId,
		stream:   stream,
		handler:  handler,
		store:    dist.store,
		registry: dist.registry,
	})

	return nil
}

func (dist *distributor) Distribute(ctx context.Context, record record.Record) (bool, error) {
	subs, hasSubs := dist.subs[record.Stream.Group]
	if !hasSubs {
		return false, nil
	}

	msg, err := dist.registry.ToMessage(record)
	if err != nil {
		// TODO faulty implementation, catch later
		return false, fmt.Errorf("dist: %w", err)
	}

	for _, sub := range subs {
		err = sub.Handle(ctx, msg)
		if err != nil {
			// TODO faulty implementation, catch later
			return false, err
		}
	}
	return true, nil
}

func wrapSubscriber(subscriber interface{}) (stream.Handler, error) {
	switch h := subscriber.(type) {
	case stream.Handler:
		return h, nil
	default:
		return nil, fmt.Errorf("no way to handle")
	}
}

package distributor

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
)

type registry interface {
	ToMessage(r record.Record) (streams.Message, error)
}

func New(registry registry, store Store) *distributor {
	return &distributor{
		subs:     make(map[string][]streams.Handler),
		registry: registry,
		store:    store,
	}
}

type distributor struct {
	registry registry
	mu       sync.Mutex // guards below maps
	subs     map[string][]streams.Handler
	store    Store
}

type Store interface {
	Begin(ctx context.Context) (store.Tx, error)
	GetSubscriberPosition(tx store.Tx, subscriberId string, stream streams.Id) (int64, error)
	SetSubscriberPosition(tx store.Tx, subscriberId string, stream streams.Id, position int64) error
	ReadRecords(ctx context.Context, id streams.Id, from int64) ([]record.Record, error)
}

// package facade for hiding record/message translations
// designed to be specific for the individual subscriber
type messageStore interface {
	Begin(ctx context.Context) (store.Tx, error)
	GetLastPosition(tx store.Tx) (int64, error)
	SetPosition(tx store.Tx, position int64) error
	ReadMessages(ctx context.Context, from int64) ([]streams.Message, error)
}

func (dist *distributor) Register(ctx context.Context, subscriberId string, stream streams.Id, subscriber interface{}) error {
	dist.mu.Lock()
	defer dist.mu.Unlock()
	handler, err := wrapSubscriber(subscriber)
	if err != nil {
		return err
	}

	dist.subs[stream.Group] = append(dist.subs[stream.Group], &recordingSubscription{
		groupStream: !stream.HasEntity(),
		handler:     handler,
		store:       newMsgStore(dist.store, dist.registry, stream, subscriberId),
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

func wrapSubscriber(subscriber interface{}) (streams.Handler, error) {
	switch h := subscriber.(type) {
	case streams.Handler:
		return h, nil
	default:
		return nil, fmt.Errorf("no way to handle")
	}
}

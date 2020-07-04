package broker

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
)

type Handler interface {
	// Receives messages from a stream
	Handle(ctx context.Context, msg streams.Message) error
}

type Registry interface {
	ToMessage(r record.Record) (streams.Message, error)
}

type Store interface {
	Begin(ctx context.Context) (store.Tx, error)
	SubscriptionPositionLock(tx store.Tx, id streams.Id, subscriptionIds ...string) ([]store.SubscriptionPosition, error)
	ReadRecords(ctx context.Context, id streams.Id, from, to, limit int64) ([]record.Record, error)
	SetSubscriptionPosition(tx store.Tx, id streams.Id, position store.SubscriptionPosition) error
}

type RecordHandler interface {
	Handle(ctx context.Context, record record.Record) (bool, error)
}
type RecordHandlerFunc func(ctx context.Context, record record.Record) (bool, error)

func (fn RecordHandlerFunc) Handle(ctx context.Context, record record.Record) (bool, error) {
	return fn(ctx, record)
}

type Protocol interface {
	Register(ctx context.Context, group string, input RecordHandler) (RecordHandler, error)
}

type Subscription interface {
	Handle(ctx context.Context, record record.Record) (bool, error)
	AddSubscriber(id streams.Id, subscriptionId string, subscriber streams.Handler)
}

func New(store Store, registry Registry, protocol Protocol) *Broker {
	return &Broker{
		store:       store,
		registry:    registry,
		protocol:    protocol,
		mu:          sync.Mutex{},
		subscribers: make(map[string]Subscription),
		publishers:  make(map[string]RecordHandler),
	}
}

type Broker struct {
	store    Store
	registry Registry
	protocol Protocol

	mu          sync.Mutex
	subscribers map[string]Subscription
	publishers  map[string]RecordHandler
}

func (broker *Broker) Notify(ctx context.Context, records ...record.Record) error {
	for _, record := range records {
		err := broker.notify(ctx, record)
		if err != nil {
			return err
		}
	}
	return nil
}

func (broker *Broker) notify(ctx context.Context, r record.Record) error {
	h, found := broker.publishers[r.Group]
	if !found {
		return fmt.Errorf("missing subscriber: %s", r.Group)
	}
	send, err := h.Handle(ctx, r)
	if err != nil {
		return err
	}
	if !send {
		return fmt.Errorf("failed to publish %s:%d", r.Stream, r.Number)
	}
	return nil
}

func (broker *Broker) Register(ctx context.Context, subscriberId string, streamId streams.Id, subscriber streams.Handler) error {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	sub, found := broker.subscribers[streamId.Group]
	if !found {
		sub = newSub(broker.registry, broker.store, streamId.Group)
		broker.subscribers[streamId.Group] = sub
	}

	_, found = broker.publishers[streamId.Group]
	if !found {
		publisher, err := broker.protocol.Register(ctx, streamId.Group, sub)
		if err != nil {
			return err
		}
		broker.publishers[streamId.Group] = publisher
	}

	sub.AddSubscriber(streamId, subscriberId, subscriber)

	return nil
}

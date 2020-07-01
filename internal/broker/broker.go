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

func New(store Store, registry Registry, protocol Protocol) *OptimisticBroker {
	return &OptimisticBroker{
		store:         store,
		registry:      registry,
		protocol:      protocol,
		mu:            sync.Mutex{},
		subscriptions: make(map[string]*subscription),
	}
}

type OptimisticBroker struct {
	store    Store
	registry Registry
	protocol Protocol

	mu            sync.Mutex
	subscriptions map[string]*subscription
}

func (broker *OptimisticBroker) Notify(ctx context.Context, records ...record.Record) error {
	for _, record := range records {
		err := broker.notify(ctx, record)
		if err != nil {
			return err
		}
	}
	return nil
}

func (broker *OptimisticBroker) notify(ctx context.Context, r record.Record) error {
	h, found := broker.subscriptions[r.Group]
	if !found {
		return fmt.Errorf("missing subscriber: %s", r.Group)
	}
	send, err := h.publisher.Handle(ctx, r)
	if err != nil {
		return err
	}
	if !send {
		return fmt.Errorf("failed to publish %s:%d", r.Stream, r.Number)
	}
	return nil
}

func (broker *OptimisticBroker) Register(ctx context.Context, subscriberId string, streamId streams.Id, subscriber streams.Handler) error {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	sub, found := broker.subscriptions[streamId.Group]
	if !found {
		sub = newSub(broker.registry, broker.store, streamId.Group)
		publisher, err := broker.protocol.Register(ctx, streamId.Group, sub)
		if err != nil {
			return err
		}
		sub.publisher = publisher
		broker.subscriptions[streamId.Group] = sub
	}

	sub.AddSubscriber(streamId, subscriberId, subscriber)

	return nil
}

package broker

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
)

func newSub(registry Registry, store Store, group string) *subscription {
	return &subscription{
		mu:            sync.Mutex{},
		subscriptions: make(map[string]*streamHandler),
		stream:        streams.ParseId(group),
		ids:           nil,
		publisher:     nil,
		store:         store,
		registry:      registry,
	}
}

type subscription struct {
	mu            sync.Mutex
	subscriptions map[string]*streamHandler
	ids           []string
	publisher     RecordHandler
	stream        streams.Id
	store         Store
	registry      Registry
}

// called when messages are received from the protocol transport
func (sub *subscription) Handle(ctx context.Context, record record.Record) (bool, error) {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	if len(sub.ids) == 0 {
		return true, nil
	}

	tx, err := sub.store.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	min, err := updatePosition(sub.store, tx, sub.stream, sub.subscriptions)
	if err != nil {
		return false, err
	}

	// TODO Split into using Pagination
	// This runs the risk of reading many records for
	// a first subscriber on an old group of streams
	records, err := sub.store.ReadRecords(ctx, sub.stream, min, math.MaxInt64)
	if err != nil {
		return false, err
	}

	// TODO Use Channels instead
	// Potential performance gain of splitting this into
	// concurrent channels
	for _, r := range records {
		msg, err := sub.registry.ToMessage(r)
		if err != nil {
			return false, err
		}
		for _, s := range sub.subscriptions {
			err = s.Handle(ctx, msg)
			if err != nil {
				return true, err
			}
		}
	}

	var errs []error
	for id, s := range sub.subscriptions {
		err = sub.store.SetSubscriptionPosition(tx, sub.stream, store.SubscriptionPosition{
			SubscriptionId: id,
			Position:       s.position,
		})
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		// TODO Add Custom error
		return false, fmt.Errorf("could not write positions")
	}

	err = tx.Commit()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (sub *subscription) AddSubscriber(id streams.Id, subscriberId string, subscriber Handler) {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	sub.subscriptions[subscriberId] = newStreamHandler(id, subscriber)
	sub.ids = append(sub.ids, subscriberId)
}

func updatePosition(dao Store, tx store.Tx, id streams.Id, subs map[string]*streamHandler) (int64, error) {
	var ids []string
	for subId, _ := range subs {
		ids = append(ids, subId)
	}
	positions, err := dao.SubscriptionPositionLock(tx, id, ids...)
	if err != nil {
		return -1, err
	}

	if len(positions) == 0 {
		for id, _ := range subs {
			positions = append(positions, store.SubscriptionPosition{
				SubscriptionId: id,
				Position:       -1,
			})
		}
	}

	var leastPosition int64 = positions[0].Position
	for _, p := range positions {
		if p.Position < leastPosition {
			leastPosition = p.Position
		}
		subscriber, found := subs[p.SubscriptionId]
		if found {
			subscriber.position = p.Position
		}
	}
	return leastPosition, nil
}

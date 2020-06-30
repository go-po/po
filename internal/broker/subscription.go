package broker

import (
	"context"
	"sync"

	"github.com/go-po/po/internal/pager"
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

	var to int64 = record.GroupNumber
	if sub.stream.HasEntity() {
		to = record.Number
	}

	boxes, wg := sub.startSubscriptionProcessors(ctx, tx)

	err = pager.FromTo(min, to, 50, sub.readRecordsPaged(ctx, boxes))

	// Done writing, now close the inboxes
	for _, box := range boxes {
		close(box)
	}

	wg.Wait()

	if err != nil {
		return false, err
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
	sub.subscriptions[subscriberId] = newStreamHandler(id, subscriberId, sub.store, subscriber)
	sub.ids = append(sub.ids, subscriberId)
}

func (sub *subscription) readRecordsPaged(ctx context.Context, boxes []chan streams.Message) pager.Func {
	return func(from, to, limit int64) (int, error) {
		records, err := sub.store.ReadRecords(ctx, sub.stream, from, to, limit)
		if err != nil {
			return 0, err
		}

		for _, r := range records {
			msg, err := sub.registry.ToMessage(r)
			if err != nil {
				return 0, err
			}
			for _, box := range boxes {
				box <- msg
			}
		}
		return len(records), nil
	}
}

func (sub *subscription) startSubscriptionProcessors(ctx context.Context, tx store.Tx) ([]chan streams.Message, *sync.WaitGroup) {
	var boxes []chan streams.Message
	wg := &sync.WaitGroup{}
	for _, s := range sub.subscriptions {
		inbox := make(chan streams.Message)

		wg.Add(1)
		go func(handler *streamHandler) {
			handler.Process(ctx, tx, inbox)
			wg.Done()
		}(s)

		boxes = append(boxes, inbox)
	}
	return boxes, wg
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

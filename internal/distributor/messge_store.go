package distributor

import (
	"context"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/stream"
)

func newMsgStore(store Store, registry registry, stream stream.Id, subscriberId string) *msgStore {
	return &msgStore{
		store:        store,
		stream:       stream,
		subscriberId: subscriberId,
		registry:     registry,
	}
}

type msgStore struct {
	store        Store
	stream       stream.Id
	subscriberId string
	registry     registry
}

func (facade *msgStore) Begin(ctx context.Context) (store.Tx, error) {
	return facade.store.Begin(ctx)
}

func (facade *msgStore) GetLastPosition(tx store.Tx) (int64, error) {
	return facade.store.GetLastPosition(tx, facade.subscriberId, facade.stream)
}

func (facade *msgStore) SetPosition(tx store.Tx, position int64) error {
	return facade.store.SetPosition(tx, facade.subscriberId, facade.stream, position)
}

func (facade *msgStore) ReadMessages(ctx context.Context, from int64) ([]stream.Message, error) {
	records, err := facade.store.ReadRecords(ctx, facade.stream, from)
	if err != nil {
		return nil, err
	}
	var messages []stream.Message
	for _, r := range records {
		msg, err := facade.registry.ToMessage(r)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

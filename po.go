package po

import (
	"context"
	"github.com/kyuff/po/internal/store"
)

type Store interface {
	ReadRecords(ctx context.Context, streamId string) ([]store.Record, error)
	Begin(ctx context.Context) (store.Tx, error)
	Store(tx store.Tx, record store.Record) error
}

type Notifier interface {
	Notify(ctx context.Context, records ...store.Record) error
	Subscribe(ctx context.Context, subscriptionId, streamId string, subscriber interface{}) error
}

func New(store Store, notifier Notifier) *Po {
	return &Po{
		store:  store,
		notify: notifier,
	}
}

type Po struct {
	store  Store
	notify Notifier
}

func (po *Po) Stream(ctx context.Context, streamId string) *Stream {
	return &Stream{
		ID:     streamId,
		ctx:    ctx,
		store:  po.store,
		notify: po.notify,
	}
}

// convenience method to load a stream and project it
func (po *Po) Project(ctx context.Context, streamId string, projection interface{}) error {
	return po.Stream(ctx, streamId).Project(projection)
}

func (po *Po) Subscribe(ctx context.Context, subscriptionId, streamId string, subscriber interface{}) error {
	return po.notify.Subscribe(ctx, subscriptionId, streamId, subscriber)
}

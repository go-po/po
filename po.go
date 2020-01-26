package po

import (
	"context"
	"github.com/kyuff/po/internal/record"
	"github.com/kyuff/po/internal/registry"
	"github.com/kyuff/po/internal/store"
)

type Store interface {
	ReadRecords(ctx context.Context, streamId string) ([]record.Record, error)
	Begin(ctx context.Context) (store.Tx, error)
	Store(tx store.Tx, record record.Record) error
}

type Broker interface {
	Notify(ctx context.Context, records ...record.Record) error
	Subscribe(ctx context.Context, subscriptionId, streamId string, subscriber interface{}) error
}

type Registry interface {
	LookupType(msg interface{}) string
	Unmarshal(typeName string, b []byte) (interface{}, error)
	Marshal(msg interface{}) ([]byte, error)
}

func New(store Store, broker Broker) *Po {
	return &Po{
		store:  store,
		broker: broker,
	}
}

type Po struct {
	store  Store
	broker Broker
}

func (po *Po) Stream(ctx context.Context, streamId string) *Stream {
	return &Stream{
		ID:       streamId,
		ctx:      ctx,
		store:    po.store,
		broker:   po.broker,
		registry: registry.DefaultRegistry,
	}
}

// convenience method to load a stream and project it
func (po *Po) Project(ctx context.Context, streamId string, projection interface{}) error {
	return po.Stream(ctx, streamId).Project(projection)
}

func (po *Po) Subscribe(ctx context.Context, subscriptionId, streamId string, subscriber interface{}) error {
	return po.broker.Subscribe(ctx, subscriptionId, streamId, subscriber)
}

func RegisterMessages(initializers ...registry.MessageUnmarshaller) {
	registry.Register(initializers...)
}

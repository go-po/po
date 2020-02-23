package po

import (
	"context"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/distributor"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/stream"
)

type Store interface {
	ReadRecords(ctx context.Context, id stream.Id) ([]record.Record, error)
	Begin(ctx context.Context) (store.Tx, error)
	AssignGroupNumber(ctx context.Context, r record.Record) (int64, error)
	StoreRecord(tx store.Tx, id stream.Id, contentType string, data []byte) (record.Record, error)
}

type Broker interface {
	Notify(ctx context.Context, records ...record.Record) error
	Subscribe(ctx context.Context, streamId stream.Id) error
	Distributor(distributor broker.Distributor)
}

type Registry interface {
	Unmarshal(typeName string, b []byte) (interface{}, error)
	Marshal(msg interface{}) ([]byte, string, error)
	ToMessage(r record.Record) (stream.Message, error)
}

type Distributor interface {
	broker.Distributor
	Register(ctx context.Context, subscriberId string, streamId stream.Id, subscriber interface{}) error
}

func New(store Store, broker Broker) *Po {
	dist := distributor.New(store, registry.DefaultRegistry)
	broker.Distributor(dist)
	return &Po{
		store:       store,
		broker:      broker,
		distributor: dist,
	}
}

type Po struct {
	store       Store
	broker      Broker
	distributor Distributor
}

func (po *Po) Stream(ctx context.Context, streamId string) *Stream {
	return &Stream{
		ID:       stream.ParseId(streamId),
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
	id := stream.ParseId(streamId)
	err := po.distributor.Register(ctx, subscriptionId, id, subscriber)
	if err != nil {
		return err
	}
	return po.broker.Subscribe(ctx, id)
}

func RegisterMessages(initializers ...registry.MessageUnmarshaller) {
	registry.Register(initializers...)
}

package po

import (
	"context"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/stream"
)

type Store interface {
	ReadRecords(ctx context.Context, id stream.Id, from int64) ([]record.Record, error)
	AssignGroup(ctx context.Context, id stream.Id, number int64) (record.Record, error)

	Begin(ctx context.Context) (store.Tx, error)
	StoreRecord(tx store.Tx, id stream.Id, number int64, contentType string, data []byte) (record.Record, error)

	GetSubscriberPosition(tx store.Tx, subscriberId string, id stream.Id) (int64, error)
	SetSubscriberPosition(tx store.Tx, subscriberId string, stream stream.Id, position int64) error
	GetStreamPosition(ctx context.Context, id stream.Id) (int64, error)
}

type Broker interface {
	Notify(ctx context.Context, records ...record.Record) error
	Register(ctx context.Context, subscriberId string, streamId stream.Id, subscriber interface{}) error
}

type Registry interface {
	Unmarshal(typeName string, b []byte) (interface{}, error)
	Marshal(msg interface{}) ([]byte, string, error)
	ToMessage(r record.Record) (stream.Message, error)
}

type Distributor interface {
	broker.Distributor
}

type Po struct {
	store    Store
	broker   Broker
	registry Registry
}

func (po *Po) Stream(ctx context.Context, streamId string) *Stream {
	return &Stream{
		ID:       stream.ParseId(streamId),
		ctx:      ctx,
		store:    po.store,
		broker:   po.broker,
		registry: po.registry,
	}
}

// convenience method to load a stream and project it
func (po *Po) Project(ctx context.Context, streamId string, projection interface{}) error {
	return po.Stream(ctx, streamId).Project(projection)
}

func (po *Po) Subscribe(ctx context.Context, subscriptionId, streamId string, subscriber interface{}) error {
	id := stream.ParseId(streamId)
	return po.broker.Register(ctx, subscriptionId, id, subscriber)
}

func RegisterMessages(initializers ...registry.MessageUnmarshaller) {
	registry.Register(initializers...)
}

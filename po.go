package po

import (
	"context"

	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
)

type Store interface {
	ReadRecords(ctx context.Context, id streams.Id, from int64) ([]record.Record, error)
	AssignGroup(ctx context.Context, id streams.Id, number int64) (record.Record, error)

	Begin(ctx context.Context) (store.Tx, error)
	StoreRecord(tx store.Tx, id streams.Id, number int64, contentType string, data []byte) (record.Record, error)

	ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error)
	UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error

	GetSubscriberPosition(tx store.Tx, subscriberId string, id streams.Id) (int64, error)
	SetSubscriberPosition(tx store.Tx, subscriberId string, stream streams.Id, position int64) error
	GetStreamPosition(ctx context.Context, id streams.Id) (int64, error)
}

type Broker interface {
	Notify(ctx context.Context, records ...record.Record) error
	Register(ctx context.Context, subscriberId string, streamId streams.Id, subscriber interface{}) error
}

type Registry interface {
	Unmarshal(typeName string, b []byte) (interface{}, error)
	Marshal(msg interface{}) ([]byte, string, error)
	ToMessage(r record.Record) (streams.Message, error)
}

type Logger interface {
	Debugf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errf(err error, template string, args ...interface{})
}

type Distributor interface {
	broker.Distributor
}

type Po struct {
	logger   Logger
	store    Store
	broker   Broker
	registry Registry
}

func (po *Po) Stream(ctx context.Context, id streams.Id) *Stream {
	return &Stream{
		logger:   po.logger,
		ID:       id,
		ctx:      ctx,
		registry: po.registry,
		broker:   po.broker,
		store:    po.store,
		position: -1,
	}
}

// convenience method to load a stream and project it
func (po *Po) Project(ctx context.Context, id streams.Id, projection streams.Handler) error {
	return po.Stream(ctx, id).Project(projection)
}

func (po *Po) Subscribe(ctx context.Context, subscriptionId string, id streams.Id, subscriber interface{}) error {
	return po.broker.Register(ctx, subscriptionId, id, subscriber)
}

func (po *Po) Execute(ctx context.Context, id streams.Id, exec Executor) error {
	stream := po.Stream(ctx, id)
	return stream.Execute(exec)
}

func (po *Po) Append(ctx context.Context, id streams.Id, messages ...interface{}) error {
	stream := po.Stream(ctx, id)
	_, err := stream.Append(messages...)
	return err
}

func RegisterMessages(initializers ...registry.MessageUnmarshaller) {
	registry.Register(initializers...)
}

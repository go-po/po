package po

import (
	"context"

	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/internal/observer/nullary"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
)

type Store interface {
	WriteRecords(ctx context.Context, id streams.Id, data ...record.Data) ([]record.Record, error)
	WriteRecordsFrom(ctx context.Context, id streams.Id, position int64, data ...record.Data) ([]record.Record, error)
	ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error)
	UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error
	Begin(ctx context.Context) (store.Tx, error)
	SubscriptionPositionLock(tx store.Tx, id streams.Id, subscriptionIds ...string) ([]store.SubscriptionPosition, error)
	ReadRecords(ctx context.Context, id streams.Id, from, to, limit int64) ([]record.Record, error)
	SetSubscriptionPosition(tx store.Tx, id streams.Id, position store.SubscriptionPosition) error
}

type Broker interface {
	Notify(ctx context.Context, records ...record.Record) error
	Register(ctx context.Context, subscriberId string, streamId streams.Id, subscriber streams.Handler) error
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

// Implemented by commands.
// Contract is that the CommandHandler is hydrated with all
// messages on the stream is applied to. Thereafter the Execute
// method is called, allowing the CommandHandler to append
// messages within a transaction.
// Returning an error will cause a rollback.
// Otherwise all are discarded.
type CommandHandler interface {
	// Hydrates a stream
	Handle(ctx context.Context, msg streams.Message) error
	// Applies the command
	Execute(appender TransactionAppender) error
}

// Append to a transaction.
//Messages will be written to the store on commit
type TransactionAppender interface {
	// appends tot he stream
	Append(messages ...interface{})
	// current size of the stream
	Size() int64
}

type Handler interface {
	// Receives messages from a stream
	Handle(ctx context.Context, msg streams.Message) error
}

// Utility for functional style handlers
type HandlerFunc func(ctx context.Context, msg streams.Message) error

func (fn HandlerFunc) Handle(ctx context.Context, msg streams.Message) error {
	return fn(ctx, msg)
}

type poObserver struct {
	Stream  nullary.ClientTrace
	Project nullary.ClientTrace
}

type messageStream interface {
	Project(projection Handler) error
	Execute(exec CommandHandler) error
	Append(messages ...interface{}) (int64, error)
}

type Po struct {
	obs      poObserver
	builder  *observer.Builder
	logger   Logger
	store    Store
	broker   Broker
	registry Registry
}

func (po *Po) Stream(ctx context.Context, id streams.Id) *Stream {
	done := po.obs.Stream.Observe(ctx)
	defer done()
	return NewStream(ctx, id, po.store, po.broker, po.registry)
}

// convenience method to load a stream and project it
func (po *Po) Project(ctx context.Context, id streams.Id, projection Handler) error {
	done := po.obs.Project.Observe(ctx)
	defer done()
	return po.Stream(ctx, id).Project(projection)
}

func (po *Po) Subscribe(ctx context.Context, subscriptionId string, id streams.Id, subscriber Handler) error {
	return po.broker.Register(ctx, subscriptionId, id, subscriber)
}

func (po *Po) Execute(ctx context.Context, id streams.Id, exec CommandHandler) error {
	stream := po.Stream(ctx, id)
	return stream.Execute(exec)
}

func (po *Po) Append(ctx context.Context, id streams.Id, messages ...interface{}) (int64, error) {
	return po.Stream(ctx, id).Append(messages...)
}

func RegisterMessages(initializers ...registry.MessageUnmarshaller) {
	registry.Register(initializers...)
}

package po

import (
	"context"
	"sync"

	"github.com/go-po/po/streams"
)

var _ messageStream = &OptimisticLockingStream{}

type optimisticStore interface {
	appenderStore
	projectorStore
	snapshotStore
}

func NewOptimisticLockingStream(ctx context.Context, streamId streams.Id, store optimisticStore, broker Broker, registry Registry) *OptimisticLockingStream {
	projector := newProjectorFunc(store, registry)
	snapshotter := newSnapshots(store, projector)
	appender := newAppenderFunc(store, broker, registry)
	executioner := newRetryExecutor(3, newExecutor(projector, appender))
	return &OptimisticLockingStream{
		Id:  streamId,
		ctx: ctx,

		projector: snapshotter,
		appender:  appender,
		executor:  executioner,

		mu:           sync.RWMutex{},
		lockPosition: -1,
	}
}

// Stream that uses Optimistic locking when
// appending to the message stream
type OptimisticLockingStream struct {
	Id        streams.Id
	ctx       context.Context // to use for the operation
	projector projector
	executor  executorFunc
	appender  appenderFunc

	mu           sync.RWMutex // protects the fields below
	lockPosition int64        // last known position
}

func (stream *OptimisticLockingStream) Append(messages ...interface{}) (int64, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	var err error
	for _, msg := range messages {
		stream.lockPosition, err = stream.appender(stream.ctx, stream.Id, stream.lockPosition, msg)
		if err != nil {
			return stream.lockPosition, err
		}
	}
	return stream.lockPosition, nil
}

// Projects all messages onto the given Handler.
// If the handler implements streams.NamedSnapshot, snapshotting will be performed.
//
// The projection will also lock this Stream instance to the most recently read
// message number for the stream.
func (stream *OptimisticLockingStream) Project(projection Handler) error {
	// TODO add observability
	stream.mu.Lock()
	defer stream.mu.Unlock()

	position, err := stream.projector.Project(stream.ctx, stream.Id, stream.lockPosition, projection)
	if err != nil {
		return err
	}
	stream.lockPosition = position
	return nil
}

func (stream *OptimisticLockingStream) Execute(exec CommandHandler) error {
	// TODO add observability
	stream.mu.Lock()
	defer stream.mu.Unlock()

	position, err := stream.executor(stream.ctx, stream.Id, stream.lockPosition, exec)
	stream.lockPosition = position
	return err
}

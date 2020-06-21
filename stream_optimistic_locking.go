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

type executorFunc func(ctx context.Context, id streams.Id, lockPosition int64, cmd CommandHandler) (int64, error)

func NewOptimisticLockingStream(ctx context.Context, streamId streams.Id, store optimisticStore, registry Registry) *OptimisticLockingStream {
	projector := newProjectorFunc(store, registry)
	snapshotter := newSnapshots(store, projector)
	appender := newAppenderFunc(store, registry)
	return &OptimisticLockingStream{
		Id:           streamId,
		ctx:          ctx,
		store:        store,
		registry:     registry,
		projector:    snapshotter,
		appender:     appender,
		executor:     newExecutorFunc(projector, appender, 3),
		mu:           sync.RWMutex{},
		lockPosition: -1,
	}
}

// Stream that uses Optimistic locking when
// appending to the message stream
type OptimisticLockingStream struct {
	Id        streams.Id
	ctx       context.Context // to use for the operation
	store     optimisticStore
	registry  Registry
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

func newExecutorFunc(projector projectorFunc, appender appenderFunc, retryCount int) executorFunc {
	return func(ctx context.Context, id streams.Id, lockPosition int64, cmd CommandHandler) (int64, error) {
		position, err := projector(ctx, id, lockPosition, cmd)
		if err != nil {
			return lockPosition, err
		}
		tx := &optimisticAppender{position: position}
		err = cmd.Execute(tx)
		if err != nil {
			return position, err
		}
		position, err = appender(ctx, id, position, tx.messages)
		if err != nil {
			return position, err
		}
		return -1, nil
	}
}

type optimisticAppender struct {
	messages []interface{}
	position int64
}

func (appender *optimisticAppender) Append(messages ...interface{}) {
	appender.messages = append(appender.messages, messages...)
}

func (appender *optimisticAppender) Size() int64 {
	return appender.position
}

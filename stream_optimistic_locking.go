package po

import (
	"context"
	"sync"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
)

var _ messageStream = &OptimisticLockingStream{}

type optimisticStore interface {
	WriteRecord(ctx context.Context, id streams.Id, contentType string, b []byte) (int64, error)
	WriteRecordAt(ctx context.Context, id streams.Id, contentType string, b []byte, position int64) error

	ReadRecords(ctx context.Context, id streams.Id, from int64) ([]record.Record, error)
	snapshotStore
}

type appenderFunc func(ctx context.Context, id streams.Id, position int64, message interface{}) (int64, error)
type projectorFunc func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error)

func NewOptimisticLockingStream(ctx context.Context, streamId streams.Id, store optimisticStore, registry Registry) *OptimisticLockingStream {
	return &OptimisticLockingStream{
		Id:           streamId,
		ctx:          ctx,
		store:        store,
		registry:     registry,
		projector:    newProjectorFunc(store, newSnapshots(store), registry),
		appender:     newAppenderFunc(store, registry),
		mu:           sync.RWMutex{},
		lockPosition: -1,
	}
}

type Snapshots interface {
	Snapshot(ctx context.Context, id streams.Id, projection Handler) (snapshotCommit, int64, error)
}

// Stream that uses Optimistic locking when
// appending to the message stream
type OptimisticLockingStream struct {
	Id        streams.Id
	ctx       context.Context // to use for the operation
	store     optimisticStore
	registry  Registry
	projector projectorFunc
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

	position, err := stream.projector(stream.ctx, stream.Id, stream.lockPosition, projection)
	if err != nil {
		return err
	}
	stream.lockPosition = position
	return nil
}

func (stream *OptimisticLockingStream) Execute(exec CommandHandler) error {
	panic("implement me")
}

func newProjectorFunc(store optimisticStore, snap Snapshots, registry Registry) projectorFunc {
	return func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
		snapCommit, snapPosition, err := snap.Snapshot(ctx, id, projection)
		if err != nil {
			return -1, err
		}
		if snapPosition > -1 {
			lockPosition = snapPosition
		}

		records, err := store.ReadRecords(ctx, id, lockPosition)
		if err != nil {
			return -1, err
		}

		if len(records) == 0 {
			// nothing new, bail out
			return lockPosition, nil
		}

		var messages []streams.Message
		for _, r := range records {
			message, err := registry.ToMessage(r)
			if err != nil {
				return -1, err
			}
			messages = append(messages, message)
		}

		for _, message := range messages {
			err = projection.Handle(ctx, message)
			if err != nil {
				return -1, err
			}
		}

		// store the position as the stream object is now considered active
		// This to guarantee users of the projection that messages appended
		// afterwards will be in the order their projection was made.
		if len(messages) == 0 {
			return lockPosition, nil
		}
		message := messages[len(messages)-1]
		if id.HasEntity() {
			lockPosition = message.Number
		} else {
			lockPosition = message.GroupNumber
		}

		err = snapCommit(lockPosition)
		if err != nil {
			return lockPosition, err
		}

		return lockPosition, nil
	}
}

func newAppenderFunc(store optimisticStore, registry Registry) appenderFunc {
	return func(ctx context.Context, id streams.Id, position int64, message interface{}) (int64, error) {
		b, contentType, err := registry.Marshal(message)
		if err != nil {
			return -1, err
		}

		if position < 0 {
			// this append have not seen the lockPosition yet,
			// so have to get it from the store when performing the first write
			return store.WriteRecord(ctx, id, contentType, b)
		}

		err = store.WriteRecordAt(ctx, id, contentType, b, position+1)
		if err != nil {
			return -1, err
		}
		return position + 1, nil
	}
}

package po

import (
	"context"
	"sync"

	"github.com/go-po/po/streams"
)

var _ messageStream = &OptimisticLockingStream{}

type optimisticStore interface {
	WriteRecord(ctx context.Context, id streams.Id, contentType string, b []byte) (int64, error)
	WriteRecordAt(ctx context.Context, id streams.Id, contentType string, b []byte, position int64) error
}

func NewOptimisticLockingStream(ctx context.Context, streamId streams.Id, store optimisticStore, registry Registry) *OptimisticLockingStream {
	return &OptimisticLockingStream{
		Id:           streamId,
		ctx:          ctx,
		store:        store,
		registry:     registry,
		mu:           sync.RWMutex{},
		lockPosition: -1,
	}
}

// Stream that uses Optimistic locking when
// appending to the message stream
type OptimisticLockingStream struct {
	Id       streams.Id
	ctx      context.Context // to use for the operation
	store    optimisticStore
	registry Registry

	mu           sync.RWMutex // protects the fields below
	lockPosition int64        // last known position
}

func (stream *OptimisticLockingStream) Append(messages ...interface{}) (int64, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	var err error
	for _, msg := range messages {
		stream.lockPosition, err = stream.append(stream.lockPosition, msg)
		if err != nil {
			return stream.lockPosition, err
		}
	}
	return stream.lockPosition, nil
}

func (stream *OptimisticLockingStream) append(position int64, message interface{}) (int64, error) {
	b, contentType, err := stream.registry.Marshal(message)
	if err != nil {
		return -1, err
	}

	if position < 0 {
		// this append have not seen the lockPosition yet,
		// so have to get it from the store when performing the first write
		return stream.store.WriteRecord(stream.ctx, stream.Id, contentType, b)
	}

	err = stream.store.WriteRecordAt(stream.ctx, stream.Id, contentType, b, position+1)
	if err != nil {
		return -1, err
	}
	return position + 1, nil
}

func (stream *OptimisticLockingStream) Project(projection Handler) error {
	panic("implement me")
}

func (stream *OptimisticLockingStream) Execute(exec CommandHandler) error {
	panic("implement me")
}

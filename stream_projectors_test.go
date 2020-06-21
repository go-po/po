package po

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
	"github.com/stretchr/testify/assert"
)

func TestOptimisticLockingStream_Project(t *testing.T) {
	ctx := context.Background()
	streamId := streams.ParseId("teststream-1")

	// setup
	newTestFixture := func(messageCount int64) projector {
		store := &stubAppenderStore{messageCount: 0}
		appender := newAppenderFunc(store, testRegistry)
		var i int64
		for i = 0; i < messageCount; i++ {
			_, err := appender(ctx, streamId, i, Msg{Name: fmt.Sprintf("Message %d", i)})
			assert.NoError(t, err)
		}
		return newProjectorFunc(store, testRegistry)
	}

	t.Run("empty stream", func(t *testing.T) {
		// setup
		sut := newTestFixture(0)
		count := 0
		// execute
		pos, err := sut.Project(ctx, streamId, -1, HandlerFunc(func(ctx context.Context, msg streams.Message) error {
			count = count + 1
			return nil
		}))
		// verify
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, -1, int(pos))
	})

	t.Run("with data", func(t *testing.T) {
		// setup
		sut := newTestFixture(10)
		count := 0
		// execute
		pos, err := sut.Project(ctx, streamId, -1, HandlerFunc(func(ctx context.Context, msg streams.Message) error {
			count = count + 1
			return nil
		}))
		// verify
		assert.NoError(t, err)
		assert.Equal(t, 10, count, "messages received")
		assert.Equal(t, 10, int(pos))
	})

}

func newProjectionHandler(handler HandlerFunc) *stubProjectionHandler {
	return &stubProjectionHandler{
		handler: handler,
	}
}

type stubProjectionHandler struct {
	handler Handler
}

func (stub *stubProjectionHandler) SnapshotName() string {
	return "stub-snapshot"
}

func (stub *stubProjectionHandler) Handle(ctx context.Context, msg streams.Message) error {
	return stub.handler.Handle(ctx, msg)
}

var _ snapshotStore = &stubSnapshotStore{}

type stubSnapshotStore struct {
	reads    int
	writes   int
	snapshot record.Snapshot
}

func (stub *stubSnapshotStore) ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error) {
	stub.reads = stub.reads + 1
	return stub.snapshot, nil
}

func (stub *stubSnapshotStore) UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error {
	stub.writes = stub.writes + 1
	stub.snapshot = snapshot
	return nil
}

func TestOptimisticLockingStream_Project_Snapshot(t *testing.T) {
	// setup
	ctx := context.Background()
	streamId := streams.ParseId("snapshot-1")

	newTestFixture := func(snapshotPosition int64, inner projector) (projector, *stubSnapshotStore) {
		store := &stubSnapshotStore{snapshot: record.Snapshot{
			Data:        []byte(`{}`),
			Position:    snapshotPosition,
			ContentType: "application/json",
		}}

		return newSnapshots(store, inner), store
	}

	t.Run("empty", func(t *testing.T) {
		// setup
		inner := projectorFunc(func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
			return -1, nil
		})
		sut, _ := newTestFixture(0, inner)

		// execute
		pos, err := sut.Project(ctx, streamId, -1, newProjectionHandler(func(ctx context.Context, msg streams.Message) error {
			return nil
		}))

		// verify
		assert.NoError(t, err)
		assert.Equal(t, -1, int(pos))
	})

	t.Run("inner progress", func(t *testing.T) {
		// setup
		inner := projectorFunc(func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
			return 10, nil
		})
		sut, store := newTestFixture(0, inner)

		// execute
		pos, err := sut.Project(ctx, streamId, -1, newProjectionHandler(func(ctx context.Context, msg streams.Message) error {
			return nil
		}))

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 10, int(pos))
		assert.Equal(t, 10, int(store.snapshot.Position))
		assert.Equal(t, 1, store.reads)
		assert.Equal(t, 1, store.writes)
	})

	t.Run("halfway projected", func(t *testing.T) {
		// setup
		var gotLockPosition int64 = -10
		inner := projectorFunc(func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
			gotLockPosition = lockPosition
			return 10, nil
		})
		sut, store := newTestFixture(4, inner)

		// execute
		pos, err := sut.Project(ctx, streamId, -1, newProjectionHandler(func(ctx context.Context, msg streams.Message) error {
			return nil
		}))

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 4, int(gotLockPosition))
		assert.Equal(t, 10, int(pos))
		assert.Equal(t, 10, int(store.snapshot.Position))
	})

	t.Run("not projector", func(t *testing.T) {
		// setup
		inner := projectorFunc(func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
			return 10, nil
		})
		sut, store := newTestFixture(0, inner)
		count := 0

		// execute
		_, err := sut.Project(ctx, streamId, -1, HandlerFunc(func(ctx context.Context, msg streams.Message) error {
			count = count + 1
			return nil
		}))

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 0, store.reads)
		assert.Equal(t, 0, store.writes)
	})
}

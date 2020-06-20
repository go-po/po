package po

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
	"github.com/stretchr/testify/assert"
)

type Msg struct {
	Name string
}

var testRegistry = registry.New()

func init() {
	testRegistry.Register(func(b []byte) (interface{}, error) {
		msg := Msg{}
		err := json.Unmarshal(b, &msg)
		return msg, err
	})
}

var _ optimisticStore = &stubStore{}

type stubStore struct {
	messageCount int64
	records      []record.Record
	snapshot     record.Snapshot
}

func (stub *stubStore) ReadRecords(ctx context.Context, id streams.Id, from int64) ([]record.Record, error) {
	if from < 0 {
		return stub.records, nil
	}
	return stub.records[from:], nil
}

func (stub *stubStore) ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error) {
	return stub.snapshot, nil
}

func (stub *stubStore) UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error {
	stub.snapshot = snapshot
	return nil
}

func (stub *stubStore) incCount() {
	stub.messageCount = stub.messageCount + 1
}

func (stub *stubStore) WriteRecord(ctx context.Context, id streams.Id, contentType string, b []byte) (int64, error) {
	stub.incCount()
	stub.records = append(stub.records, record.Record{
		Stream:      id,
		Number:      stub.messageCount,
		GroupNumber: stub.messageCount,
		Data:        b,
		Group:       id.Group,
		ContentType: contentType,
		Time:        time.Now(),
	})
	return stub.messageCount, nil
}

func (stub *stubStore) WriteRecordAt(ctx context.Context, id streams.Id, contentType string, b []byte, position int64) error {
	if position != (stub.messageCount + 1) {
		return store.WriteConflictError{
			StreamId: id,
			Position: position,
		}
	}
	stub.incCount()
	stub.records = append(stub.records, record.Record{
		Stream:      id,
		Number:      stub.messageCount,
		GroupNumber: stub.messageCount,
		Data:        b,
		Group:       id.Group,
		ContentType: contentType,
		Time:        time.Now(),
	})
	return nil
}

type stubSnapshotter struct {
	position int64
}

func (stub *stubSnapshotter) Snapshot(ctx context.Context, id streams.Id, projection Handler) (snapshotCommit, int64, error) {
	return func(position int64) error {
		stub.position = position
		return nil
	}, stub.position, nil
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

func TestOptimisticLockingStream_Project(t *testing.T) {
	ctx := context.Background()
	streamId := streams.ParseId("teststream-1")

	// setup
	newTestFixture := func(messageCount int64, snap *stubSnapshotter) projectorFunc {
		if snap == nil {
			snap = &stubSnapshotter{}
		}
		store := &stubStore{messageCount: 0}
		appender := newAppenderFunc(store, testRegistry)
		var i int64
		for i = 0; i < messageCount; i++ {
			_, err := appender(ctx, streamId, i, Msg{Name: fmt.Sprintf("Message %d", i)})
			assert.NoError(t, err)
		}
		return newProjectorFunc(store, snap, testRegistry)
	}

	t.Run("empty stream", func(t *testing.T) {
		// setup
		sut := newTestFixture(0, nil)
		count := 0
		// execute
		pos, err := sut(ctx, streamId, -1, HandlerFunc(func(ctx context.Context, msg streams.Message) error {
			count = count + 1
			return nil
		}))
		// verify
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, 0, int(pos))
	})

	t.Run("with data", func(t *testing.T) {
		// setup
		sut := newTestFixture(10, nil)
		count := 0
		// execute
		pos, err := sut(ctx, streamId, -1, HandlerFunc(func(ctx context.Context, msg streams.Message) error {
			count = count + 1
			return nil
		}))
		// verify
		assert.NoError(t, err)
		assert.Equal(t, 10, count, "messages received")
		assert.Equal(t, 10, int(pos))
	})

	t.Run("existing", func(t *testing.T) {
		// setup
		snap := &stubSnapshotter{}
		sut := newTestFixture(10, snap)
		count := 0
		// execute
		pos, err := sut(ctx, streamId, -1, HandlerFunc(func(ctx context.Context, msg streams.Message) error {
			count = count + 1
			return nil
		}))
		// verify
		assert.NoError(t, err)
		assert.Equal(t, 10, count, "messages received")
		assert.Equal(t, 10, int(pos))
		assert.Equal(t, 10, int(snap.position))
	})

	t.Run("existing with data", func(t *testing.T) {
		// setup
		snap := &stubSnapshotter{position: 4}
		sut := newTestFixture(10, snap)
		count := 0
		// execute
		pos, err := sut(ctx, streamId, -1, HandlerFunc(func(ctx context.Context, msg streams.Message) error {
			count = count + 1
			return nil
		}))
		// verify
		assert.NoError(t, err)
		assert.Equal(t, 6, count, "messages received")
		assert.Equal(t, 10, int(pos))
		assert.Equal(t, 10, int(snap.position))
	})
}

func TestOptimisticLockingStream_Append(t *testing.T) {
	// setup
	newTestFixture := func(lockPosition int64, messageCount int64) (*OptimisticLockingStream, *stubStore) {
		store := &stubStore{messageCount: messageCount}
		stream := NewOptimisticLockingStream(
			context.Background(),
			streams.ParseId("teststream-1"),
			store,
			testRegistry,
		)
		stream.lockPosition = lockPosition
		return stream, store
	}
	type verify func(t *testing.T, n int64, err error, store *stubStore)
	verifyAll := func(t *testing.T, n int64, err error, store *stubStore, assertions ...verify) {
		t.Helper()
		for _, v := range assertions {
			v(t, n, err, store)
		}
	}

	noErr := func() verify {
		return func(t *testing.T, n int64, err error, store *stubStore) {
			assert.NoError(t, err)
		}
	}

	errWriteConflict := func() verify {
		return func(t *testing.T, n int64, err error, s *stubStore) {
			assert.IsType(t, store.WriteConflictError{}, err, "write conflict error")
		}
	}
	num := func(expect int64) verify {
		return func(t *testing.T, n int64, err error, store *stubStore) {
			assert.Equal(t, expect, n, "number")
		}
	}

	t.Run("fresh stream", func(t *testing.T) {
		// setup
		stream, store := newTestFixture(-1, 0)
		// execute
		n, err := stream.Append(Msg{Name: "Append Test"})
		// verify
		verifyAll(t, n, err, store, noErr(), num(1))
	})

	t.Run("existing stream", func(t *testing.T) {
		// setup
		stream, store := newTestFixture(-1, 3)
		// execute
		n, err := stream.Append(Msg{Name: "Append Test"})
		// verify
		verifyAll(t, n, err, store, noErr(), num(4))
	})

	t.Run("locked stream", func(t *testing.T) {
		// setup
		stream, store := newTestFixture(3, 3)
		// execute
		n, err := stream.Append(Msg{Name: "Append Test"})
		// verify
		verifyAll(t, n, err, store, noErr(), num(4))
	})

	t.Run("locked stream with conflict", func(t *testing.T) {
		// setup
		stream, store := newTestFixture(3, 5)
		// execute
		n, err := stream.Append(Msg{Name: "Append Test"})
		// verify
		verifyAll(t, n, err, store, errWriteConflict())
	})

}

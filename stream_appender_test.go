package po

import (
	"context"
	"testing"
	"time"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
	"github.com/stretchr/testify/assert"
)

var _ appenderStore = &stubAppenderStore{}

type stubAppenderStore struct {
	messageCount int64
	records      []record.Record
	snapshot     record.Snapshot
}

func (stub *stubAppenderStore) ReadRecords(ctx context.Context, id streams.Id, from, to, limit int64) ([]record.Record, error) {
	if from < 0 {
		return stub.records, nil
	}
	data := stub.records[from:to]
	if int64(len(data)) > limit {
		return data[:limit], nil
	}
	return data, nil
}

func (stub *stubAppenderStore) ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error) {
	return stub.snapshot, nil
}

func (stub *stubAppenderStore) UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error {
	stub.snapshot = snapshot
	return nil
}

func (stub *stubAppenderStore) incCount() {
	stub.messageCount = stub.messageCount + 1
}

type stubNotifier struct {
}

func (stubNotifier) Notify(ctx context.Context, records ...record.Record) error {
	return nil
}

func (stub *stubAppenderStore) WriteRecords(ctx context.Context, id streams.Id, datas ...record.Data) ([]record.Record, error) {
	var written []record.Record
	for _, data := range datas {
		stub.incCount()
		written = append(written, record.Record{
			Stream:       id,
			Number:       stub.messageCount,
			GlobalNumber: stub.messageCount,
			Data:         data.Data,
			Group:        id.Group,
			ContentType:  data.ContentType,
			Time:         time.Now(),
		})
	}
	stub.records = append(stub.records, written...)

	return stub.records, nil
}

func (stub *stubAppenderStore) WriteRecordsFrom(ctx context.Context, id streams.Id, position int64, datas ...record.Data) ([]record.Record, error) {
	if position != stub.messageCount {
		return nil, store.WriteConflictError{
			StreamId: id,
			Position: position,
		}
	}
	return stub.WriteRecords(ctx, id, datas...)
}

func TestOptimisticLockingStream_Append(t *testing.T) {
	// setup
	ctx := context.Background()
	streamId := streams.ParseId("teststream-1")
	type verify func(t *testing.T, n int64, err error, store *stubAppenderStore)
	verifyAll := func(t *testing.T, n int64, err error, store *stubAppenderStore, assertions ...verify) {
		t.Helper()
		for _, v := range assertions {
			v(t, n, err, store)
		}
	}

	noErr := func() verify {
		return func(t *testing.T, n int64, err error, store *stubAppenderStore) {
			assert.NoError(t, err)
		}
	}

	errWriteConflict := func() verify {
		return func(t *testing.T, n int64, err error, s *stubAppenderStore) {
			assert.IsType(t, store.WriteConflictError{}, err, "write conflict error")
		}
	}
	num := func(expect int64) verify {
		return func(t *testing.T, n int64, err error, store *stubAppenderStore) {
			assert.Equal(t, expect, n, "number")
		}
	}

	t.Run("fresh stream", func(t *testing.T) {
		// setup
		store := &stubAppenderStore{messageCount: 0}
		sut := newAppenderFunc(store, stubNotifier{}, testRegistry)
		// execute
		n, err := sut(ctx, streamId, -1, Msg{Name: "Append Test"})
		// verify
		verifyAll(t, n, err, store, noErr(), num(1))
	})

	t.Run("existing stream", func(t *testing.T) {
		// setup
		store := &stubAppenderStore{messageCount: 3}
		sut := newAppenderFunc(store, stubNotifier{}, testRegistry)
		// execute
		n, err := sut(ctx, streamId, -1, Msg{Name: "Append Test"})
		// verify
		verifyAll(t, n, err, store, noErr(), num(4))
	})

	t.Run("locked stream", func(t *testing.T) {
		// setup
		store := &stubAppenderStore{messageCount: 3}
		sut := newAppenderFunc(store, stubNotifier{}, testRegistry)
		// execute
		n, err := sut(ctx, streamId, 3, Msg{Name: "Append Test"})
		// verify
		verifyAll(t, n, err, store, noErr(), num(4))
	})

	t.Run("locked stream with conflict", func(t *testing.T) {
		// setup
		store := &stubAppenderStore{messageCount: 5}
		sut := newAppenderFunc(store, stubNotifier{}, testRegistry)
		// execute
		n, err := sut(ctx, streamId, 3, Msg{Name: "Append Test"})
		// verify
		verifyAll(t, n, err, store, errWriteConflict())
	})

}

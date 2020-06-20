package po

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
	"github.com/stretchr/testify/assert"
)

var _ optimisticStore = &stubStore{}

type stubStore struct {
	messageCount int64
}

func (stub *stubStore) WriteRecord(ctx context.Context, id streams.Id, contentType string, b []byte) (int64, error) {
	stub.messageCount = stub.messageCount + 1
	return stub.messageCount, nil
}

func (stub *stubStore) WriteRecordAt(ctx context.Context, id streams.Id, contentType string, b []byte, position int64) error {
	if position != (stub.messageCount + 1) {
		return store.WriteConflictError{
			StreamId: id,
			Position: position,
		}
	}
	stub.messageCount = stub.messageCount + 1
	return nil
}

func TestOptimisticLockingStream_Append(t *testing.T) {
	// setup
	type Msg struct {
		Name string
	}
	testRegistry := registry.New()
	testRegistry.Register(func(b []byte) (interface{}, error) {
		msg := Msg{}
		err := json.Unmarshal(b, &msg)
		return msg, err
	})
	newEntityStream := func(lockPosition int64, messageCount int64) (*OptimisticLockingStream, *stubStore) {
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
		stream, store := newEntityStream(-1, 0)
		// execute
		n, err := stream.Append(Msg{Name: "Append Test"})
		// verify
		verifyAll(t, n, err, store, noErr(), num(1))
	})

	t.Run("existing stream", func(t *testing.T) {
		// setup
		stream, store := newEntityStream(-1, 3)
		// execute
		n, err := stream.Append(Msg{Name: "Append Test"})
		// verify
		verifyAll(t, n, err, store, noErr(), num(4))
	})

	t.Run("locked stream", func(t *testing.T) {
		// setup
		stream, store := newEntityStream(3, 3)
		// execute
		n, err := stream.Append(Msg{Name: "Append Test"})
		// verify
		verifyAll(t, n, err, store, noErr(), num(4))
	})

	t.Run("locked stream with conflict", func(t *testing.T) {
		// setup
		stream, store := newEntityStream(3, 5)
		// execute
		n, err := stream.Append(Msg{Name: "Append Test"})
		// verify
		verifyAll(t, n, err, store, errWriteConflict())
	})

}

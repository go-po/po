package broker

import (
	"context"
	"testing"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
	"github.com/stretchr/testify/assert"
)

type mockUpdatePositionStore struct {
	Store
	positions []store.SubscriptionPosition
	records   []record.Record
}

func (mock *mockUpdatePositionStore) SubscriptionPositionLock(tx store.Tx, id streams.Id, subscriptionIds ...string) ([]store.SubscriptionPosition, error) {
	return mock.positions, nil
}

func (mock *mockUpdatePositionStore) ReadRecords(ctx context.Context, id streams.Id, from, to int64) ([]record.Record, error) {
	return mock.records, nil
}

type mockBrokerTx struct {
	commit   bool
	rollback bool
}

func (mock *mockBrokerTx) Commit() error {
	mock.commit = true
	return nil
}

func (mock *mockBrokerTx) Rollback() error {
	mock.rollback = true
	return nil
}

func TestUpdatePosition(t *testing.T) {
	tx := &mockBrokerTx{}
	id := streams.ParseId("updateposition")

	mockStreamHandler := func(p int64) *streamHandler {
		return &streamHandler{
			handler: streams.HandlerFunc(func(ctx context.Context, msg streams.Message) error {
				return nil
			}),
			stream:   id,
			position: p,
		}
	}

	t.Run("one match", func(t *testing.T) {
		// setup
		store := &mockUpdatePositionStore{positions: []store.SubscriptionPosition{
			{
				SubscriptionId: "a",
				Position:       5,
			},
		}}
		data := map[string]*streamHandler{
			"a": mockStreamHandler(0),
		}
		least, err := updatePosition(store, tx, id, data)

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 5, int(data["a"].position))
		assert.Equal(t, 5, int(least))
	})

	t.Run("one mismatch", func(t *testing.T) {
		// setup
		store := &mockUpdatePositionStore{positions: []store.SubscriptionPosition{}}
		data := map[string]*streamHandler{
			"a": mockStreamHandler(0),
		}
		least, err := updatePosition(store, tx, id, data)

		// verify
		assert.NoError(t, err)
		assert.Equal(t, -1, int(data["a"].position))
		assert.Equal(t, -1, int(least))
	})
	t.Run("multiple", func(t *testing.T) {
		// setup
		store := &mockUpdatePositionStore{positions: []store.SubscriptionPosition{
			{"a", 5},
			{"b", 8},
		}}
		data := map[string]*streamHandler{
			"a": mockStreamHandler(0),
			"b": mockStreamHandler(2),
		}
		least, err := updatePosition(store, tx, id, data)

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 5, int(data["a"].position))
		assert.Equal(t, 8, int(data["b"].position))
		assert.Equal(t, 5, int(least))
	})

	t.Run("missing sub", func(t *testing.T) {
		// setup
		store := &mockUpdatePositionStore{positions: []store.SubscriptionPosition{
			{"a", 5},
			{"b", 8},
		}}
		data := map[string]*streamHandler{
			"a": mockStreamHandler(0),
		}
		least, err := updatePosition(store, tx, id, data)

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 5, int(data["a"].position))
		assert.Equal(t, 5, int(least))
	})
}

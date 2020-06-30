package broker

import (
	"context"
	"testing"
	"time"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
	"github.com/stretchr/testify/assert"
)

type mockStore struct {
	positions []store.SubscriptionPosition
	records   []record.Record
	sets      []store.SubscriptionPosition
}

func (mock *mockStore) Begin(ctx context.Context) (store.Tx, error) {
	return &mockBrokerTx{}, nil
}

func (mock *mockStore) SubscriptionPositionLock(tx store.Tx, id streams.Id, subscriptionIds ...string) ([]store.SubscriptionPosition, error) {
	return mock.positions, nil
}

func (mock *mockStore) ReadRecords(ctx context.Context, id streams.Id, from, to, limit int64) ([]record.Record, error) {
	return mock.records, nil
}

func (mock *mockStore) SetSubscriptionPosition(tx store.Tx, id streams.Id, position store.SubscriptionPosition) error {
	mock.sets = append(mock.sets, position)
	return nil
}

type mockRegistry struct {
}

func (m mockRegistry) ToMessage(r record.Record) (streams.Message, error) {
	return streams.Message{
		Number:      r.Number,
		Stream:      r.Stream,
		Type:        r.Group,
		Data:        r.Data,
		GroupNumber: r.GroupNumber,
		Time:        r.Time,
	}, nil
}

type mockProtocol struct {
	publisher RecordHandler
	input     RecordHandler
}

func (mock *mockProtocol) Register(ctx context.Context, group string, input RecordHandler) (RecordHandler, error) {
	mock.input = input
	return mock.publisher, nil
}

func (mock *mockProtocol) publish(t *testing.T, record record.Record) (bool, error) {
	if !assert.NotNil(t, mock.input, "not registered") {
		t.FailNow()
	}
	return mock.input.Handle(context.Background(), record)
}

func newCountingHandler() *mockCountingHandler {
	return &mockCountingHandler{}
}

type mockCountingHandler struct {
	count int
}

func (mock *mockCountingHandler) Handle(ctx context.Context, msg streams.Message) error {
	mock.count = mock.count + 1
	return nil
}

func TestBroker(t *testing.T) {
	ctx := context.Background()
	id := streams.ParseId("broker")
	emptyHandler := streams.HandlerFunc(func(ctx context.Context, msg streams.Message) error {
		return nil
	})

	R := func(id streams.Id, number int64, globalNumber int64) record.Record {
		return record.Record{
			Number:        number,
			Stream:        id,
			Data:          []byte(`{}`),
			Group:         id.Group,
			ContentType:   "application/json",
			GroupNumber:   globalNumber,
			Time:          time.Now(),
			CorrelationId: "correlation id",
		}
	}

	t.Run("multiple subscriptions", func(t *testing.T) {
		// setup
		store := &mockStore{}
		registry := &mockRegistry{}
		protocol := &mockProtocol{publisher: RecordHandlerFunc(func(ctx context.Context, record record.Record) (bool, error) {
			return true, nil
		})}
		broker := New(store, registry, protocol)

		// execute/verify
		for _, subscriberId := range []string{"A", "B", "C"} {
			// execute
			err := broker.Register(ctx, subscriberId, id, emptyHandler)
			// verify
			assert.NoError(t, err)
		}
	})

	t.Run("notify", func(t *testing.T) {
		// setup
		store := &mockStore{
			positions: nil,
			records: []record.Record{
				R(id, 0, 0),
				R(id, 1, 1),
			},
		}
		registry := &mockRegistry{}
		protocol := &mockProtocol{publisher: RecordHandlerFunc(func(ctx context.Context, record record.Record) (bool, error) {
			return true, nil
		})}
		handler := newCountingHandler()
		broker := New(store, registry, protocol)
		_ = broker.Register(ctx, "A", id, handler)

		// execute
		_, err := protocol.publish(t, R(id, 2, 2))

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 2, handler.count)
	})

	t.Run("notify with position", func(t *testing.T) {
		// setup
		store := &mockStore{
			positions: []store.SubscriptionPosition{{"A", 0}},
			records: []record.Record{
				R(id, 0, 0),
				R(id, 1, 1),
			},
		}
		registry := &mockRegistry{}
		protocol := &mockProtocol{publisher: RecordHandlerFunc(func(ctx context.Context, record record.Record) (bool, error) {
			return true, nil
		})}
		handler := newCountingHandler()
		broker := New(store, registry, protocol)
		_ = broker.Register(ctx, "A", id, handler)

		// execute
		_, err := protocol.publish(t, R(id, 2, 2))

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 1, handler.count)
	})

	t.Run("notify multiples", func(t *testing.T) {
		// setup
		store := &mockStore{
			positions: []store.SubscriptionPosition{
				{"A", 0},
				{"B", 2},
			},
			records: []record.Record{
				R(id, 0, 0),
				R(id, 1, 1),
				R(id, 2, 2),
				R(id, 3, 3),
			},
		}
		registry := &mockRegistry{}
		protocol := &mockProtocol{publisher: RecordHandlerFunc(func(ctx context.Context, record record.Record) (bool, error) {
			return true, nil
		})}
		handlerA := newCountingHandler()
		handlerB := newCountingHandler()
		broker := New(store, registry, protocol)
		_ = broker.Register(ctx, "A", id, handlerA)
		_ = broker.Register(ctx, "B", id, handlerB)

		// execute
		_, err := protocol.publish(t, R(id, 3, 3))

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 3, handlerA.count)
		assert.Equal(t, 1, handlerB.count)
	})
}

package broker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
	"github.com/stretchr/testify/assert"
)

func newMockStore(t *testing.T, positions []store.SubscriptionPosition, records []record.Record) *mockStore {
	return &mockStore{
		t:         t,
		positions: positions,
		sets:      make(map[string]int64),
		records:   records,
	}
}

type mockStore struct {
	t         *testing.T
	mu        sync.Mutex
	positions []store.SubscriptionPosition
	records   []record.Record
	sets      map[string]int64
}

// used to verify it's passed correctly around
var globalTx store.Tx = &mockBrokerTx{}

func (mock *mockStore) Begin(ctx context.Context) (store.Tx, error) {
	return globalTx, nil
}

func (mock *mockStore) SubscriptionPositionLock(tx store.Tx, id streams.Id, subscriptionIds ...string) ([]store.SubscriptionPosition, error) {
	return mock.positions, nil
}

func (mock *mockStore) ReadRecords(ctx context.Context, id streams.Id, from, to, limit int64) ([]record.Record, error) {
	return mock.records, nil
}

func (mock *mockStore) SetSubscriptionPosition(tx store.Tx, id streams.Id, position store.SubscriptionPosition) error {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	assert.Equal(mock.t, globalTx, tx, "wrong tx provided")
	mock.sets[position.SubscriptionId] = position.Position
	return nil
}

func (mock *mockStore) verifyPosition(t *testing.T, subscriberId string, expectedPosition int64) {
	got, isSet := mock.sets[subscriberId]
	if assert.True(t, isSet, "subscriber id %s position not set", subscriberId) {
		assert.Equal(t, expectedPosition, got)
	}
}

type mockRegistry struct{}

func (m mockRegistry) ToMessage(r record.Record) (streams.Message, error) {
	return streams.Message{
		Number:       r.Number,
		Stream:       r.Stream,
		Type:         r.Group,
		Data:         r.Data,
		GlobalNumber: r.GlobalNumber,
		Time:         r.Time,
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
	t.Helper()
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

func newFailingHandler(failAfter int64) *mockFailingHandler {
	return &mockFailingHandler{failAfter: failAfter}
}

type mockFailingHandler struct {
	count     int
	failAfter int64
}

func (mock *mockFailingHandler) Handle(ctx context.Context, msg streams.Message) error {
	mock.count = mock.count + 1
	if int64(mock.count) > mock.failAfter {
		return fmt.Errorf("failed at %d", mock.count)
	}
	return nil
}

func TestBroker(t *testing.T) {
	ctx := context.Background()
	id := streams.ParseId("broker")
	registry := &mockRegistry{}
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
			GlobalNumber:  globalNumber,
			Time:          time.Now(),
			CorrelationId: "correlation id",
		}
	}

	Rs := func(id streams.Id, from, to int64) []record.Record {
		var result []record.Record
		for i := from; i < to; i++ {
			result = append(result, R(id, i, i))
		}
		return result
	}

	t.Run("multiple subscriptions", func(t *testing.T) {
		// setup
		store := newMockStore(t, nil, nil)
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
		store := newMockStore(t, nil, Rs(id, 0, 2))

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
		store := newMockStore(t,
			[]store.SubscriptionPosition{{"A", 0}},
			Rs(id, 0, 2),
		)
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
		mockStore := newMockStore(t,
			[]store.SubscriptionPosition{{"A", 0}, {"B", 2}},
			Rs(id, 0, 4),
		)
		protocol := &mockProtocol{publisher: RecordHandlerFunc(func(ctx context.Context, record record.Record) (bool, error) {
			return true, nil
		})}
		handlerA := newCountingHandler()
		handlerB := newCountingHandler()
		broker := New(mockStore, registry, protocol)
		_ = broker.Register(ctx, "A", id, handlerA)
		_ = broker.Register(ctx, "B", id, handlerB)

		// execute
		_, err := protocol.publish(t, R(id, 3, 3))

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 3, handlerA.count, "count A")
		assert.Equal(t, 1, handlerB.count, "count B")
		mockStore.verifyPosition(t, "A", 3)
		mockStore.verifyPosition(t, "B", 3)
	})

	t.Run("one failing", func(t *testing.T) {
		// setup
		mockStore := newMockStore(t,
			[]store.SubscriptionPosition{{"A", 0}, {"B", 0}},
			Rs(id, 0, 4),
		)
		protocol := &mockProtocol{publisher: RecordHandlerFunc(func(ctx context.Context, record record.Record) (bool, error) {
			return true, nil
		})}
		handlerA := newCountingHandler()
		handlerB := newFailingHandler(1)
		broker := New(mockStore, registry, protocol)
		_ = broker.Register(ctx, "A", id, handlerA)
		_ = broker.Register(ctx, "B", id, handlerB)

		// execute
		_, err := protocol.publish(t, R(id, 3, 3))

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 3, handlerA.count, "count A")
		assert.Equal(t, 2, handlerB.count, "count B")
		mockStore.verifyPosition(t, "A", 3)
		mockStore.verifyPosition(t, "B", 1)
	})
}

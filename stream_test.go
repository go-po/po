package po

import (
	"context"
	"encoding/json"
	"github.com/kyuff/po/internal/broker/mockbroker"
	"github.com/kyuff/po/internal/registry"
	"github.com/kyuff/po/internal/store"
	"github.com/kyuff/po/internal/store/mockstore"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStream_Append(t *testing.T) {

	type A struct {
		A int
	}

	type B struct {
		B string
	}

	testRegistry := registry.New()
	testRegistry.Register(
		func(b []byte) (interface{}, error) {
			msg := A{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
		func(b []byte) (interface{}, error) {
			msg := B{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
	)

	type verify func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker)
	all := func(v ...verify) []verify { return v }
	msgs := func(msg ...interface{}) []interface{} { return msg }
	noErr := func() verify {
		return func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker) {
			assert.NoError(t, err)
		}
	}
	txNotStarted := func() verify {
		return func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker) {
			assert.Nil(t, store.Tx)
		}
	}
	txStarted := func() verify {
		return func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker) {
			assert.NotNil(t, store.Tx)
		}
	}
	recordsStored := func(expected int) verify {
		return func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker) {
			assert.Equal(t, expected, len(store.Records))
		}
	}
	stored := func(i int, expected store.Record) verify {
		return func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker) {
			got := store.Records[i]
			assert.Equal(t, expected.Data, got.Data, "store.Data")
			assert.Equal(t, expected.Type, got.Type, "store.Type")
			assert.Equal(t, expected.Stream, got.Stream, "store.Stream")
		}
	}
	recordsNotified := func(expected int) verify {
		return func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker) {
			assert.Equal(t, expected, len(broker.Records))
		}
	}
	notified := func(i int, expected store.Record) verify {
		return func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker) {
			got := broker.Records[i]
			assert.Equal(t, expected.Data, got.Data, "broker.Data")
			assert.Equal(t, expected.Type, got.Type, "broker.Type")
			assert.Equal(t, expected.Stream, got.Stream, "broker.Stream")
		}
	}

	tests := []struct {
		name     string
		messages []interface{}
		verify   []verify
	}{
		{
			name:     "empty append",
			messages: msgs(),
			verify: all(
				noErr(),
				txNotStarted(),
				recordsStored(0),
				recordsNotified(0),
			),
		},
		{
			name:     "one item",
			messages: msgs(A{A: 1}),
			verify: all(
				noErr(),
				txStarted(),
				recordsStored(1),
				stored(0, store.Record{
					Stream: "test",
					Data:   []byte(`{"A":1}`),
					Type:   "po.A",
				}),
				recordsNotified(1),
				notified(0, store.Record{
					Stream: "test",
					Data:   []byte(`{"A":1}`),
					Type:   "po.A",
				}),
			),
		},
		{
			name: "multiple",
			messages: msgs(
				A{A: 1},
				B{B: "B"},
				A{A: 2},
			),
			verify: all(
				noErr(),
				txStarted(),
				recordsStored(3),
				recordsNotified(3),
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// setup
			store := &mockstore.MockStore{}
			broker := &mockbroker.MockBroker{}
			stream := &Stream{
				ID:       "test",
				ctx:      context.Background(),
				store:    store,
				broker:   broker,
				registry: testRegistry,
			}

			// execute
			err := stream.Append(test.messages...)

			// verify
			for _, v := range test.verify {
				v(t, err, store, broker)
			}

		})
	}
}

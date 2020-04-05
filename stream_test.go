package po

import (
	"context"
	"encoding/json"
	"github.com/go-po/po/internal/broker/mockbroker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/store/mockstore"
	"github.com/go-po/po/streams"
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
	recordsInStore := func(expected int) verify {
		return func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker) {
			assert.Equal(t, expected, len(store.Records))
		}
	}
	stored := func(i int, expected record.Record) verify {
		return func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker) {
			got := store.Records[i]
			assert.Equal(t, expected.Data, got.Data, "store.Data")
			assert.Equal(t, expected.ContentType, got.ContentType, "store.ContentType")
			assert.Equal(t, expected.Stream, got.Stream, "store.Stream")
		}
	}
	recordsNotified := func(expected int) verify {
		return func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker) {
			assert.Equal(t, expected, len(broker.Records))
		}
	}
	notified := func(i int, expected record.Record) verify {
		return func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker) {
			got := broker.Records[i]
			assert.Equal(t, expected.Data, got.Data, "broker.Data")
			assert.Equal(t, expected.Group, got.Group, "broker.Group")
			assert.Equal(t, expected.Stream, got.Stream, "broker.Stream")
		}
	}
	streamId := streams.ParseId("test")

	records := func(count int) []record.Record {
		var result []record.Record
		for i := 1; i < count+1; i++ {
			result = append(result, record.Record{
				Number: int64(i),
				Stream: streamId,
				Data:   []byte(`{"A":1}`),
				Group:  "po.A",
			})
		}
		return result
	}

	tests := []struct {
		name     string
		fixture  []record.Record // data in the stream before appending
		messages []interface{}   // data appended
		verify   []verify
	}{
		{
			name:     "empty append",
			fixture:  records(0),
			messages: msgs(),
			verify: all(
				noErr(),
				txNotStarted(),
				recordsInStore(0),
				recordsNotified(0),
			),
		},
		{
			name:     "one item",
			messages: msgs(A{A: 1}),
			fixture:  records(0),
			verify: all(
				noErr(),
				txStarted(),
				recordsInStore(1),
				stored(0, record.Record{
					Stream:      streamId,
					Data:        []byte(`{"A":1}`),
					ContentType: "application/json; type=po.A",
				}),
				recordsNotified(1),
				notified(0, record.Record{
					Stream:      streamId,
					Data:        []byte(`{"A":1}`),
					ContentType: "application/json; type=po.A",
				}),
			),
		},
		{
			name:    "multiple",
			fixture: records(0),
			messages: msgs(
				A{A: 1},
				B{B: "B"},
				A{A: 2},
			),
			verify: all(
				noErr(),
				txStarted(),
				recordsInStore(3),
				stored(0, record.Record{
					Stream:      streamId,
					Data:        []byte(`{"A":1}`),
					ContentType: "application/json; type=po.A",
				}),
				stored(1, record.Record{
					Stream:      streamId,
					Data:        []byte(`{"B":"B"}`),
					ContentType: "application/json; type=po.B",
				}),
				stored(2, record.Record{
					Stream:      streamId,
					Data:        []byte(`{"A":2}`),
					ContentType: "application/json; type=po.A",
				}),
				recordsNotified(3),
			),
		},
		{
			name:    "raising the id",
			fixture: records(2),
			messages: msgs(
				B{B: "Data"},
			),
			verify: all(
				noErr(),
				txStarted(),
				recordsInStore(3),
				stored(2, record.Record{
					Stream:      streamId,
					Data:        []byte(`{"B":"Data"}`),
					ContentType: "application/json; type=po.B",
				}),
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// setup
			store := &mockstore.MockStore{Records: test.fixture}
			broker := &mockbroker.MockBroker{}
			stream := &Stream{
				ID:       streamId,
				ctx:      context.Background(),
				store:    store,
				broker:   broker,
				registry: testRegistry,
			}

			// execute
			_, err := stream.Append(test.messages...)

			// verify
			for _, v := range test.verify {
				v(t, err, store, broker)
			}

		})
	}
}

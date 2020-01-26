package po

import (
	"context"
	"github.com/kyuff/po/internal/broker/mockbroker"
	"github.com/kyuff/po/internal/registry"
	"github.com/kyuff/po/internal/store/mockstore"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStream_Append(t *testing.T) {
	type verify func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker)
	all := func(v ...verify) []verify { return v }
	noErr := func() verify {
		return func(t *testing.T, err error, store *mockstore.MockStore, broker *mockbroker.MockBroker) {
			assert.NoError(t, err)
		}
	}
	tests := []struct {
		name     string
		messages []interface{}
		verify   []verify
	}{
		{
			name:     "one item",
			messages: nil,
			verify: all(
				noErr(),
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// setup
			store := &mockstore.MockStore{}
			broker := &mockbroker.MockBroker{}
			registry := registry.New()
			stream := &Stream{
				ID:       "test",
				ctx:      context.Background(),
				store:    store,
				broker:   broker,
				registry: registry,
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

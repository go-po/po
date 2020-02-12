// +build integration

package rabbitmq

import (
	"context"
	"encoding/json"
	"github.com/go-po/po"
	"github.com/go-po/po/internal/record"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestBroker_Roundtrip(t *testing.T) {
	// setup
	uri := "amqp://po:po@localhost:5671/"
	broker, err := New(uri, "test-exchange")
	assert.NoError(t, err)
	defer func() {
		_ = broker.Shutdown()
	}()
	sub := &testSubscriber{}
	err = broker.Subscribe(context.Background(), "test-subscriber", "my test stream", sub)
	assert.NoError(t, err, "subscribing")

	// execute publish
	for i := 1; i <= 5; i++ {
		err = broker.Notify(context.Background(), record.Record{
			Number:      int64(i),
			Stream:      "my test stream",
			Data:        []byte(`{ "Foo" : "Bar" }`),
			Type:        "rabbitmq.TestMessage",
			Time:        time.Now(),
			GroupNumber: 0,
		})
		// verify publish
		assert.NoError(t, err)
	}

	// verify receive
	time.Sleep(time.Second) // Wait for rabbit to distribute
	if assert.Equal(t, 5, len(sub.msgs), "number of messages received") {
		assert.Equal(t, int64(1), sub.msgs[0].Number)
		assert.Equal(t, "my test stream", sub.msgs[0].Stream)
		msg, ok := sub.msgs[0].Data.(TestMessage)
		if assert.True(t, ok, "message type") {
			assert.Equal(t, "Bar", msg.Foo)
		}
	}

}

type testSubscriber struct {
	msgs []po.Message
	err  error
}

func (s *testSubscriber) Handle(ctx context.Context, msg po.Message) error {
	s.msgs = append(s.msgs, msg)
	return s.err
}

type TestMessage struct {
	Foo string
}

func init() {
	po.RegisterMessages(
		func(b []byte) (interface{}, error) {
			msg := TestMessage{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
	)
}

// +build integration

package rabbitmq

import (
	"context"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/stream"
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
	stub := &stubDistributor{}
	broker.distributor = stub
	err = broker.Subscribe(context.Background(), stream.ParseId("my test stream"))
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
	time.Sleep(200 * time.Millisecond) // Wait for rabbit to distribute
	if assert.Equal(t, 5, len(stub.records), "number of records received") {
		assert.Equal(t, int64(1), stub.records[0].Number)
		assert.Equal(t, "my test stream", stub.records[0].Stream)
		assert.Equal(t, []byte(`{ "Foo" : "Bar" }`), stub.records[0].Data)
	}

}

type stubDistributor struct {
	records []record.Record
	ack     bool
	err     error
}

func (stub *stubDistributor) Distribute(ctx context.Context, record record.Record) (bool, error) {
	stub.records = append(stub.records, record)
	return stub.ack, stub.err
}

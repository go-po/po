// +build integration

package rabbitmq

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

const uri = "amqp://po:po@localhost:5671/"

func TestBroker_Roundtrip(t *testing.T) {
	// setup
	var records []record.Record
	for i := 1; i <= 5; i++ {
		records = append(records, record.Record{
			Number:      int64(i),
			Stream:      stream.ParseId("my test stream-" + strconv.Itoa(i)),
			Data:        []byte(`{ "Foo" : "Bar" }`),
			Group:       "rabbitmq.TestMessage",
			Time:        time.Now(),
			GroupNumber: 0,
		})
	}
	stubAssigner := &stubAssigner{
		records: records,
	}
	broker, err := New(uri, "test-exchange", "TestBroker_Roundtrip", stubAssigner)
	assert.NoError(t, err)
	defer func() {
		_ = broker.Shutdown()
	}()
	stubDist := &stubDistributor{}
	broker.distributor = stubDist
	err = broker.Subscribe(context.Background(), stream.ParseId("my test stream"))
	assert.NoError(t, err, "subscribing")

	// execute publish
	err = broker.Notify(context.Background(), records...)

	// verify publish
	assert.NoError(t, err)

	// verify receive
	time.Sleep(400 * time.Millisecond) // Wait for rabbit to distribute
	if assert.Equal(t, 5, len(stubDist.records), "number of records received") {
		assert.Equal(t, int64(1), stubDist.records[0].Number)
		assert.Equal(t, "my test stream-1", stubDist.records[0].Stream.String())
		assert.Equal(t, "my test stream-2", stubDist.records[1].Stream.String())
		assert.Equal(t, []byte(`{ "Foo" : "Bar" }`), stubDist.records[0].Data)
	}

	assert.NoError(t, broker.Shutdown(), "shutdown")

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

type stubAssigner struct {
	records []record.Record
	err     error
}

func (stub *stubAssigner) AssignGroup(ctx context.Context, id stream.Id, number int64) (record.Record, error) {
	for _, r := range stub.records {
		if r.Stream.String() == id.String() && r.Number == number {
			return r, stub.err
		}
	}
	return record.Record{}, fmt.Errorf("record not found: %s", id)
}

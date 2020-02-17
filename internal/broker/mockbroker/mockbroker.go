package mockbroker

import (
	"context"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
)

type MockBroker struct {
	Records []record.Record
}

func (mock *MockBroker) Subscribe(ctx context.Context, streamId stream.Id) error {
	return nil
}

func (mock *MockBroker) Distributor(distributor broker.Distributor) {

}

func (mock *MockBroker) Notify(ctx context.Context, records ...record.Record) error {
	mock.Records = append(mock.Records, records...)
	return nil
}

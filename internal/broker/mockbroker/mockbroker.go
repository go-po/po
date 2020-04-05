package mockbroker

import (
	"context"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
)

type MockBroker struct {
	Err     error
	Records []record.Record
}

func (mock *MockBroker) Register(ctx context.Context, subscriberId string, streamId streams.Id, subscriber interface{}) error {
	return mock.Err
}

func (mock *MockBroker) Notify(ctx context.Context, records ...record.Record) error {
	mock.Records = append(mock.Records, records...)
	return mock.Err
}

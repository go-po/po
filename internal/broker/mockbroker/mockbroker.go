package mockbroker

import (
	"context"
	"github.com/kyuff/po/internal/record"
)

type MockBroker struct {
	Records []record.Record
}

func (mock *MockBroker) Notify(ctx context.Context, records ...record.Record) error {
	mock.Records = append(mock.Records, records...)
	return nil
}

func (mock *MockBroker) Subscribe(ctx context.Context, subscriptionId, streamId string, subscriber interface{}) error {
	return nil
}

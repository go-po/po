package mockbroker

import (
	"context"
	"github.com/kyuff/po/internal/store"
)

type MockBroker struct {
}

func (mock *MockBroker) Notify(ctx context.Context, records ...store.Record) error {
	return nil
}

func (mock *MockBroker) Subscribe(ctx context.Context, subscriptionId, streamId string, subscriber interface{}) error {
	return nil
}

package mockstore

import (
	"context"
	"github.com/kyuff/po/internal/store"
)

type MockStore struct {
}

func (mock *MockStore) ReadRecords(ctx context.Context, streamId string) ([]store.Record, error) {
	return nil, nil
}

func (mock *MockStore) Begin(ctx context.Context) (store.Tx, error) {
	return &MockTx{}, nil
}

func (mock *MockStore) Store(tx store.Tx, record store.Record) error {
	panic("implement me")
}

var _ store.Tx = &MockTx{}

type MockTx struct {
}

func (mock *MockTx) Commit() error {
	return nil
}

func (mock *MockTx) Rollback() error {
	return nil
}

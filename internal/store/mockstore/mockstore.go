package mockstore

import (
	"context"
	"github.com/kyuff/po/internal/store"
)

type MockStore struct {
	Tx      *MockTx
	Records []store.Record
}

func (mock *MockStore) ReadRecords(ctx context.Context, streamId string) ([]store.Record, error) {
	return mock.Records, nil
}

func (mock *MockStore) Begin(ctx context.Context) (store.Tx, error) {
	mock.Tx = &MockTx{}
	return mock.Tx, nil
}

func (mock *MockStore) Store(tx store.Tx, record store.Record) error {
	mock.Records = append(mock.Records, record)
	return nil
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

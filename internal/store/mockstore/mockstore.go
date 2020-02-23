package mockstore

import (
	"context"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/stream"
	"time"
)

type MockStore struct {
	Tx      *MockTx
	Records []record.Record
}

func (mock *MockStore) AssignGroupNumber(ctx context.Context, r record.Record) (int64, error) {
	return 0, nil
}

func (mock *MockStore) ReadRecords(ctx context.Context, id stream.Id) ([]record.Record, error) {
	return mock.Records, nil
}

func (mock *MockStore) Begin(ctx context.Context) (store.Tx, error) {
	mock.Tx = &MockTx{}
	return mock.Tx, nil
}

func (mock *MockStore) StoreRecord(tx store.Tx, id stream.Id, msgType string, data []byte) (record.Record, error) {
	r := record.Record{
		Number:      0,
		Stream:      id,
		Data:        data,
		Group:       msgType,
		GroupNumber: 0,
		Time:        time.Time{},
	}
	mock.Records = append(mock.Records, r)
	return r, nil
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

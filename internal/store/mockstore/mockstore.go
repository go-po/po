package mockstore

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
	"time"
)

type MockStore struct {
	Tx      *MockTx
	Records []record.Record
}

func (mock *MockStore) ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error) {
	return record.Snapshot{}, fmt.Errorf("implement me")
}

func (mock *MockStore) UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error {
	return fmt.Errorf("implement me")
}

func (mock *MockStore) GetStreamPosition(ctx context.Context, id streams.Id) (int64, error) {
	return int64(len(mock.Records)), nil
}

func (mock *MockStore) AssignGroup(ctx context.Context, id streams.Id, number int64) (record.Record, error) {
	panic("implement me")
}

func (mock *MockStore) ReadRecords(ctx context.Context, id streams.Id, from int64) ([]record.Record, error) {
	return mock.Records, nil
}

func (mock *MockStore) GetSubscriberPosition(tx store.Tx, subscriberId string, stream streams.Id) (int64, error) {
	return 0, nil
}

func (mock *MockStore) SetSubscriberPosition(tx store.Tx, subscriberId string, stream streams.Id, position int64) error {
	return nil
}

func (mock *MockStore) Begin(ctx context.Context) (store.Tx, error) {
	mock.Tx = &MockTx{}
	return mock.Tx, nil
}

func (mock *MockStore) StoreRecord(tx store.Tx, id streams.Id, number int64, contentType string, data []byte) (record.Record, error) {
	r := record.Record{
		Number:      number,
		Stream:      id,
		Data:        data,
		ContentType: contentType,
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

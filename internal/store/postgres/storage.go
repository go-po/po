package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/internal/store/postgres/generated/db"
	"github.com/go-po/po/streams"
)

func NewFromUrl(databaseUrl string) (*Storage, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		return nil, err
	}
	return NewFromConn(db), nil
}

func NewFromConn(conn *sql.DB) *Storage {
	return &Storage{
		conn: conn,
	}
}

type Storage struct {
	conn     *sql.DB
	db       *db.Queries
	observer store.Observer
}

func (store *Storage) Begin(ctx context.Context) (store.Tx, error) {
	return begin(ctx, store.conn)
}

func (store *Storage) SubscriptionPositionLock(storeTx store.Tx, id streams.Id, subscriptionIds ...string) ([]store.SubscriptionPosition, error) {
	tx, isTx := storeTx.(*storageTx)
	if !isTx {
		return nil, fmt.Errorf("wrong tx: %T", storeTx)
	}
	return subscriberPositionLock(tx.ctx, tx.tx, id, subscriptionIds...)
}

func (store *Storage) SetSubscriptionPosition(storeTx store.Tx, id streams.Id, position store.SubscriptionPosition) error {
	tx, isTx := storeTx.(*storageTx)
	if !isTx {
		return fmt.Errorf("wrong tx: %T", storeTx)
	}
	return updateSubscriberPosition(tx.ctx, tx.tx, id, position)
}

func (store *Storage) WriteRecords(ctx context.Context, id streams.Id, data ...record.Data) ([]record.Record, error) {
	return writeRecords(ctx, store.conn, id, -1, data...)
}

func (store *Storage) WriteRecordsFrom(ctx context.Context, id streams.Id, position int64, data ...record.Data) ([]record.Record, error) {
	return writeRecords(ctx, store.conn, id, position, data...)
}

func (store *Storage) ReadRecords(ctx context.Context, id streams.Id, from int64, to, limit int64) ([]record.Record, error) {
	done := store.observer.ReadRecords(ctx, id)
	defer done()
	return readRecords(ctx, store.conn, id, from, to, limit)
}

func (store *Storage) ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error) {
	done := store.observer.ReadSnapshot(ctx, id, snapshotId)
	defer done()
	return readSnapshot(ctx, store.conn, id, snapshotId)

}

func (store *Storage) UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error {
	done := store.observer.UpdateSnapshot(ctx, id, snapshotId)
	defer done()

	return updateSnapshot(ctx, store.conn, id, snapshotId, snapshot)
}

func (store *Storage) DeleteSnapshot(ctx context.Context, id streams.Id, snapshotId string) error {
	return deleteSnapshot(ctx, store.conn, id, snapshotId)
}

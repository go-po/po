package postgres

import (
	"context"
	"database/sql"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/internal/store/postgres/generated/db"
	"github.com/go-po/po/streams"
)

type Storage struct {
	conn     *sql.DB
	db       *db.Queries
	observer store.Observer
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

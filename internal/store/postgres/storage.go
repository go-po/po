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

func (store *Storage) ReadRecords(ctx context.Context, id streams.Id, from int64) ([]record.Record, error) {
	done := store.observer.ReadRecords(ctx, id)
	defer done()

	var records []record.Record
	var poMsgs []db.PoMsg
	var err error

	if id.HasEntity() {

	} else {

	}
	if err != nil {
		return nil, err
	}
	for _, msg := range poMsgs {
		records = append(records, toRecord(msg))
	}
	return records, nil
}

func (store *Storage) ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error) {
	done := store.observer.ReadSnapshot(ctx, id, snapshotId)
	defer done()

	position, err := store.db.GetSnapshotPosition(ctx, db.GetSnapshotPositionParams{
		Stream:   id.String(),
		Listener: snapshotId,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return record.Snapshot{
				Data:        emptyJson,
				Position:    0,
				ContentType: "application/json",
			}, nil
		}
		return record.Snapshot{}, err
	}
	return record.Snapshot{
		Data:        position.Data,
		Position:    position.No,
		ContentType: position.ContentType,
	}, nil
}

func (store *Storage) UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error {
	done := store.observer.UpdateSnapshot(ctx, id, snapshotId)
	defer done()

	err := store.db.SetSubscriberPosition(ctx, db.SetSubscriberPositionParams{
		Stream:      id.String(),
		Listener:    snapshotId,
		No:          snapshot.Position,
		ContentType: snapshot.ContentType,
		Data:        snapshot.Data,
	})
	if err != nil {
		return err
	}
	return nil
}

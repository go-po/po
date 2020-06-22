package postgres

import (
	"context"
	"database/sql"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store/postgres/generated/db"
	"github.com/go-po/po/streams"
)

var emptyJson = []byte("{}")

func readSnapshot(ctx context.Context, conn *sql.DB, id streams.Id, snapshotId string) (record.Snapshot, error) {
	dao := db.New(conn)
	position, err := dao.GetSnapshotPosition(ctx, db.GetSnapshotPositionParams{
		Stream:     id.String(),
		SnapshotID: snapshotId,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return record.Snapshot{
				Data:        emptyJson,
				Position:    -1,
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

func updateSnapshot(ctx context.Context, conn *sql.DB, id streams.Id, snapshotId string, snapshot record.Snapshot) error {
	dao := db.New(conn)
	err := dao.UpdateSnapshot(ctx, db.UpdateSnapshotParams{
		Stream:      id.String(),
		SnapshotID:  snapshotId,
		No:          snapshot.Position,
		ContentType: snapshot.ContentType,
		Data:        snapshot.Data,
	})
	if err != nil {
		return err
	}
	return nil
}

func deleteSnapshot(ctx context.Context, conn *sql.DB, id streams.Id, snapshotId string) error {
	return db.New(conn).DeleteSnapshot(ctx, db.DeleteSnapshotParams{
		Stream:     id.String(),
		SnapshotID: snapshotId,
	})
}

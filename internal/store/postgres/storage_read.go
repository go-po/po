package postgres

import (
	"context"
	"database/sql"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store/postgres/generated/db"
	"github.com/go-po/po/streams"
)

func readRecords(ctx context.Context, conn *sql.DB, id streams.Id, from int64) ([]record.Record, error) {

	dao := db.New(conn)

	var records []record.Record
	var msgs []db.PoMessage
	var err error

	if id.HasEntity() {
		msgs, err = dao.ReadRecordsByStream(ctx, db.ReadRecordsByStreamParams{
			Stream: id.String(),
			No:     from,
		})
	} else {
		msgs, err = dao.ReadRecordsByGroup(ctx, db.ReadRecordsByGroupParams{
			Grp: id.String(),
			ID:  from,
		})
	}

	if err != nil {
		return nil, err
	}
	for _, msg := range msgs {
		records = append(records, msgToRecord(msg))
	}
	return records, nil
}

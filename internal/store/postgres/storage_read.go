package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store/postgres/generated/db"
	"github.com/go-po/po/streams"
)

func readRecords(ctx context.Context, conn *sql.DB, id streams.Id, from, to, limit int64) ([]record.Record, error) {

	if limit > math.MaxInt32 || limit < 1 {
		return nil, fmt.Errorf("limit cap: %d", limit)
	}

	dao := db.New(conn)

	var records []record.Record
	var msgs []db.PoMessage
	var err error

	if id.HasEntity() {
		msgs, err = dao.ReadRecordsByStream(ctx, db.ReadRecordsByStreamParams{
			Stream: id.String(),
			No:     from,
			No_2:   to,
			Limit:  int32(limit),
		})
	} else {
		msgs, err = dao.ReadRecordsByGroup(ctx, db.ReadRecordsByGroupParams{
			Grp:   id.Group,
			ID:    from,
			ID_2:  to,
			Limit: int32(limit),
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

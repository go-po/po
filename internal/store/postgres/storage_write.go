package postgres

import (
	"context"
	"database/sql"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/internal/store/postgres/generated/db"
	"github.com/go-po/po/streams"
	"github.com/lib/pq"
)

func writeRecords(ctx context.Context, conn *sql.DB, id streams.Id, position int64, data ...record.Data) ([]record.Record, error) {
	if len(data) == 0 {
		return nil, nil
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	dao := db.New(tx)

	if position < 0 {
		position, err = dao.GetStreamPosition(ctx, id.String())
		if err != nil {
			return nil, err
		}
	}

	var records []record.Record
	for _, r := range data {
		stored, err := writeRecord(ctx, dao, id, r, position+1)
		if err != nil {
			return nil, err
		}
		records = append(records, stored)
		position = stored.Number
	}

	return records, tx.Commit()
}

func msgToRecord(msg db.PoMessage) record.Record {
	return record.Record{
		Number:      msg.No,
		Stream:      streams.ParseId(msg.Stream),
		Data:        msg.Data,
		Group:       msg.Grp,
		ContentType: msg.ContentType,
		GroupNumber: msg.ID,
		Time:        msg.Created,
	}
}

func writeRecord(ctx context.Context, dao *db.Queries, id streams.Id, data record.Data, position int64) (record.Record, error) {
	stored, err := dao.StoreRecord(ctx, db.StoreRecordParams{
		Stream:        id.String(),
		No:            position,
		Grp:           id.Group,
		ContentType:   data.ContentType,
		Data:          data.Data,
		CorrelationID: sql.NullString{},
	})
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			if err.Code.Name() == "unique_violation" {
				return record.Record{}, store.WriteConflictError{
					StreamId: id,
					Position: position,
					Err:      err,
				}
			}
		}
		return record.Record{}, err
	}
	return msgToRecord(stored), nil
}

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/internal/store/postgres/generated/db"
	"github.com/go-po/po/stream"
	"github.com/lib/pq"
)

func NewFromUrl(databaseUrl string) (*PGStore, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		return nil, err
	}
	return New(db)
}

func New(conn *sql.DB) (*PGStore, error) {
	err := migrateDatabase(conn)
	if err != nil {
		return nil, err
	}
	return &PGStore{
		conn: conn,
		db:   db.New(conn),
	}, nil
}

type PGStore struct {
	conn *sql.DB
	db   *db.Queries
}

func (store *PGStore) ReadRecordsFromTx(tx store.Tx, id stream.Id, from int64) ([]record.Record, error) {
	return nil, nil
}

func (store *PGStore) ReadRecordsFrom(ctx context.Context, id stream.Id, from int64) ([]record.Record, error) {
	var records []record.Record
	var poMsgs []db.PoMsg
	var err error

	if id.HasEntity() {
		poMsgs, err = store.db.GetRecordsByStream(ctx, db.GetRecordsByStreamParams{
			Stream: id.String(),
			No:     from,
		})
	} else {
		poMsgs, err = store.db.GetRecordsByGroup(ctx, db.GetRecordsByGroupParams{
			Grp: id.Group,
			GrpNo: sql.NullInt64{
				Int64: from,
				Valid: true,
			},
		})
	}
	if err != nil {
		return nil, err
	}
	for _, msg := range poMsgs {
		records = append(records, toRecord(msg))
	}
	return records, nil
}

func (store *PGStore) GetLastPosition(tx store.Tx, subscriberId string, stream stream.Id) (int64, error) {
	t, ok := tx.(*pgTx)
	if !ok {
		return 0, ErrUnknownTx{tx}
	}
	position, err := t.db.GetPosition(t.ctx, db.GetPositionParams{
		Stream:   stream.String(),
		Listener: subscriberId,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return position.No, nil
}

func (store *PGStore) SetPosition(tx store.Tx, subscriberId string, stream stream.Id, position int64) error {
	t, ok := tx.(*pgTx)
	if !ok {
		return ErrUnknownTx{tx}
	}
	err := t.db.SetPosition(t.ctx, db.SetPositionParams{
		Stream:   stream.String(),
		Listener: subscriberId,
		No:       position,
	})
	if err != nil {
		return err
	}
	return nil
}

func (store *PGStore) ReadRecords(ctx context.Context, id stream.Id) ([]record.Record, error) {
	var msgs []db.PoMsg
	var err error
	if id.HasEntity() {
		msgs, err = store.db.GetRecordsByStream(ctx, db.GetRecordsByStreamParams{
			Stream: id.String(),
			No:     0, // will be useful when using snapshots
		})
	} else {
		msgs, err = store.db.GetRecordsByGroup(ctx, db.GetRecordsByGroupParams{
			Grp:   id.Group,
			GrpNo: sql.NullInt64{}, // will be useful when using snapshots
		})
	}

	if err != nil {
		return nil, err
	}

	var result []record.Record
	for _, msg := range msgs {
		result = append(result, toRecord(msg))
	}
	return result, nil
}

func (store *PGStore) begin(ctx context.Context) (*pgTx, error) {
	tx, err := store.conn.BeginTx(ctx, &sql.TxOptions{
		//Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return nil, err
	}
	return &pgTx{
		db:  store.db.WithTx(tx),
		tx:  tx,
		ctx: ctx,
	}, nil
}

func (store *PGStore) Begin(ctx context.Context) (store.Tx, error) {
	return store.begin(ctx)
}

func (store *PGStore) StoreRecord(tx store.Tx, id stream.Id, number int64, contentType string, data []byte) (record.Record, error) {
	t, ok := tx.(*pgTx)
	if !ok {
		return record.Record{}, ErrUnknownTx{tx}
	}

	next, err := t.db.GetNextIndex(t.ctx, id.String())
	if err != nil {
		if err == sql.ErrNoRows {
			next = 1
		} else {
			return record.Record{}, fmt.Errorf("get next index: %s", err)
		}
	}

	if next != number {
		return record.Record{}, fmt.Errorf("number out of order: number=%d next=%d", number, next)
	}

	err = t.db.Insert(t.ctx, db.InsertParams{
		Stream:      id.String(),
		No:          next,
		ContentType: contentType,
		Data:        data,
		Grp:         id.Group,
	})
	if err != nil {
		return record.Record{}, fmt.Errorf("insert: %s", err)
	}

	err = t.db.SetNextIndex(t.ctx, db.SetNextIndexParams{
		Stream: id.String(),
		Next:   next + 1,
	})
	if err != nil {
		return record.Record{}, fmt.Errorf("set next index: %s", err)
	}

	msg, err := t.db.GetRecordByStream(t.ctx, db.GetRecordByStreamParams{
		Stream: id.String(),
		No:     next,
	})
	if err != nil {
		return record.Record{}, fmt.Errorf("get record: %s", err)
	}

	return toRecord(msg), nil
}

func (store *PGStore) AssignGroup(ctx context.Context, id stream.Id, number int64) (record.Record, error) {
	tx, err := store.begin(ctx)
	if err != nil {
		return record.Record{}, err
	}
	defer func() { _ = tx.Rollback() }()

	next, nextIndexErr := tx.db.GetNextIndex(tx.ctx, id.Group)
	if nextIndexErr != nil {
		if nextIndexErr == sql.ErrNoRows {
			next = 1
		} else if pqErr, ok := nextIndexErr.(*pq.Error); ok {
			fmt.Printf("PQ: %#v\n", pqErr)
		} else {
			return record.Record{}, fmt.Errorf("get next index: %T %s", nextIndexErr, nextIndexErr)
		}
	}

	err = tx.db.SetNextIndex(ctx, db.SetNextIndexParams{
		Next:   next + 1,
		Stream: id.Group,
	})
	if err != nil {
		return record.Record{}, err
	}

	msg, err := tx.db.SetGroupNumber(tx.ctx, db.SetGroupNumberParams{
		GrpNo: sql.NullInt64{
			Int64: next,
			Valid: true,
		},
		Stream: id.String(),
		No:     number,
	})
	if err != nil {
		return record.Record{}, fmt.Errorf("write group [%s:%d]: %w", id, number, err)
	}
	err = tx.Commit()
	if err != nil {
		return record.Record{}, err
	}

	return toRecord(msg), nil
}

type pgTx struct {
	ctx context.Context
	db  *db.Queries
	tx  *sql.Tx
}

func (t *pgTx) Commit() error {
	return t.tx.Commit()
}

func (t *pgTx) Rollback() error {
	return t.tx.Rollback()
}

func toRecord(msg db.PoMsg) record.Record {
	var grpNo int64 = 0
	if msg.GrpNo.Valid {
		grpNo = msg.GrpNo.Int64
	}
	return record.Record{
		Number:      msg.No,
		Stream:      stream.ParseId(msg.Stream),
		Data:        msg.Data,
		Group:       msg.Grp,
		GroupNumber: grpNo,
		ContentType: msg.ContentType,
		Time:        msg.Created,
	}
}

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

var emptyJson = []byte("{}")

func NewFromUrl(databaseUrl string, observer store.Observer) (*PGStore, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		return nil, err
	}
	return New(db, observer)
}

func New(conn *sql.DB, observer store.Observer) (*PGStore, error) {
	err := migrateDatabase(conn)
	if err != nil {
		return nil, err
	}
	return &PGStore{
		conn:     conn,
		db:       db.New(conn),
		observer: observer,
	}, nil
}

type PGStore struct {
	conn     *sql.DB
	db       *db.Queries
	observer store.Observer
}

func (store *PGStore) ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error) {
	return record.Snapshot{}, nil
}

func (store *PGStore) UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error {
	return nil
}

func (store *PGStore) GetStreamPosition(ctx context.Context, id streams.Id) (int64, error) {
	return 0, nil
}

func (store *PGStore) ReadRecords(ctx context.Context, id streams.Id, from int64) ([]record.Record, error) {
	return nil, nil
}

func (store *PGStore) GetSubscriberPosition(tx store.Tx, subscriberId string, stream streams.Id) (int64, error) {
	return 0, nil
}

func (store *PGStore) SetSubscriberPosition(tx store.Tx, subscriberId string, stream streams.Id, position int64) error {
	return nil
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

func (store *PGStore) StoreRecord(tx store.Tx, id streams.Id, number int64, contentType string, data []byte) (record.Record, error) {
	t, ok := tx.(*pgTx)
	if !ok {
		return record.Record{}, ErrUnknownTx{tx}
	}

	done := store.observer.StoreRecord(t.ctx, id)
	defer done()

	next, err := t.db.GetNextIndex(t.ctx, db.GetNextIndexParams{
		Stream: id.String(),
		Grp:    false,
	})
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
		Grp:    false,
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

func (store *PGStore) AssignGroup(ctx context.Context, id streams.Id, number int64) (record.Record, error) {
	return record.Record{}, nil
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

	return record.Record{
		Number:      msg.No,
		Stream:      streams.ParseId(msg.Stream),
		Data:        msg.Data,
		Group:       msg.Grp,
		GroupNumber: grpNo,
		ContentType: msg.ContentType,
		Time:        msg.Created,
	}
}

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/internal/store/postgres/generated/db"
	"github.com/go-po/po/stream"
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

func (store *PGStore) ReadRecords(ctx context.Context, id stream.Id) ([]record.Record, error) {
	panic("implement me")
}

func (store *PGStore) Begin(ctx context.Context) (store.Tx, error) {
	tx, err := store.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &pgTx{
		db:  store.db.WithTx(tx),
		tx:  tx,
		ctx: ctx,
	}, nil
}

func (store *PGStore) Store(tx store.Tx, record record.Record) error {
	t, ok := tx.(*pgTx)
	if !ok {
		return fmt.Errorf("unknown tx type: %T", tx)
	}

	next, err := t.db.GetNextIndex(t.ctx, record.Stream.String())
	if err != nil {
		return err
	}
	err = t.db.Insert(t.ctx, db.InsertParams{
		Stream:      record.Stream.String(),
		No:          next,
		Grp:         record.Stream.Group,
		ContentType: "application/json",
		Data:        record.Data,
	})

	err = t.db.SetNextIndex(t.ctx, record.Stream.String())
	if err != nil {
		return err
	}

	return nil
}

func (store *PGStore) AssignGroupNumber(ctx context.Context, r record.Record) (int64, error) {
	panic("implement me")
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

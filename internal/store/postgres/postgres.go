package postgres

import (
	"context"
	"database/sql"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/stream"
)

func NewFromUrl(databaseUrl string) (*PGStore, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		return nil, err
	}
	return New(db)
}

func New(db *sql.DB) (*PGStore, error) {
	err := migrateDatabase(db)
	if err != nil {
		return nil, err
	}
	return &PGStore{
		db: db,
	}, nil
}

type PGStore struct {
	db *sql.DB
}

func (store *PGStore) ReadRecords(ctx context.Context, id stream.Id) ([]record.Record, error) {
	panic("implement me")
}

func (store *PGStore) Begin(ctx context.Context) (store.Tx, error) {
	panic("implement me")
}

func (store *PGStore) Store(tx store.Tx, record record.Record) error {
	panic("implement me")
}

func (store *PGStore) AssignGroupNumber(ctx context.Context, r record.Record) (int64, error) {
	panic("implement me")
}

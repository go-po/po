package postgres

import (
	"context"
	"database/sql"

	"github.com/go-po/po/internal/store"
)

type storageTx struct {
	ctx context.Context
	tx  *sql.Tx
}

func (facade *storageTx) Commit() error {
	return facade.tx.Commit()
}

func (facade *storageTx) Rollback() error {
	return facade.tx.Rollback()
}

func begin(ctx context.Context, conn *sql.DB) (store.Tx, error) {
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{
		//Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return nil, err
	}
	return &storageTx{
		tx:  tx,
		ctx: ctx,
	}, nil
}

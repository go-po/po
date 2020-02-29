package postgres

import (
	"fmt"
	"github.com/go-po/po/internal/store"
)

type ErrUnknownTx struct {
	tx store.Tx
}

func (err ErrUnknownTx) Error() string {
	return fmt.Sprintf("unknown tx type: %T", err.tx)
}

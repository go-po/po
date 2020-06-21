package store

import (
	"fmt"

	"github.com/go-po/po/streams"
)

type Tx interface {
	Commit() error
	Rollback() error
}

// Error type used when optimistic locking hits a write conflict
type WriteConflictError struct {
	StreamId streams.Id
	Position int64 // position attempted to write
	Err      error // possible underlying storage engine error
}

func (err WriteConflictError) Error() string {
	return fmt.Sprintf("write conflict on [%s] position %d", err.StreamId, err.Position)
}

func (err WriteConflictError) Is(target error) bool {
	_, ok := target.(WriteConflictError)
	return ok
}
